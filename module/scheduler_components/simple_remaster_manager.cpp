#include "module/scheduler_components/simple_remaster_manager.h"

#include <glog/logging.h>

namespace slog {

SimpleRemasterManager::SimpleRemasterManager(
    shared_ptr<Storage<Key, Record>> storage,
    shared_ptr<TransactionMap> all_txns)
  : storage_(storage), all_txns_(all_txns) {}

VerifyMasterResult
SimpleRemasterManager::VerifyMaster(const TxnReplicaId txn_replica_id) {
  auto& txn_holder = all_txns_->at(txn_replica_id);
  auto& keys = txn_holder.KeysInPartition();
  if (keys.empty()) {
    return VerifyMasterResult::VALID;
  }

  auto txn = txn_holder.GetTransaction();
  auto& txn_master_metadata = txn->internal().master_metadata();
  if (txn_master_metadata.empty()) { // This should only be the case for testing
    LOG(WARNING) << "Master metadata empty: txn id " << txn->internal().id();
    return VerifyMasterResult::VALID;
  }

  // Determine which local log this txn is from. Since only single home or
  // lock only txns, all keys will have same master
  auto local_log_machine_id = txn_master_metadata.begin()->second.master();

  // Block this txn behind other txns from same local log
  // TODO: check the counters now? would abort earlier
  if (blocked_queue_.count(local_log_machine_id) && !blocked_queue_[local_log_machine_id].empty()) {
    blocked_queue_[local_log_machine_id].push_back(txn_replica_id);
    return VerifyMasterResult::WAITING;
  }

  // Test counters
  auto result = CheckCounters(txn_holder);
  if (result == VerifyMasterResult::WAITING) {
    blocked_queue_[local_log_machine_id].push_back(txn_replica_id);
  }
  return result;
}

VerifyMasterResult SimpleRemasterManager::CheckCounters(TransactionHolder& txn_holder) {
  auto& keys = txn_holder.KeysInPartition();
  auto& txn_master_metadata = txn_holder.GetTransaction()->internal().master_metadata();
  for (auto& key_pair : keys) {
    auto& key = key_pair.first;

    auto txn_counter = txn_master_metadata.at(key).counter();

    // Get current counter from storage
    auto storage_counter = 0; // default to 0 for a new key
    Record record;
    bool found = storage_->Read(key, record);
    if (found) {        
      storage_counter = record.metadata.counter;
      CHECK(txn_master_metadata.at(key).master() == record.metadata.master)
              << "Masters don't match for same key \"" << key << "\"";
    }

    if (txn_counter < storage_counter) {
      return VerifyMasterResult::ABORT;
    } else if (txn_counter > storage_counter) {
      return VerifyMasterResult::WAITING;
    }
  }
  return VerifyMasterResult::VALID;
}

RemasterOccurredResult
SimpleRemasterManager::RemasterOccured(const Key remaster_key, const uint32_t remaster_counter) {
  RemasterOccurredResult result;
  // Try to unblock each txn at the head of a queue, if it contains the remastered key.
  // Note that multiple queues could contain the same key with different counters
  for (auto& queue_pair : blocked_queue_) {
    if (!queue_pair.second.empty()) {
      auto txn_replica_id = queue_pair.second.front();
      auto& txn_keys = all_txns_->at(txn_replica_id).KeysInPartition();
      for (auto& key_pair : txn_keys) {
        auto& txn_key = key_pair.first;
        if (txn_key == remaster_key) {
          // TODO: check here if counters match, saves an iteration through all keys
          TryToUnblock(queue_pair.first, result);
          break;
        }
      }
    }
  }
  return result;
}

void SimpleRemasterManager::TryToUnblock(
    const uint32_t local_log_machine_id,
    RemasterOccurredResult& result) {
  if (blocked_queue_.count(local_log_machine_id) == 0 || blocked_queue_[local_log_machine_id].empty()) {
    return;
  }

  auto txn_replica_id = blocked_queue_[local_log_machine_id].front();
  auto& txn_holder = all_txns_->at(txn_replica_id);
  auto& keys = txn_holder.KeysInPartition();

  auto counter_result = CheckCounters(txn_holder);
  if (counter_result == VerifyMasterResult::WAITING) {
    return;
  } else if (counter_result == VerifyMasterResult::VALID) {
    result.unblocked.push_back(txn_replica_id);
  } else if (counter_result == VerifyMasterResult::ABORT) {
    result.should_abort.push_back(txn_replica_id);
  }

  // Head of queue has changed
  blocked_queue_[local_log_machine_id].pop_front();

  // Note: queue may be left empty, since there are not many replicas
  TryToUnblock(local_log_machine_id, result);
}

} // namespace slog
