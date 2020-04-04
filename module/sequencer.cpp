#include "module/sequencer.h"

#include "common/proto_utils.h"
#include "paxos/simple_multi_paxos.h"

namespace slog {

using internal::Batch;
using internal::Request;
using internal::Response;

Sequencer::Sequencer(ConfigurationPtr config, Broker& broker)
  : BasicModule(
        "Sequencer",
        broker.AddChannel(SEQUENCER_CHANNEL),
        config->GetBatchDuration() /* wake_up_every_ms */),
    config_(config),
    local_paxos_(new SimpleMultiPaxosClient(*this, LOCAL_PAXOS)),
    batch_id_counter_(0) {
  NewBatch();
}

void Sequencer::NewBatch() {
  batch_.reset(new Batch());
  batch_->set_transaction_type(TransactionType::SINGLE_HOME);
}

void Sequencer::HandleInternalRequest(
    Request&& req,
    string&& /* from_machine_id */) {
  switch (req.type_case()) {
    case Request::kForwardTxn: {
      // Received a single-home txn
      auto txn = req.mutable_forward_txn()->release_txn();
      PutSingleHomeTransactionIntoBatch(txn);
      break;
    }
    case Request::kForwardBatch: {
      // Received a batch of multi-home txns
      if (req.forward_batch().part_case() == internal::ForwardBatch::kBatchData) {
        ProcessMultiHomeBatch(std::move(req));
      }
      break;
    }
    default:
      LOG(ERROR) << "Unexpected request type received: \""
                 << CASE_NAME(req.type_case(), Request) << "\"";
      break;
  }
}

void Sequencer::HandlePeriodicWakeUp() {
  // Do nothing if there is nothing to send
  if (batch_->transactions().empty()) {
    return;
  }

  auto batch_id = NextBatchId();
  batch_->set_id(batch_id);

  VLOG(3) << "Finished batch " << batch_id
          << ". Sending out for ordering and replicating";

  Request req;
  auto forward_batch = req.mutable_forward_batch();
  // minus 1 so that batch id counter starts from 0
  forward_batch->set_same_origin_position(batch_id_counter_ - 1);
  forward_batch->set_allocated_batch_data(batch_.release());

  // Send batch id to local paxos for ordering
  local_paxos_->Propose(config_->GetLocalPartition());

  // Replicate batch to all machines
  for (uint32_t part = 0; part < config_->GetNumPartitions(); part++) {
    for (uint32_t rep = 0; rep < config_->GetNumReplicas(); rep++) {
      auto machine_id = MakeMachineIdAsString(rep, part);
      Send(req, machine_id, SCHEDULER_CHANNEL);
    }
  }

  NewBatch();
}

void Sequencer::ProcessMultiHomeBatch(Request&& req) {
  const auto& batch = req.forward_batch().batch_data();
  if (batch.transaction_type() != TransactionType::MULTI_HOME) {
    LOG(ERROR) << "Batch has to contain multi-home txns";
    return;
  }

  auto local_rep = config_->GetLocalReplica();
  // For each multi-home txn, create a lock-only txn and put into
  // the single-home batch to be sent to the local log
  for (auto& txn : batch.transactions()) {
    auto lock_only_txn = new Transaction();

    lock_only_txn->mutable_internal()
        ->set_id(txn.internal().id());

    lock_only_txn->mutable_internal()
        ->set_type(TransactionType::LOCK_ONLY);
    
    const auto& metadata = txn.internal().master_metadata();
    
    for (auto& key_value : txn.read_set()) {
      auto master = metadata.at(key_value.first).master();
      if (master == local_rep) {
        lock_only_txn->mutable_read_set()->insert(key_value);
      }
    }
    for (auto& key_value : txn.write_set()) {
      auto master = metadata.at(key_value.first).master();
      if (master == local_rep) {
        lock_only_txn->mutable_write_set()->insert(key_value);
      }
    }
    // TODO: Ignore lock only txns with no key
    PutSingleHomeTransactionIntoBatch(lock_only_txn);
  }

  // Replicate the batch of multi-home txns to all machines in the same region
  for (uint32_t part = 0; part < config_->GetNumPartitions(); part++) {
    auto machine_id = MakeMachineIdAsString(local_rep, part);
    Send(req, machine_id, SCHEDULER_CHANNEL);
  }
}

void Sequencer::PutSingleHomeTransactionIntoBatch(Transaction* txn) {
  CHECK(
      txn->internal().type() == TransactionType::SINGLE_HOME
      || txn->internal().type() == TransactionType::LOCK_ONLY)
      << "Sequencer batch can only contain single-home or lock-only txn. "
      << "Multi-home txn or unknown txn type received instead.";
  batch_->mutable_transactions()->AddAllocated(txn);
}

BatchId Sequencer::NextBatchId() {
  batch_id_counter_++;
  return batch_id_counter_ * MAX_NUM_MACHINES + config_->GetLocalMachineIdAsNumber();
}

} // namespace slog