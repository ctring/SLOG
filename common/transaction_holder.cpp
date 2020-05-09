#include "common/transaction_holder.h"

#include <glog/logging.h>

using std::pair;
using std::make_pair;
using std::vector;
using std::unordered_set;
using std::string;

namespace slog {

namespace {

void ExtractKeyPartitions(
    vector<std::pair<Key, LockMode>>& keys,
    unordered_set<uint32_t>& partition_participants,
    ConfigurationPtr config,
    const Transaction& txn) {
  for (const auto& kv : txn.read_set()) {
    partition_participants.emplace(config->GetPartitionOfKey(kv.first));
    // If this key is also in write_set, give it write lock instead
    if (config->KeyIsInLocalPartition(kv.first) 
        && !txn.write_set().contains(kv.first)) {
      keys.emplace_back(kv.first, LockMode::READ);
    }
  }
  for (const auto& kv : txn.write_set()) {
    partition_participants.emplace(config->GetPartitionOfKey(kv.first));
    if (config->KeyIsInLocalPartition(kv.first)) {
      keys.emplace_back(kv.first, LockMode::WRITE);
    }
  }
}

} // namespace

TransactionHolder::TransactionHolder() : txn_(nullptr), worker_("") {}
TransactionHolder::TransactionHolder(ConfigurationPtr config, Transaction* txn) : worker_("") {
  SetTransaction(config, txn);
}

TransactionHolder::~TransactionHolder() {
  delete txn_;
}

void TransactionHolder::SetTransaction(const ConfigurationPtr config, Transaction* txn) {
  // TODO: partition_participants_ is only needed by MH and SH, could avoid computing for LO
  ExtractKeyPartitions(keys_in_partition_, partition_participants_, config, *txn);
  txn_ = txn;
}

Transaction* TransactionHolder::GetTransaction() const {
  return txn_;
}

Transaction* TransactionHolder::ReleaseTransaction() {
  auto tmp = txn_;
  txn_ = nullptr;
  return tmp;
}

void TransactionHolder::SetWorker(const string& worker) {
  worker_ = worker;
}

const string& TransactionHolder::GetWorker() const {
  return worker_;
}

const vector<pair<Key, LockMode>>& TransactionHolder::KeysInPartition() const {
  return keys_in_partition_;
}

const std::unordered_set<uint32_t>& TransactionHolder::PartitionParticipants() const {
  return partition_participants_;
}

vector<internal::Request>& TransactionHolder::EarlyRemoteReads() {
  return early_remote_reads_;
}

uint32_t TransactionHolder::GetReplicaId() const {
  return GetReplicaId(txn_);
}

uint32_t TransactionHolder::GetReplicaId(Transaction* txn) {
  // Note that this uses all metadata, not just keys in partition. This shouldn't be empty,
  // except for in testing.
  // TODO: make this fatal, add metadata to tests
  if (txn->internal().master_metadata().empty()) {
    LOG(WARNING) << "Master metadata empty: txn id " << txn->internal().id();
    return 0;
  }
  // Get the master of an element from the metadata. For single-home and lock-onlies, all masters
  // will be the same in the metadata
  return txn->internal().master_metadata().begin()->second.master();
}

const TxnIdReplicaIdPair TransactionHolder::GetTransactionIdReplicaIdPair() const {
  return GetTransactionIdReplicaIdPair(txn_);
}

const TxnIdReplicaIdPair TransactionHolder::GetTransactionIdReplicaIdPair(Transaction* txn) {
  auto txn_id = txn->internal().id();
  auto local_log_id = GetReplicaId(txn);
  return make_pair(txn_id, local_log_id);
}

} // namespace slog