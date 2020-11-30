#include "module/sequencer.h"

#include <glog/logging.h>

#include "common/proto_utils.h"
#include "module/ticker.h"
#include "paxos/simple_multi_paxos.h"

using std::move;

namespace slog {

using internal::Batch;
using internal::Request;
using internal::Response;

Sequencer::Sequencer(
    const ConfigurationPtr& config,
    const std::shared_ptr<Broker>& broker,
    int poll_timeout_ms)
  : NetworkedModule("Sequencer", broker, kSequencerChannel, poll_timeout_ms),
    config_(config),
    batch_id_counter_(0) {
  NewBatch();
}

vector<zmq::socket_t> Sequencer::InitializeCustomSockets() {
  vector<zmq::socket_t> ticker_socket;
  ticker_socket.push_back(Ticker::Subscribe(*context()));
  return ticker_socket;
}

void Sequencer::NewBatch() {
  if (batch_ == nullptr) {
    batch_.reset(new Batch());
  }
  batch_->Clear();
  batch_->set_transaction_type(TransactionType::SINGLE_HOME);
}

void Sequencer::HandleInternalRequest(ReusableRequest&& req, MachineId /* from */) {
  switch (req.get()->type_case()) {
    case Request::kForwardTxn: {
      // Received a single-home txn
      auto txn = req.get()->mutable_forward_txn()->release_txn();

      RecordTxnEvent(
          config_,
          txn->mutable_internal(),
          TransactionEvent::ENTER_SEQUENCER);

      PutSingleHomeTransactionIntoBatch(txn);
      break;
    }
    case Request::kForwardBatch: {
      // Received a batch of multi-home txns
      if (req.get()->forward_batch().part_case() == internal::ForwardBatch::kBatchData) {
        ProcessMultiHomeBatch(move(req));
      }
      break;
    }
    default:
      LOG(ERROR) << "Unexpected request type received: \""
                 << CASE_NAME(req.get()->type_case(), Request) << "\"";
      break;
  }
}

void Sequencer::HandleCustomSocket(zmq::socket_t& socket, size_t /* socket_index */) {
  // Remove the dummy message out of the queue
  if (zmq::message_t msg; !socket.recv(msg, zmq::recv_flags::dontwait)) {
    return;
  }

#ifdef ENABLE_REPLICATION_DELAY
  MaybeSendDelayedBatches();
#endif /* ENABLE_REPLICATION_DELAY */

  // Do nothing if there is nothing to send
  if (batch_->transactions().empty()) {
    return;
  }

  auto batch_id = NextBatchId();
  batch_->set_id(batch_id);

  VLOG(3) << "Finished batch " << batch_id << " of size " << batch_->transactions().size()
          << ". Sending out for ordering and replicating";

  auto paxos_req = AcquireRequest();
  auto paxos_propose = paxos_req.get()->mutable_paxos_propose();
  paxos_propose->set_value(config_->local_partition());
  Send(*paxos_req.get(), kLocalPaxos);

  auto batch_req = AcquireRequest();
  auto forward_batch = batch_req.get()->mutable_forward_batch();
  // minus 1 so that batch id counter starts from 0
  forward_batch->set_same_origin_position(batch_id_counter_ - 1);
  // this is only temporary
  forward_batch->set_allocated_batch_data(batch_.get());

  // Replicate batch to all machines
  RecordTxnEvent(
      config_,
      forward_batch->mutable_batch_data(),
      TransactionEvent::EXIT_SEQUENCER_IN_BATCH);

#ifdef ENABLE_REPLICATION_DELAY
  // Maybe delay current batch
  if ((uint32_t)(rand() % 100) < config_->replication_delay_percent()) {
    // Completely release the batch because its lifetime is now tied with the
    // delayed request
    batch_.release();
    DelaySingleHomeBatch(move(batch_req));
    NewBatch();
    return;
  } // Otherwise send it normally
#endif /* GetReplicationDelayEnabled */

  // Replicate batch to all machines
  auto num_partitions = config_->num_partitions();
  auto num_replicas = config_->num_replicas();
  for (uint32_t part = 0; part < num_partitions; part++) {
    for (uint32_t rep = 0; rep < num_replicas; rep++) {
      auto machine_id = config_->MakeMachineId(rep, part);
      Send(*batch_req.get(), kInterleaverChannel, machine_id);
    }
  }
  // Release the batch so that it won't die along with the request
  forward_batch->release_batch_data();

  NewBatch();
}

void Sequencer::ProcessMultiHomeBatch(ReusableRequest&& req) {
  auto batch = req.get()->mutable_forward_batch()->mutable_batch_data();
  if (batch->transaction_type() != TransactionType::MULTI_HOME) {
    LOG(ERROR) << "Batch has to contain multi-home txns";
    return;
  }

  RecordTxnEvent(
      config_,
      batch,
      TransactionEvent::ENTER_SEQUENCER_IN_BATCH);

  auto local_rep = config_->local_replica();
  // For each multi-home txn, create a lock-only txn and put into
  // the single-home batch to be sent to the local log
  for (auto& txn : batch->transactions()) {
    auto lock_only_txn = new Transaction();
    
    const auto& metadata = txn.internal().master_metadata();
    auto lock_only_metadata = lock_only_txn->mutable_internal()->mutable_master_metadata();
    
    // Copy keys and metadata in local replica
    for (auto& key_value : txn.read_set()) {
      auto master = metadata.at(key_value.first).master();
      if (master == local_rep) {
        lock_only_txn->mutable_read_set()->insert(key_value);
        lock_only_metadata->insert({key_value.first, metadata.at(key_value.first)});
      }
    }
    for (auto& key_value : txn.write_set()) {
      auto master = metadata.at(key_value.first).master();
      if (master == local_rep) {
        lock_only_txn->mutable_write_set()->insert(key_value);
        lock_only_metadata->insert({key_value.first, metadata.at(key_value.first)});
      }
    }

#ifdef REMASTER_PROTOCOL_COUNTERLESS
    // Add additional lock only at new replica
    // TODO: refactor to remote metadata from lock-onlys. Requires
    // changes in the scheduler
    if (txn.procedure_case() == Transaction::kRemaster) {
    lock_only_txn->mutable_remaster()->set_new_master((txn.remaster().new_master()));
      if (txn.remaster().new_master() == local_rep) {
        lock_only_txn->CopyFrom(txn);
        lock_only_txn->mutable_remaster()->set_is_new_master_lock_only(true);
      }
    }
#endif /* REMASTER_PROTOCOL_COUNTERLESS */ 

    lock_only_txn->mutable_internal()->set_id(txn.internal().id());
    lock_only_txn->mutable_internal()->set_type(TransactionType::LOCK_ONLY);

    if (!lock_only_txn->read_set().empty() || !lock_only_txn->write_set().empty()) {
      PutSingleHomeTransactionIntoBatch(lock_only_txn);
    }
  }

  // Replicate the batch of multi-home txns to all machines in the same region
  RecordTxnEvent(
      config_,
      batch,
      TransactionEvent::EXIT_SEQUENCER_IN_BATCH);

  auto num_partitions = config_->num_partitions();
  for (uint32_t part = 0; part < num_partitions; part++) {
    auto machine_id = config_->MakeMachineId(local_rep, part);
    Send(*req.get(), kInterleaverChannel, machine_id);
  }
}

void Sequencer::PutSingleHomeTransactionIntoBatch(Transaction* txn) {
  DCHECK(
      txn->internal().type() == TransactionType::SINGLE_HOME
      || txn->internal().type() == TransactionType::LOCK_ONLY)
      << "Sequencer batch can only contain single-home or lock-only txn. "
      << "Multi-home txn or unknown txn type received instead.";
  batch_->mutable_transactions()->AddAllocated(txn);
}

BatchId Sequencer::NextBatchId() {
  batch_id_counter_++;
  return batch_id_counter_ * kMaxNumMachines + config_->local_machine_id();
}

#ifdef ENABLE_REPLICATION_DELAY
void Sequencer::DelaySingleHomeBatch(ReusableRequest&& request) {
  delayed_batches_.emplace_back(request);

  // Send the batch to interleavers in the local replica only
  auto local_rep = config_->local_replica();
  auto num_partitions = config_->num_partitions();
  for (uint32_t part = 0; part < num_partitions; part++) {
    auto machine_id = config_->MakeMachineId(local_rep, part);
    Send(
        *request.get(),
        kInterleaverChannel,
        machine_id);
  }
}

void Sequencer::MaybeSendDelayedBatches() {
  for (auto itr = delayed_batches_.begin(); itr != delayed_batches_.end();) {
    // Create a geometric distribution of delay. Each batch has 1 / DelayAmount chance
    // of being sent at every tick
    if (rand() % config_->replication_delay_amount() == 0) {
      VLOG(4) << "Sending delayed batch";
      auto& request = *itr;

      // Replicate batch to all machines EXCEPT local replica
      auto num_replicas = config_->num_replicas();
      auto num_partitions = config_->num_partitions();
      for (uint32_t rep = 0; rep < num_replicas; rep++) {
        if (rep == config_->local_replica()) {
          // Already sent to local replica
          continue;
        }
        for (uint32_t part = 0; part < num_partitions; part++) {
          auto machine_id = config_->MakeMachineId(rep, part);
          Send(
              *request.get(),
              kInterleaverChannel,
              machine_id);
        }
      }

      itr = delayed_batches_.erase(itr);
    } else {
      itr++;
    }
  }
}
#endif /* ENABLE_REPLICATION_DELAY */

} // namespace slog