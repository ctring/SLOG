#pragma once

#include <unordered_map>
#include <unordered_set>

#include <zmq.hpp>

#include "common/configuration.h"
#include "common/types.h"
#include "common/transaction_holder.h"
#include "module/base/module.h"
#include "module/scheduler_components/commands.h"
#include "proto/transaction.pb.h"
#include "proto/internal.pb.h"
#include "storage/storage.h"

using std::shared_ptr;
using std::unique_ptr;
using std::unordered_set;

namespace slog {

struct TransactionState {
  enum class Phase {
    READ_LOCAL_STORAGE,
    WAIT_REMOTE_READ,
    EXECUTE,
    COMMIT,
    FINISH,
  };

  TransactionState() = default;
  TransactionState(TransactionHolder* txn_holder) : txn_holder(txn_holder) {}
  TransactionHolder* txn_holder = nullptr;
  uint32_t remote_reads_waiting_on = 0;
  Phase phase = Phase::READ_LOCAL_STORAGE;
};

/**
 * A worker executes and commits transactions. Every time it receives from
 * the scheduler a message pertaining to a transaction X, it will either
 * initializes the state for X if X is a new transaction or try to advance
 * X to the subsequent phases as much as possible.
 */
class Worker : public Module {
public:
  Worker(
      const string& identity,
      const ConfigurationPtr& config,
      zmq::context_t& context,
      const shared_ptr<Storage<Key, Record>>& storage);
  void SetUp() final;
  void Loop() final;

private:
  /**
   * Initializes the state of a new transaction
   */
  TxnId ProcessWorkerRequest(const internal::WorkerRequest& req);
  
  /**
   * Applies remote read for transactions that are in the WAIT_REMOTE_READ phase.
   * When all remote reads are received, the transaction is moved to the EXECUTE phase.
   */
  TxnId ProcessRemoteReadResult(const internal::RemoteReadResult& read_result);

  /**
   * Drives most of the phase transition of a transaction
   */
  void AdvanceTransaction(TxnId txn_id);

  /**
   * Checks master metadata information and reads local data to the transaction
   * buffer, then broadcast local data to other partitions
   */
  void ReadLocalStorage(TxnId txn_id);
  
  /**
   * Executes the code inside the transaction
   */
  void Execute(TxnId txn_id);

  /**
   * Applies the writes to local storage
   */
  void Commit(TxnId txn_id);

  /**
   * Returns the result back to the scheduler and cleans up the transaction state
   */
  void Finish(TxnId txn_id);

  void SendToOtherPartitions(
      internal::Request&& request,
      const std::unordered_set<uint32_t>& partitions);

  void SendToScheduler(
      const google::protobuf::Message& req_or_res,
      string&& forward_to_machine = "");

  std::string identity_;
  ConfigurationPtr config_;
  zmq::socket_t scheduler_socket_;
  shared_ptr<Storage<Key, Record>> storage_;
  unique_ptr<Commands> commands_;
  zmq::pollitem_t poll_item_;

  unordered_map<TxnId, TransactionState> txn_states_;
};

} // namespace slog