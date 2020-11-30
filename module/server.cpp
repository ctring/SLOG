#include "module/server.h"

#include <glog/logging.h>

#include "common/constants.h"
#include "common/json_utils.h"
#include "common/proto_utils.h"
#include "connection/zmq_utils.h"
#include "proto/internal.pb.h"

using std::move;

namespace slog {

Server::Server(
    const ConfigurationPtr& config,
    const shared_ptr<Broker>& broker,
    int poll_timeout_ms)
  : NetworkedModule("Server", broker, kServerChannel, poll_timeout_ms),
    config_(config),
    txn_id_counter_(0) {}

/***********************************************
                SetUp and Loop
***********************************************/

std::vector<zmq::socket_t> Server::InitializeCustomSockets() {
  string endpoint = "tcp://*:" + std::to_string(config_->server_port());
  zmq::socket_t client_socket(*context(), ZMQ_ROUTER);
  client_socket.setsockopt(ZMQ_LINGER, 0);
  client_socket.setsockopt(ZMQ_RCVHWM, 0);
  client_socket.setsockopt(ZMQ_SNDHWM, 0);
  client_socket.bind(endpoint);

  LOG(INFO) << "Bound Server to: " << endpoint;

  vector<zmq::socket_t> sockets;
  sockets.push_back(move(client_socket));
  return sockets;
}

/***********************************************
                  API Requests
***********************************************/

void Server::HandleCustomSocket(zmq::socket_t& socket, size_t) {
  zmq::message_t identity;
  if (!socket.recv(identity, zmq::recv_flags::dontwait)) {
    return;
  }
  if (!identity.more()) {
    LOG(ERROR) << "Invalid message from client: Only identity part is found";
    return;
  }
  api::Request request;
  if (!ReceiveProtoWithEmptyDelimiter(socket, request)) {
    LOG(ERROR) << "Invalid message from client: Body is not a proto";
    return;
  }

  // While this is called txn id, we use it for any kind of request
  auto txn_id = NextTxnId();
  CHECK(pending_responses_.count(txn_id) == 0) << "Duplicate transaction id: " << txn_id;

  pending_responses_[txn_id] = {
    // Save the identity of the client to response later
    .identity = std::move(identity),
    // Stream id is used by a client to match up request-response on its side.
    // The server does not use this and just echos it back to the client.
    .stream_id = request.stream_id()
  };

  switch (request.type_case()) {
    case api::Request::kTxn: {
      auto txn = request.mutable_txn()->release_txn();
      auto txn_internal = txn->mutable_internal();
      RecordTxnEvent(config_, txn_internal, TransactionEvent::ENTER_SERVER);
      txn_internal->set_id(txn_id);
      txn_internal->set_coordinating_server(config_->local_machine_id());

      if(ValidateTransaction(txn)) {
        // Send to forwarder
        internal::Request forward_request;
        forward_request.mutable_forward_txn()->set_allocated_txn(txn);
        RecordTxnEvent(config_, txn_internal, TransactionEvent::EXIT_SERVER_TO_FORWARDER);
        Send(forward_request, kForwarderChannel);
      } else {
        // Return abort to client
        txn->set_status(TransactionStatus::ABORTED);

        auto r = AcquireRequest();
        auto completed_subtxn = r.get()->mutable_completed_subtxn();
        completed_subtxn->set_allocated_txn(txn);
        // Txn only exists in single, local partition
        completed_subtxn->set_partition(0);
        completed_subtxn->add_involved_partitions(0);
        ProcessCompletedSubtxn(move(r));
      }
      break;
    }
    case api::Request::kStats: {
      internal::Request stats_request;

      auto level = request.stats().level();
      stats_request.mutable_stats()->set_id(txn_id);
      stats_request.mutable_stats()->set_level(level);

      // Send to appropriate module based on provided information
      switch (request.stats().module()) {
        case api::StatsModule::SERVER:
          ProcessStatsRequest(stats_request.stats());
          break;
        case api::StatsModule::SCHEDULER:
          Send(stats_request, kSchedulerChannel);
          break;
        default:
          LOG(ERROR) << "Invalid module for stats request";
          break;
      }
      break;
    }
    default:
      pending_responses_.erase(txn_id);
      LOG(ERROR) << "Unexpected request type received: \""
                 << CASE_NAME(request.type_case(), api::Request) << "\"";
      break;
  }
}

/***********************************************
              Internal Requests
***********************************************/

void Server::HandleInternalRequest(ReusableRequest&& req, MachineId /* from */) {
  if (req.get()->type_case() != internal::Request::kCompletedSubtxn) {
    LOG(ERROR) << "Unexpected request type received: \""
              << CASE_NAME(req.get()->type_case(), internal::Request) << "\"";
    return;
  }
  ProcessCompletedSubtxn(move(req));
}

void Server::ProcessCompletedSubtxn(ReusableRequest&& req) {
  auto completed_subtxn = req.get()->mutable_completed_subtxn();
  RecordTxnEvent(
      config_,
      completed_subtxn->mutable_txn()->mutable_internal(),
      TransactionEvent::RETURN_TO_SERVER);

  auto txn_id = completed_subtxn->txn().internal().id();
  if (pending_responses_.count(txn_id) == 0) {
    return;
  }
  auto& completed_txn = completed_txns_[txn_id];
  auto sub_txn_origin = completed_subtxn->partition();
  // If this is the first sub-transaction, initialize the
  // finished transaction with this sub-transaction as the starting
  // point and populate the list of partitions that we are still
  // waiting for sub-transactions from. Otherwise, remove the
  // partition of this sub-transaction from the awaiting list and
  // merge the sub-txn to the current txn.
  if (completed_txn.txn() == nullptr) {
    completed_txn.req = move(req);
    completed_txn.awaited_partitions.clear();
    for (auto p : completed_subtxn->involved_partitions()) {
      if (p != sub_txn_origin) {
        completed_txn.awaited_partitions.insert(p);
      }
    }
  } else if (completed_txn.awaited_partitions.erase(sub_txn_origin)) {
    MergeTransaction(*completed_txn.txn(), completed_subtxn->txn());
  }

  // If all sub-txns are received, response back to the client and
  // clean up all tracking data for this txn.
  if (completed_txn.awaited_partitions.empty()) {
    api::Response response;
    auto txn_response = response.mutable_txn();
    auto txn = completed_txn.txn();
    // This is ony temporary, "txn" is still owned by the request inside completed_txn
    txn_response->set_allocated_txn(txn);

    RecordTxnEvent(
        config_,
        txn->mutable_internal(),
        TransactionEvent::EXIT_SERVER_TO_CLIENT);

    SendAPIResponse(txn_id, move(response));
    // Release the txn so that it won't be freed along with the Response object
    txn_response->release_txn();
    completed_txns_.erase(txn_id);
  }
}

void Server::ProcessStatsRequest(const internal::StatsRequest& stats_request) {
  using rapidjson::StringRef;

  int level = stats_request.level();

  rapidjson::Document stats;
  stats.SetObject();
  auto& alloc = stats.GetAllocator();

  // Add stats for current transactions in the system
  stats.AddMember(StringRef(TXN_ID_COUNTER), txn_id_counter_, alloc);
  stats.AddMember(StringRef(NUM_PENDING_RESPONSES), pending_responses_.size(), alloc);
  stats.AddMember(StringRef(NUM_PARTIALLY_COMPLETED_TXNS), completed_txns_.size(), alloc);
  if (level >= 1) {
    stats.AddMember(
        StringRef(PENDING_RESPONSES),
        ToJsonArrayOfKeyValue(
            pending_responses_,
            [](const auto& resp) { return resp.stream_id; },
            alloc),
        alloc);

    stats.AddMember(
        StringRef(PARTIALLY_COMPLETED_TXNS),
        ToJsonArray(
            completed_txns_,
            [](const auto& p) { return p.first; },
            alloc),
        alloc);
  }

  rapidjson::StringBuffer buf;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
  stats.Accept(writer);

  auto res = AcquireResponse();
  res.get()->mutable_stats()->set_id(stats_request.id());
  res.get()->mutable_stats()->set_stats_json(buf.GetString());
  HandleInternalResponse(move(res), 0);
}


/***********************************************
              Internal Responses
***********************************************/

void Server::HandleInternalResponse(ReusableResponse&& res, MachineId) {
  if (res.get()->type_case() != internal::Response::kStats) {
    LOG(ERROR) << "Unexpected response type received: \""
               << CASE_NAME(res.get()->type_case(), internal::Response) << "\"";
  }
  api::Response response;
  auto stats_response = response.mutable_stats();
  stats_response->set_allocated_stats_json(
      res.get()->mutable_stats()->release_stats_json());
  SendAPIResponse(res.get()->stats().id(), std::move(response));
}

/***********************************************
                    Helpers
***********************************************/

void Server::SendAPIResponse(TxnId txn_id, api::Response&& res) {
  if (pending_responses_.count(txn_id) == 0) {
    LOG(ERROR) << "Cannot find info to response back to client for txn: " << txn_id;
    return;
  }
  auto& pr = pending_responses_[txn_id];
  auto& socket = GetCustomSocket(0);

  res.set_stream_id(pr.stream_id);

  socket.send(pr.identity, zmq::send_flags::sndmore);
  SendProtoWithEmptyDelimiter(socket, res);

  pending_responses_.erase(txn_id);
}

bool Server::ValidateTransaction(const Transaction* txn) {
  CHECK_NE(txn->read_set_size() + txn->write_set_size(), 0)
    << "Txn accesses no keys: " << txn->internal().id();

  if (txn->procedure_case() == Transaction::ProcedureCase::kRemaster) {
    CHECK_EQ(txn->read_set_size(), 0)
        << "Remaster txns should write to 1 key, txn id: " << txn->internal().id();
    CHECK_EQ(txn->write_set_size(), 1)
        << "Remaster txns should write to 1 key, txn id: " << txn->internal().id();
  }

  return true;
}

TxnId Server::NextTxnId() {
  txn_id_counter_++;
  return txn_id_counter_ * kMaxNumMachines + config_->local_machine_id();
}

} // namespace slog