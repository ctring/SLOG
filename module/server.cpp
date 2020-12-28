#include "module/server.h"

#include "common/constants.h"
#include "common/json_utils.h"
#include "connection/zmq_utils.h"
#include "proto/internal.pb.h"

using std::move;

namespace slog {

void ValidateTransaction(Transaction* txn) {
  txn->set_status(TransactionStatus::ABORTED);
  if (txn->read_set_size() + txn->write_set_size() == 0) {
    txn->set_abort_reason("Txn accesses no key");
    return;
  }
  if (txn->procedure_case() == Transaction::ProcedureCase::kRemaster) {
    if (txn->read_set_size() > 0) {
      txn->set_abort_reason("Remaster txns should not read anything");
      return;
    }
    if (txn->write_set_size() != 1) {
      txn->set_abort_reason("Remaster txns should write to 1 key");
      return;
    }
  }
  txn->set_status(TransactionStatus::NOT_STARTED);
}

Server::Server(const ConfigurationPtr& config, const shared_ptr<Broker>& broker, std::chrono::milliseconds poll_timeout)
    : NetworkedModule("Server", broker, kServerChannel, poll_timeout), config_(config), txn_id_counter_(0) {}

/***********************************************
                Custom socket
***********************************************/

std::vector<zmq::socket_t> Server::InitializeCustomSockets() {
  string endpoint = "tcp://*:" + std::to_string(config_->server_port());
  zmq::socket_t client_socket(*context(), ZMQ_ROUTER);
  client_socket.set(zmq::sockopt::linger, 0);
  client_socket.set(zmq::sockopt::rcvhwm, 0);
  client_socket.set(zmq::sockopt::sndhwm, 0);
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
  auto res = pending_responses_.try_emplace(txn_id, move(identity), request.stream_id());
  DCHECK(res.second) << "Duplicate transaction id: " << txn_id;

  switch (request.type_case()) {
    case api::Request::kTxn: {
      auto txn = request.mutable_txn()->release_txn();
      auto txn_internal = txn->mutable_internal();
      RecordTxnEvent(config_, txn_internal, TransactionEvent::ENTER_SERVER);
      txn_internal->set_id(txn_id);
      txn_internal->set_coordinating_server(config_->local_machine_id());

      ValidateTransaction(txn);
      if (txn->status() == TransactionStatus::ABORTED) {
        SendTxnToClient(txn);
        break;
      }

      // Send to forwarder
      internal::Request forward_request;
      forward_request.mutable_forward_txn()->set_allocated_txn(txn);
      RecordTxnEvent(config_, txn_internal, TransactionEvent::EXIT_SERVER_TO_FORWARDER);
      Send(forward_request, kForwarderChannel);
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
      LOG(ERROR) << "Unexpected request type received: \"" << CASE_NAME(request.type_case(), api::Request) << "\"";
      break;
  }
}

/***********************************************
              Internal Requests
***********************************************/

void Server::HandleInternalRequest(ReusableRequest&& req, MachineId /* from */) {
  if (req.get()->type_case() != internal::Request::kCompletedSubtxn) {
    LOG(ERROR) << "Unexpected request type received: \"" << CASE_NAME(req.get()->type_case(), internal::Request)
               << "\"";
    return;
  }
  ProcessCompletedSubtxn(move(req));
}

void Server::ProcessCompletedSubtxn(ReusableRequest&& req) {
  auto completed_subtxn = req.get()->mutable_completed_subtxn();
  RecordTxnEvent(config_, completed_subtxn->mutable_txn()->mutable_internal(), TransactionEvent::RETURN_TO_SERVER);

  auto txn_id = completed_subtxn->txn().internal().id();
  if (pending_responses_.count(txn_id) == 0) {
    return;
  }

  auto res = completed_txns_.try_emplace(txn_id, config_, completed_subtxn->num_involved_partitions());
  auto& completed_txn = res.first->second;
  if (completed_txn.AddSubTxn(std::move(req))) {
    SendTxnToClient(completed_txn.ReleaseTxn());
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
    stats.AddMember(StringRef(PENDING_RESPONSES),
                    ToJsonArrayOfKeyValue(
                        pending_responses_, [](const auto& resp) { return resp.stream_id; }, alloc),
                    alloc);

    stats.AddMember(StringRef(PARTIALLY_COMPLETED_TXNS),
                    ToJsonArray(
                        completed_txns_, [](const auto& p) { return p.first; }, alloc),
                    alloc);
  }

  rapidjson::StringBuffer buf;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
  stats.Accept(writer);

  auto res = NewResponse();
  res.get()->mutable_stats()->set_id(stats_request.id());
  res.get()->mutable_stats()->set_stats_json(buf.GetString());
  HandleInternalResponse(move(res), 0);
}

/***********************************************
              Internal Responses
***********************************************/

void Server::HandleInternalResponse(ReusableResponse&& res, MachineId) {
  if (res.get()->type_case() != internal::Response::kStats) {
    LOG(ERROR) << "Unexpected response type received: \"" << CASE_NAME(res.get()->type_case(), internal::Response)
               << "\"";
  }
  api::Response response;
  auto stats_response = response.mutable_stats();
  stats_response->set_allocated_stats_json(res.get()->mutable_stats()->release_stats_json());
  SendResponseToClient(res.get()->stats().id(), std::move(response));
}

/***********************************************
                    Helpers
***********************************************/

void Server::SendTxnToClient(Transaction* txn) {
  RecordTxnEvent(config_, txn->mutable_internal(), TransactionEvent::EXIT_SERVER_TO_CLIENT);

  api::Response response;
  auto txn_response = response.mutable_txn();
  txn_response->set_allocated_txn(txn);
  SendResponseToClient(txn->internal().id(), move(response));
}

void Server::SendResponseToClient(TxnId txn_id, api::Response&& res) {
  auto it = pending_responses_.find(txn_id);
  if (it == pending_responses_.end()) {
    LOG(ERROR) << "Cannot find info to response back to client for txn: " << txn_id;
    return;
  }
  auto& socket = GetCustomSocket(0);
  // Stream id is for the client to match request/response
  res.set_stream_id(it->second.stream_id);
  // Send identity to the socket to select the client to response to
  socket.send(it->second.identity, zmq::send_flags::sndmore);
  // Send the actual message
  SendProtoWithEmptyDelimiter(socket, res);

  pending_responses_.erase(txn_id);
}

TxnId Server::NextTxnId() {
  txn_id_counter_++;
  return txn_id_counter_ * kMaxNumMachines + config_->local_machine_id();
}

}  // namespace slog