#include "module/dynamic_remasterer.h"

#include <glog/logging.h>

#include "common/constants.h"
#include "common/proto_utils.h"
#include "proto/api.pb.h"


namespace slog {

using internal::Request;
using internal::Response;

DynamicRemasterer::DynamicRemasterer(const ConfigurationPtr& config, const shared_ptr<Broker>& broker) 
  : NetworkedModule(broker, DYNAMIC_REMASTERER_CHANNEL),
    config_(config), context_(1), server_socket_(context_, ZMQ_DEALER) {
      std::ostringstream endpoint_s;
      // if (config->GetProtocol() == "ipc") {
      //   endpoint_s << "tcp://localhost:"  << config->GetServerPort();
      // } else {
      //   endpoint_s << "tcp://" << config->GetLocalAddress() << ":" << config->GetServerPort();
      // }
      // auto endpoint = endpoint_s.str();
      string endpoint = "tcp://*:" + std::to_string(config_->GetServerPort());
      // server_socket_.setsockopt(ZMQ_SNDHWM, 0);
      // server_socket_.setsockopt(ZMQ_RCVHWM, 0);
      server_socket_.connect(endpoint);
    }

void DynamicRemasterer::HandleInternalRequest(
    Request&& req,
    string&& /* from_machine_id */) {
  if (req.type_case() != Request::kForwardTxn) {
    LOG(ERROR) << "Unexpected request type received: \""
               << CASE_NAME(req.type_case(), Request) << "\"";
    return;
  }
  VLOG(3) << "Received txn";
  auto txn = req.mutable_forward_txn()->release_txn();
  MaybeRemasterKeys(txn);
}

void DynamicRemasterer::MaybeRemasterKeys(Transaction* txn) {
  VLOG(5) << "recieved txn";
  std::unordered_set<Key> local_keys;
  for (auto& key_pair : txn->read_set()) {
    auto& key = key_pair.first;
    local_keys.insert(key);
  }
  for (auto& key_pair : txn->write_set()) {
    auto& key = key_pair.first;
    local_keys.insert(key);
  }

  auto coordinating_replica = txn->internal().coordinating_server().replica();
  auto local_access = coordinating_replica == config_->GetLocalReplica();

  for (auto& key : local_keys) {
    auto is_new_key = key_accesses_.count(key) == 0;

    // local accesses don't need to be recorded
    if (local_access) {
      if (!is_new_key) {
        // Note: could just end repeats
        key_accesses_.erase(key);
      }
      continue;
    }

    auto& access_history = key_accesses_[key];

    // if streak has been broken or is just starting
    if (is_new_key || coordinating_replica != access_history.replica) {
      access_history.replica = coordinating_replica;
      access_history.repeats = 1;
      continue;
    }

    access_history.repeats += 1;

    // TODO: place in config
    if (access_history.repeats == 3) {
        SendRemaster(key, coordinating_replica);
    }
  }

  // TODO: garbage collect non local keys
}

void DynamicRemasterer::SendRemaster(Key key, uint32_t new_replica) {
  auto remaster_txn = MakeTransaction(
    {},
    {key}, /* write set */
    "",
    {},
    config_->GetLocalMachineIdAsProto(),
    new_replica /* new master */
  );

  api::Request req;
  req.mutable_txn()->set_allocated_txn(remaster_txn);

  // Send to the server
  {
    VLOG(3) << "Dynamic remaster txn sent: " << key << " to " << new_replica;
    MMessage msg;
    msg.Push(req);
    msg.SendTo(server_socket_);
  }

  // Wait and print response
  {
    MMessage msg(server_socket_);
    api::Response res;
    if (!msg.GetProto(res)) {
      LOG(FATAL) << "Malformed response";
    } else {
      const auto& txn = res.txn().txn();
      VLOG(3) << txn;
    }
  }
}

} // namespace slog