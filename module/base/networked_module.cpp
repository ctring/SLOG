#include "module/base/networked_module.h"

#include "common/constants.h"
#include "connection/broker.h"
#include "connection/sender.h"

using std::move;
using std::unique_ptr;
using std::vector;

namespace slog {

using internal::Request;
using internal::Response;

NetworkedModule::NetworkedModule(
    const std::shared_ptr<Broker>& broker,
    const std::string& name)
  : context_(broker->GetContext()),
    name_(name),
    pull_socket_(*context_, ZMQ_PULL),
    sender_(broker) {
  broker->AddChannel(name);
  pull_socket_.bind("inproc://" + name);
  pull_socket_.setsockopt(ZMQ_LINGER, 0);
  // Remove limit on the zmq message queues
  pull_socket_.setsockopt(ZMQ_RCVHWM, 0);

  poll_items_.push_back({
    static_cast<void*>(pull_socket_),
    0, /* fd */
    ZMQ_POLLIN,
    0 /* revent */
  });
}

vector<zmq::socket_t> NetworkedModule::InitializeCustomSockets() {
  return {};
}

zmq::socket_t& NetworkedModule::GetCustomSocket(size_t i) {
  return custom_sockets_[i];
}

const std::shared_ptr<zmq::context_t> NetworkedModule::GetContext() const {
  return context_;
}

const std::string& NetworkedModule::GetName() const {
  return name_;
}

void NetworkedModule::SetUp() {
  custom_sockets_ = InitializeCustomSockets();
  for (auto& socket : custom_sockets_) {
    poll_items_.push_back({ 
      static_cast<void*>(socket),
      0, /* fd */
      ZMQ_POLLIN,
      0 /* revent */
    });
  }
}

void NetworkedModule::Loop() {
  if (!zmq::poll(poll_items_, MODULE_POLL_TIMEOUT_MS)) {
    return;
  }

  // Message from pull socket
  if (poll_items_[0].revents & ZMQ_POLLIN) {
    MMessage message(pull_socket_);
    auto from_machine_id = message.GetIdentity();

    if (message.IsProto<Request>()) {
      Request req;
      message.GetProto(req);

      HandleInternalRequest(
          move(req),
          move(from_machine_id));

    } else if (message.IsProto<Response>()) {
      Response res;
      message.GetProto(res);

      HandleInternalResponse(
          move(res),
          move(from_machine_id));
    }
  }

  // Message from one of the custom sockets. These sockets
  // are indexed from 1 in poll_items_. The first poll item
  // belongs to the channel socket.
  for (size_t i = 1; i < poll_items_.size(); i++) {
    if (poll_items_[i].revents & ZMQ_POLLIN) {
      MMessage msg(custom_sockets_[i - 1]);
      HandleCustomSocketMessage(msg, i - 1 /* socket_index */);
    }
  }
}

void NetworkedModule::Send(
    const google::protobuf::Message& request_or_response,
    const std::string& to_channel,
    const std::string& to_machine_id) {
  sender_.Send(request_or_response, to_channel, to_machine_id);
}

void NetworkedModule::Send(
    const google::protobuf::Message& request_or_response,
    const std::string& to_channel) {
  sender_.Send(request_or_response, to_channel);
}

} // namespace slog