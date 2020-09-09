

#include <gtest/gtest.h>
#include "common/mmessage.h"

#include "common/configuration.h"
#include "common/constants.h"
#include "common/test_utils.h"
#include "common/proto_utils.h"
#include "module/dynamic_remasterer.h"

using namespace std;
using namespace slog;

class DynamicRemastererTest : public ::testing::Test {
public:

  void SetUp() {
    auto configs = MakeTestConfigurations(
        "dynamic_remasterer", 1 /* num_replicas */, 1 /* num_partitions */);
    slog_ = make_unique<TestSlog>(configs[0]);
    // slog_->AddDynamicRemasterer();
    sender_ = slog_->GetSender();

    string endpoint = 
        "tcp://*:" + std::to_string(configs[0]->GetServerPort());
    // // TODO: get from test slog broker
    zmq::context_t context(1);
    client_socket_ = make_unique<zmq::socket_t>(context, ZMQ_ROUTER);
    // // // client_socket.setsockopt(ZMQ_LINGER, 0);
    // // // client_socket.setsockopt(ZMQ_RCVHWM, SERVER_RCVHWM);
    // // // client_socket.setsockopt(ZMQ_SNDHWM, SERVER_SNDHWM);
    client_socket_->bind(endpoint);

    slog_->StartInNewThreads();
  }

  unique_ptr<Sender> sender_;
  unique_ptr<TestSlog> slog_;
  unique_ptr<zmq::socket_t> client_socket_;
};

TEST_F(DynamicRemastererTest, TODO) {
  for (auto i = 0; i<3; i++) {
    auto txn = MakeTransaction(
      {"A", "B"},
      {"C"},
      "some code",
      {{"A", {0, 0}}, {"B", {0, 0}}, {"C", {0, 0}}},
      MakeMachineId(1,0));
    internal::Request req;
    auto forward = req.mutable_forward_txn();
    forward->set_allocated_txn(txn);
    sender_->Send(req, DYNAMIC_REMASTERER_CHANNEL);
  }

  MMessage message(*client_socket_);
}