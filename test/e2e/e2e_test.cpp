#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "common/configuration.h"
#include "common/constants.h"
#include "common/test_utils.h"
#include "common/proto_utils.h"
#include "connection/broker.h"
#include "proto/api.pb.h"
#include "module/server.h"
#include "module/forwarder.h"
#include "storage/mem_only_storage.h"

using namespace std;
using namespace slog;

class E2ETest : public ::testing::Test {
protected:
  static const size_t NUM_MACHINES = 4;

  void SetUp() {
    configs = MakeTestConfigurations(
        "e2e", 2 /* num_replicas */, 2 /* num_partitions */);

    for (size_t i = 0; i < NUM_MACHINES; i++) {
      test_slogs[i] = make_unique<TestSlog>(configs[i]);

      test_slogs[i]->AddServerAndClient();
      test_slogs[i]->AddForwarder();
      test_slogs[i]->AddSequencer();
      test_slogs[i]->AddScheduler();
      test_slogs[i]->AddLocalPaxos();

      // Only one partition per replica participates in the global paxos process
      if (configs[i]->GetLeaderPartitionForMultiHomeOrdering() 
          == configs[i]->GetLocalPartition()) {
        test_slogs[i]->AddGlobalPaxos();
        test_slogs[i]->AddMultiHomeOrderer();
      }
    }

    // Replica 0
    test_slogs[0]->Data("A", {"valA", 0, 0});
    test_slogs[0]->Data("C", {"valC", 1, 1});
    test_slogs[1]->Data("B", {"valB", 0, 1});
    test_slogs[1]->Data("X", {"valX", 1, 0});
    // Replica 1
    test_slogs[2]->Data("A", {"valA", 0, 0});
    test_slogs[2]->Data("C", {"valC", 1, 1});
    test_slogs[3]->Data("B", {"valB", 0, 1});
    test_slogs[3]->Data("X", {"valX", 1, 0});

    for (const auto& test_slog : test_slogs) {
      test_slog->StartInNewThreads();
    }
  }

  unique_ptr<TestSlog> test_slogs[NUM_MACHINES];
  ConfigVec configs;
};

// This test submits multiple transactions to the system serially and checks the
// read values for correctness.
// TODO: submit transactions concurrently (multiple outcomes would be valid)
TEST_F(E2ETest, BasicSingleHomeSingleParition) {
  auto txn1 = MakeTransaction(
    {}, /* read_set */
    {"A"},  /* write_set */
    "SET A newA\n" /* code */);
  auto txn2 = MakeTransaction({"A"} /* read_set */, {}  /* write_set */);

  test_slogs[0]->SendTxn(txn1);
  auto txn1_resp = test_slogs[0]->RecvTxnResult();
  ASSERT_EQ(TransactionStatus::COMMITTED, txn1_resp.status());
  ASSERT_EQ(TransactionType::SINGLE_HOME, txn1_resp.internal().type());

  test_slogs[0]->SendTxn(txn2);
  auto txn2_resp = test_slogs[0]->RecvTxnResult();
  ASSERT_EQ(TransactionStatus::COMMITTED, txn2_resp.status());
  ASSERT_EQ(TransactionType::SINGLE_HOME, txn2_resp.internal().type());
  ASSERT_EQ("newA", txn2_resp.read_set().at("A"));
}

TEST_F(E2ETest, MultiPartitionTxn) {
  for (size_t i = 0; i < NUM_MACHINES; i++) {
    auto txn = MakeTransaction({"A", "B"} /* read_set */, {}  /* write_set */);

    test_slogs[i]->SendTxn(txn);
    auto txn_resp = test_slogs[i]->RecvTxnResult();
    ASSERT_EQ(TransactionStatus::COMMITTED, txn_resp.status());
    ASSERT_EQ(TransactionType::SINGLE_HOME, txn_resp.internal().type());
    ASSERT_EQ("valA", txn_resp.read_set().at("A"));
    ASSERT_EQ("valB", txn_resp.read_set().at("B"));
  }
}

TEST_F(E2ETest, MultiHomeTxn) {
  for (size_t i = 0; i < NUM_MACHINES; i++) {
    auto txn = MakeTransaction({"A", "C"} /* read_set */, {}  /* write_set */);

    test_slogs[i]->SendTxn(txn);
    auto txn_resp = test_slogs[i]->RecvTxnResult();
    ASSERT_EQ(TransactionStatus::COMMITTED, txn_resp.status());
    ASSERT_EQ(TransactionType::MULTI_HOME, txn_resp.internal().type());
    ASSERT_EQ("valA", txn_resp.read_set().at("A"));
    ASSERT_EQ("valC", txn_resp.read_set().at("C"));
  }
}

TEST_F(E2ETest, MultiHomeMutliPartitionTxn) {
  for (size_t i = 0; i < NUM_MACHINES; i++) {
    auto txn = MakeTransaction({"A", "X"} /* read_set */, {}  /* write_set */);

    test_slogs[i]->SendTxn(txn);
    auto txn_resp = test_slogs[i]->RecvTxnResult();
    ASSERT_EQ(TransactionStatus::COMMITTED, txn_resp.status());
    ASSERT_EQ(TransactionType::MULTI_HOME, txn_resp.internal().type());
    ASSERT_EQ("valA", txn_resp.read_set().at("A"));
    ASSERT_EQ("valX", txn_resp.read_set().at("X"));
  }
}