#include <gtest/gtest.h>

#include "common/test_utils.h"
#include "common/proto_utils.h"
#include "module/scheduler_components/deterministic_lock_manager.h"

using namespace std;
using namespace slog;

TEST(DeterministicLockManagerTest, GetAllLocksOnFirstTry) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  DeterministicLockManager lock_manager(configs[0]);
  auto txn = MakeTransaction(
    {"readA", "readB"},
    {"writeC"});
  ASSERT_TRUE(lock_manager.RegisterTxnAndAcquireLocks(txn));
  auto new_holders = lock_manager.ReleaseLocks(txn);
  ASSERT_TRUE(new_holders.empty());
}

TEST(DeterministicLockManagerTest, GetAllLocksMultiPartitions) {
  auto configs = MakeTestConfigurations("locking", 1, 2);
  DeterministicLockManager lock_manager(configs[0]);
  // "AAAA" is in partition 0 so lock is acquired
  auto txn1 = MakeTransaction({"readX"}, {"AAAA"});
  txn1.mutable_internal()->set_id(100);
  // "ZZZZ" is in partition 1 so is ignored
  auto txn2 = MakeTransaction({"readX"}, {"ZZZZ"});
  txn2.mutable_internal()->set_id(200);
  ASSERT_TRUE(lock_manager.RegisterTxnAndAcquireLocks(txn1));
  ASSERT_FALSE(lock_manager.RegisterTxnAndAcquireLocks(txn2));
}

TEST(DeterministicLockManager, ReadLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  DeterministicLockManager lock_manager(configs[0]);
  auto txn1 = MakeTransaction({"readA", "readB"}, {});
  txn1.mutable_internal()->set_id(100);
  auto txn2 = MakeTransaction({"readB", "readC"}, {});
  txn2.mutable_internal()->set_id(200);
  ASSERT_TRUE(lock_manager.RegisterTxnAndAcquireLocks(txn1));
  ASSERT_TRUE(lock_manager.RegisterTxnAndAcquireLocks(txn2));
  ASSERT_TRUE(lock_manager.ReleaseLocks(txn1).empty());
  ASSERT_TRUE(lock_manager.ReleaseLocks(txn2).empty());
}

TEST(DeterministicLockManager, WriteLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  DeterministicLockManager lock_manager(configs[0]);
  auto txn1 = MakeTransaction({}, {"writeA", "writeB"});
  txn1.mutable_internal()->set_id(100);
  auto txn2 = MakeTransaction({"readA"}, {"writeA"});
  txn2.mutable_internal()->set_id(200);
  ASSERT_TRUE(lock_manager.RegisterTxnAndAcquireLocks(txn1));
  ASSERT_FALSE(lock_manager.RegisterTxnAndAcquireLocks(txn2));
  // The blocked txn becomes ready
  ASSERT_EQ(lock_manager.ReleaseLocks(txn1).size(), 1);
  // Make sure the lock is already held by txn2
  ASSERT_FALSE(lock_manager.RegisterTxnAndAcquireLocks(txn1));
}

TEST(DeterministicLockManager, ReleaseLocksAndGetManyNewHolders) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  DeterministicLockManager lock_manager(configs[0]);
  auto txn1 = MakeTransaction({"A"}, {"B", "C"});
  txn1.mutable_internal()->set_id(100);
  auto txn2 = MakeTransaction({"B"}, {"A"});
  txn2.mutable_internal()->set_id(200);
  auto txn3 = MakeTransaction({"B"}, {});
  txn3.mutable_internal()->set_id(300);
  auto txn4 = MakeTransaction({"C"}, {});
  txn4.mutable_internal()->set_id(400);

  ASSERT_TRUE(lock_manager.RegisterTxnAndAcquireLocks(txn1));
  ASSERT_FALSE(lock_manager.RegisterTxnAndAcquireLocks(txn2));
  ASSERT_FALSE(lock_manager.RegisterTxnAndAcquireLocks(txn3));
  ASSERT_FALSE(lock_manager.RegisterTxnAndAcquireLocks(txn4));

  ASSERT_TRUE(lock_manager.ReleaseLocks(txn3).empty());

  auto new_ready_txns = lock_manager.ReleaseLocks(txn1);
  // Txn 300 was removed from the wait list due to the
  // ReleaseLocks call above
  ASSERT_EQ(new_ready_txns.size(), 2);
  ASSERT_TRUE(new_ready_txns.count(200) > 0);
  ASSERT_TRUE(new_ready_txns.count(400) > 0);
}

TEST(DeterministicLockManager, PartiallyAcquiredLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  DeterministicLockManager lock_manager(configs[0]);
  auto txn1 = MakeTransaction({"A"}, {"B", "C"});
  txn1.mutable_internal()->set_id(100);
  auto txn2 = MakeTransaction({"A"}, {"B"});
  txn2.mutable_internal()->set_id(200);
  auto txn3 = MakeTransaction({}, {"A", "C"});
  txn3.mutable_internal()->set_id(300);

  ASSERT_TRUE(lock_manager.RegisterTxnAndAcquireLocks(txn1));
  ASSERT_FALSE(lock_manager.RegisterTxnAndAcquireLocks(txn2));
  ASSERT_FALSE(lock_manager.RegisterTxnAndAcquireLocks(txn3));

  auto new_ready_txns = lock_manager.ReleaseLocks(txn1);
  ASSERT_EQ(new_ready_txns.size(), 1);
  ASSERT_TRUE(new_ready_txns.count(200) > 0);

  new_ready_txns = lock_manager.ReleaseLocks(txn2);
  ASSERT_EQ(new_ready_txns.size(), 1);
  ASSERT_TRUE(new_ready_txns.count(300) > 0);
}

TEST(DeterministicLockManager, PrioritizeWriteLock) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  DeterministicLockManager lock_manager(configs[0]);
  auto txn1 = MakeTransaction({"A"}, {"A"});
  txn1.mutable_internal()->set_id(100);
  auto txn2 = MakeTransaction({"A"}, {});
  txn2.mutable_internal()->set_id(200);
 
  ASSERT_TRUE(lock_manager.RegisterTxnAndAcquireLocks(txn1));
  ASSERT_FALSE(lock_manager.RegisterTxnAndAcquireLocks(txn2));

  auto new_ready_txns = lock_manager.ReleaseLocks(txn1);
  ASSERT_EQ(new_ready_txns.size(), 1);
  ASSERT_TRUE(new_ready_txns.count(200) > 0);
}

TEST(DeterministicLockManager, AcquireLocksWithLockOnlyTxn1) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  DeterministicLockManager lock_manager(configs[0]);
  auto txn1 = MakeTransaction({"A"}, {"B", "C"});
  txn1.mutable_internal()->set_id(100);
  auto txn2 = MakeTransaction({"A"}, {"B"});
  txn2.mutable_internal()->set_id(200);
  auto txn2_lockonly1 = MakeTransaction({}, {"B"});
  txn2_lockonly1.mutable_internal()->set_id(200);
  auto txn2_lockonly2 = MakeTransaction({"A"}, {});
  txn2_lockonly2.mutable_internal()->set_id(200);

  ASSERT_FALSE(lock_manager.RegisterTxn(txn1));
  ASSERT_FALSE(lock_manager.RegisterTxn(txn2));
  ASSERT_FALSE(lock_manager.AcquireLocks(txn2_lockonly1));
  ASSERT_FALSE(lock_manager.AcquireLocks(txn1));
  ASSERT_TRUE(lock_manager.AcquireLocks(txn2_lockonly2));

  auto new_ready_txns = lock_manager.ReleaseLocks(txn2);
  ASSERT_EQ(new_ready_txns.size(), 1);
  ASSERT_TRUE(new_ready_txns.count(100) > 0);
}

TEST(DeterministicLockManager, AcquireLocksWithLockOnlyTxn2) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  DeterministicLockManager lock_manager(configs[0]);
  auto txn1 = MakeTransaction({"A"}, {"B", "C"});
  txn1.mutable_internal()->set_id(100);
  auto txn2 = MakeTransaction({"A"}, {"B"});
  txn2.mutable_internal()->set_id(200);
  auto txn2_lockonly1 = MakeTransaction({}, {"B"});
  txn2_lockonly1.mutable_internal()->set_id(200);
  auto txn2_lockonly2 = MakeTransaction({"A"}, {});
  txn2_lockonly2.mutable_internal()->set_id(200);

  ASSERT_FALSE(lock_manager.AcquireLocks(txn2_lockonly1));
  ASSERT_FALSE(lock_manager.AcquireLocks(txn1));
  ASSERT_FALSE(lock_manager.AcquireLocks(txn2_lockonly2));
  ASSERT_FALSE(lock_manager.RegisterTxn(txn1));
  ASSERT_TRUE(lock_manager.RegisterTxn(txn2));

  auto new_ready_txns = lock_manager.ReleaseLocks(txn2);
  ASSERT_EQ(new_ready_txns.size(), 1);
  ASSERT_TRUE(new_ready_txns.count(100) > 0);
}

TEST(DeterministicLockManager, AcquireLocksWithLockOnlyTxnOutOfOrder) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  DeterministicLockManager lock_manager(configs[0]);
  auto txn1 = MakeTransaction({"A"}, {"B", "C"});
  txn1.mutable_internal()->set_id(100);
  auto txn2 = MakeTransaction({"A"}, {"B"});
  txn2.mutable_internal()->set_id(200);
  auto txn2_lockonly1 = MakeTransaction({}, {"B"});
  txn2_lockonly1.mutable_internal()->set_id(200);
  auto txn2_lockonly2 = MakeTransaction({"A"}, {});
  txn2_lockonly2.mutable_internal()->set_id(200);

  ASSERT_FALSE(lock_manager.AcquireLocks(txn2_lockonly1));
  ASSERT_FALSE(lock_manager.RegisterTxn(txn2));
  ASSERT_FALSE(lock_manager.AcquireLocks(txn1));
  ASSERT_FALSE(lock_manager.RegisterTxn(txn1));
  ASSERT_TRUE(lock_manager.AcquireLocks(txn2_lockonly2));

  auto new_ready_txns = lock_manager.ReleaseLocks(txn2);
  ASSERT_EQ(new_ready_txns.size(), 1);
  ASSERT_TRUE(new_ready_txns.count(100) > 0);
}

TEST(DeterministicLockManager, GhostTxns) {
  auto configs = MakeTestConfigurations("locking", 1, 2);
  DeterministicLockManager lock_manager(configs[0]);
  // "X" is in partition 1
  auto txn1 = MakeTransaction({}, {"X"});
  txn1.mutable_internal()->set_id(100);
  ASSERT_FALSE(lock_manager.RegisterTxn(txn1));

  // "Z" is in partition 1
  auto txn2 = MakeTransaction({"Z"}, {});
  txn2.mutable_internal()->set_id(101);
  ASSERT_FALSE(lock_manager.AcquireLocks(txn2));
}