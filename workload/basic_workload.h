#pragma once

#include <vector>

#include "workload/workload_generator.h"
#include "common/configuration.h"
#include "common/types.h"
#include "proto/transaction.pb.h"

using std::vector;

namespace slog {

class KeyList {
public:
  KeyList(size_t num_hot_keys = 0);

  void AddKey(Key key);
  Key GetRandomHotKey();
  Key GetRandomColdKey();

private:
  size_t num_hot_keys_;
  vector<Key> cold_keys_;
  vector<Key> hot_keys_;

  std::mt19937 re_;
};

class BasicWorkload : public WorkloadGenerator {
public:
  BasicWorkload(
      const ConfigurationPtr& config,
      const std::string& data_dir,
      const std::string& params_str);

  std::pair<Transaction*, TransactionProfile> NextTransaction() final;

private:
  ConfigurationPtr config_;

  // This is an index of keys by their partition and home.
  // Each partition holds a vector of homes, each of which
  // is a list of keys.
  vector<vector<KeyList>> partition_to_key_lists_;

  std::mt19937 re_;

  TxnId client_txn_id_counter_;
};

} // namespace slog