#pragma once

#include <string>
#include <unordered_map>
#include "proto/config.pb.h"
#include "proto/internal.pb.h"

using std::string;
using std::unordered_map;
using std::vector;

namespace slog {

class Configuration {
public:
  static std::shared_ptr<Configuration> FromFile(
      const string& file_path,
      const string& local_address,
      const proto::SlogIdentifier& local_identifier);

  Configuration(
      const proto::Configuration& config,
      const string& local_address,
      const proto::SlogIdentifier& local_identifier);

  const string& GetProtocol() const;
  const vector<string>& GetAllAddresses() const;
  uint32_t GetBrokerPort() const;
  uint32_t GetNumReplicas() const;
  uint32_t GetNumPartitions() const;
  const string& GetLocalAddress() const;
  const proto::SlogIdentifier& GetLocalSlogId() const;

private:
  string protocol_;
  uint32_t broker_port_;
  uint32_t num_replicas_;
  uint32_t num_partitions_;
  vector<string> all_addresses_;
  string local_address_;
  proto::SlogIdentifier local_id_;

};

} // namespace slog