#pragma once

#include <unordered_map>
#include <queue>
#include "common/configuration.h"
#include "common/types.h"
#include "connection/broker.h"
#include "module/base/networked_module.h"
#include "proto/transaction.pb.h"

namespace slog {

/**

 */
class DynamicRemasterer : public NetworkedModule {
public:
  DynamicRemasterer(const ConfigurationPtr& config, const shared_ptr<Broker>& broker);

protected:
  void HandleInternalRequest(
      internal::Request&& req,
      string&& from_machine_id) final;

private:
  /*
    Assumes all transactions are either local single home or lock only
  */
  void MaybeRemasterKeys(Transaction* txn);
  void SendRemaster(Key key, uint32_t new_replica);

  using AccessHistory = struct {
    uint32_t replica;
    uint32_t repeats;
  };

  std::unordered_map<Key, AccessHistory> key_accesses_;
  ConfigurationPtr config_;

  zmq::context_t context_;
  zmq::socket_t server_socket_;
};

} // namespace slog