#pragma once

#include <condition_variable>
#include <thread>
#include <unordered_map>

#include <zmq.hpp>

#include "common/configuration.h"
#include "common/constants.h"
#include "common/mmessage.h"

using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;

namespace slog {

/**
 * A Broker distributes messages coming into a machine to the modules
 * It runs its own thread with the components depicted below
 * 
 *                   --------------------------
 *                   |                        |
 *  Module A <---- Channel A                Router  <----- Incoming Messages
 *                   |          B             |
 *                   |           R            |
 *  Module B <---- Channel B      O           |
 *                   |             K          |
 *                   |              E         |
 *  Module C <---- Channel C         R        |
 *                   |                        |
 *                   |                        |
 *                   --------------------------
 *                      ^         ^         ^
 *                      |         |         |
 *                    < Broker Synchronization >
 *                      |         |         |
 *                      |         |         |
 *  Module A  ------> Sender -----------------------> Outgoing Messages
 *                                |         |
 *  Module B  ----------------> Sender -------------> Outgoing Messages
 *                                          |
 *  Module C  --------------------------> Sender ---> Outgoing Messages
 * 
 * 
 * To receive messages from other machines, it uses a ZMQ_ROUTER socket, which constructs
 * a map from an identity to the corresponding connection. Using this identity, it can
 * tell where the message comes from.
 * 
 * The messages going into the system via the router will be brokered to the channel
 * specified in each message. On the other end of each channel is a module which also runs
 * in its own thread.
 * 
 * A module sends message to another machine via a Sender object. Each Sender object maintains
 * a weak pointer to the broker to get notified when the brokers are synchronized and to access
 * the map translating logical machine IDs to physical machine addresses.
 * 
 * Not showed above: the modules can send message to each other using Sender without going through the Broker.
 */
class Broker {
public:
  Broker(
      const ConfigurationPtr& config, 
      const shared_ptr<zmq::context_t>& context,
      long poll_timeout_ms = BROKER_POLL_TIMEOUT_MS);
  ~Broker();

  void StartInNewThread();

  string AddChannel(const string& name);

  const std::shared_ptr<zmq::context_t>& GetContext() const;

  std::string GetEndpointByMachineId(const std::string& machine_id);

  std::string GetLocalMachineId() const;

private:
  string MakeEndpoint(const string& addr = "") const;

  /**
   * A broker only starts working after every other broker is up and sends a READY
   * message to everyone. There is one caveat: if after the synchronization happens, 
   * a machine goes down, and restarts, that machine cannot join anymore since the
   * READY messages are only sent once in the beginning.
   * In a real system, HEARTBEAT messages should be periodically sent out instead
   * to mitigate this problem.
   */
  bool InitializeConnection();
  
  void Run();

  void HandleIncomingMessage(MMessage&& msg);

  zmq::pollitem_t GetRouterPollItem();

  ConfigurationPtr config_;
  shared_ptr<zmq::context_t> context_;
  long poll_timeout_ms_;
  zmq::socket_t router_;

  // Thread stuff
  std::atomic<bool> running_;
  std::thread thread_;

  // Synchronization
  bool is_synchronized_;
  std::condition_variable cv_;
  std::mutex mutex_;

  // Messages that are sent to this broker when it is not READY yet
  vector<MMessage> unhandled_incoming_messages_;
  // Map from channel name to the channel
  unordered_map<string, zmq::socket_t> channels_;

  // Map from serialized-to-string MachineIds to IP addresses
  // Used to translate the identities of outgoing messages
  std::unordered_map<std::string, std::string> machine_id_to_endpoint_;

  // This is a hack so that tests behave correctly. Ideally, these sockets
  // should be scoped within InitializeConnection(). However, if we let them
  // linger indefinitely and a test ends before the cluster is fully synchronized,
  // some of these sockets would hang up if one of their READY message recipients
  // is already terminated, leaving the READY message unconsumed in the queue and
  // in turn hanging up the cleaning up process of the test. If we don't let them
  // linger at all, some of them  might be destroyed at end of function scope and
  // the READY message does not have enought time to be sent out.
  // Putting them here solves those problems but is not ideal.
  std::vector<zmq::socket_t> tmp_sockets_;
};

} // namespace slog