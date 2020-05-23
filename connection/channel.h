#pragma once

#include <zmq.hpp>

#include "common/mmessage.h"

namespace slog {

/**
 * A Channel is the interface between a module (e.g. Sequencer) and the
 * network layer. The Broker uses channels to pass message from the outside
 * into the internal modules. To read the messages, a module need to obtain
 * the listener corresponding to a channel and poll messages via the listener.
 */
class Channel {
public:
  Channel(const std::shared_ptr<zmq::context_t>& context, const std::string& name);
  Channel(const Channel&) = delete;
  Channel& operator=(const Channel&) = delete;

  /**
   * Returns the name of current channel
   */
  const std::string& GetName() const;

  /**
   * Returns a zmq pollitem data structure for this channel.
   */
  zmq::pollitem_t GetPollItem();

  /**
   * Passes a message to this channel
   */
  void Send(const MMessage& msg, bool has_more = false);

  /**
   * Receives a message from this channel
   */
  bool Receive(MMessage& msg);

  /**
   * Returns a unique pointer to the listener corresponding to
   * this channel.
   */
  std::unique_ptr<Channel> GetListener();

  /**
   * Returns the zmq context
   */
  const std::shared_ptr<zmq::context_t>& GetContext() const;

private:
  Channel(
      const std::shared_ptr<zmq::context_t>& context,
      const std::string& name,
      bool is_listener);

  std::shared_ptr<zmq::context_t> context_;
  const std::string name_;
  zmq::socket_t socket_;
  const bool is_listener_;
  std::atomic<bool> listener_created_;
};

} // namespace slog