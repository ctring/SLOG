#include "connection/poller.h"

using namespace std::chrono;

using std::optional;
using std::vector;

namespace slog {

Poller::Poller(optional<microseconds> timeout) : poll_timeout_(timeout) {}

void Poller::PushSocket(zmq::socket_t& socket) {
  poll_items_.push_back({
      socket.handle(), 0, /* fd */
      ZMQ_POLLIN, 0       /* revent */
  });
}

int Poller::Wait() {
  // Compute the time that we need to wait until the next event
  auto shortest_timeout = poll_timeout_;
  auto now = Clock::now();
  for (auto& ev : timed_callbacks_) {
    if (ev.when <= now) {
      shortest_timeout = 0us;
      break;
    } else if (!shortest_timeout.has_value() || ev.when - now < shortest_timeout.value()) {
      shortest_timeout = duration_cast<microseconds>(ev.when - now);
    }
  }

  // Wait until the next time event or some timeout.
  int rc;
  if (shortest_timeout.has_value()) {
    // By casting the timeout from microseconds to milliseconds, if it is below 1ms,
    // the casting result will be 0 and thus poll becomes non-blocking. This is intended
    // so that we spin wait instead of sleeping, making waiting more accurate.
    rc = zmq::poll(poll_items_, duration_cast<milliseconds>(shortest_timeout.value()));
  } else {
    rc = zmq::poll(poll_items_, -1);
  }

  // Process and clean up triggered callbacks
  now = Clock::now();
  for (auto it = timed_callbacks_.begin(); it != timed_callbacks_.end();) {
    if (it->when <= now) {
      it->callback();
      it = timed_callbacks_.erase(it);
    } else {
      ++it;
    }
  }

  return rc;
}

bool Poller::is_socket_ready(size_t i) const { return poll_items_[i].revents & ZMQ_POLLIN; }

void Poller::AddTimedCallback(microseconds timeout, std::function<void()>&& cb) {
  timed_callbacks_.push_back({.when = Clock::now() + timeout, .callback = move(cb)});
}

}  // namespace slog