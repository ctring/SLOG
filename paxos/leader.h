#pragma once

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/types.h"
#include "paxos/quorum_tracker.h"

using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;

namespace slog {

class SimpleMultiPaxos;

struct Proposal {
  Proposal()
    : ballot(0), value(0), is_committed(false) {}

  Proposal(uint32_t ballot, uint32_t value)
    : ballot(ballot), value(value), is_committed(false) {}

  uint32_t ballot;
  uint32_t value;
  bool is_committed;
};

class Leader {
public:
  /**
   * @param paxos   The enclosing Paxos class
   * @param members Machine Id of all members participating in this Paxos process
   * @param me      Machine Id of the current machine
   */
  Leader(
      SimpleMultiPaxos& paxos,
      const vector<string>& members,
      const string& me);

  void HandleRequest(const internal::Request& req);

  void HandleResponse(
      const internal::Response& res,
      const string& from_machine_id);
  
  bool IsMember() const;

private:
  void ProcessCommitRequest(const internal::PaxosCommitRequest commit);

  void StartNewAcceptance(uint32_t value);
  void AcceptanceStateChanged(const AcceptanceTracker* acceptance);

  void StartNewCommit(SlotId slot);
  void CommitStateChanged(const CommitTracker* commit);

  void SendToAllMembers(const internal::Request& request);

  SimpleMultiPaxos& paxos_;

  const vector<string> members_;
  const string me_;
  bool is_elected_;
  bool is_member_;
  string elected_leader_;

  SlotId min_uncommitted_slot_;
  SlotId next_empty_slot_;
  uint32_t ballot_;
  unordered_map<SlotId, Proposal> proposals_;
  vector<unique_ptr<QuorumTracker>> quorum_trackers_;
};
} // namespace slog