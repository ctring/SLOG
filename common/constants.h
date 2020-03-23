#pragma once

#include <string>

namespace slog {

const long BROKER_POLL_TIMEOUT_MS = 100;
const long MODULE_POLL_TIMEOUT_MS = 100;

const std::string SERVER_CHANNEL("server");
const std::string FORWARDER_CHANNEL("forwarder");
const std::string SEQUENCER_CHANNEL("sequencer");
const std::string MULTI_HOME_ORDERER_CHANNEL("multi_home_orderer");
const std::string SCHEDULER_CHANNEL("scheduler");

const std::string LOCAL_PAXOS("local");
const std::string GLOBAL_PAXOS("global");

const uint32_t MAX_NUM_MACHINES = 1000;

const size_t MM_PROTO = 0;
const size_t MM_FROM_CHANNEL = 1;
const size_t MM_TO_CHANNEL = 2;

const uint32_t DEFAULT_MASTER_REGION_OF_NEW_KEY = 0;
const uint32_t PAXOS_DEFAULT_LEADER_POSITION = 0;

const size_t LOCK_TABLE_SIZE_LIMIT = 1000000;

/****************************
 *      Statistics Keys
 ****************************/

const char ALL_TXNS[] = "all_txns";
const char NUM_ALL_TXNS[] = "num_all_txns";
const char NUM_READY_WORKERS[] = "num_ready_workers";
const char NUM_READY_TXNS[] = "num_ready_txns";
const char NUM_LOCKED_KEYS[] = "num_locked_keys";
const char NUM_TXNS_WAITING_FOR_LOCK[] = "num_txns_waiting_for_lock";
const char NUM_LOCKS_WAITED_PER_TXN[] = "num_locks_waited_per_txn";

} // namespace slog