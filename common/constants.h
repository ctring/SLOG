#pragma once

#include <string>

namespace slog {

const long BROKER_POLL_TIMEOUT_MS = 100;
const long MODULE_POLL_TIMEOUT_MS = 100;

const int SERVER_RCVHWM = 0;
const int SERVER_SNDHWM = 0;

const std::string SERVER_CHANNEL("server");
const std::string FORWARDER_CHANNEL("forwarder");
const std::string SEQUENCER_CHANNEL("sequencer");
const std::string MULTI_HOME_ORDERER_CHANNEL("multi_home_orderer");
const std::string SCHEDULER_CHANNEL("scheduler");
const std::string DYNAMIC_REMASTERER_CHANNEL("dynamic_remasterer");

const std::string LOCAL_PAXOS("paxos_local");
const std::string GLOBAL_PAXOS("paxos_global");

const uint32_t MAX_NUM_MACHINES = 1000;

const size_t MM_PROTO = 0;
const size_t MM_FROM_CHANNEL = 1;
const size_t MM_TO_CHANNEL = 2;

const uint32_t DEFAULT_MASTER_REGION_OF_NEW_KEY = 0;
const uint32_t PAXOS_DEFAULT_LEADER_POSITION = 0;

const size_t LOCK_TABLE_SIZE_LIMIT = 1000000;

/****************************
 *      Statistic Keys
 ****************************/

/* Server */
const char TXN_ID_COUNTER[] = "txn_id_counter";
const char NUM_PENDING_RESPONSES[] = "num_pending_responses";
const char NUM_PARTIALLY_COMPLETED_TXNS[] = "num_partially_completed_txns";
const char PENDING_RESPONSES[] = "pending_responses";
const char PARTIALLY_COMPLETED_TXNS[] = "partially_completed_txns";

/* Scheduler */
const char ALL_TXNS[] = "all_txns";
const char NUM_ALL_TXNS[] = "num_all_txns";
const char NUM_LOCKED_KEYS[] = "num_locked_keys";
const char NUM_TXNS_WAITING_FOR_LOCK[] = "num_txns_waiting_for_lock";
const char NUM_LOCKS_WAITED_PER_TXN[] = "num_locks_waited_per_txn";
const char LOCK_TABLE[] = "lock_table";
const char LOCAL_LOG_NUM_BUFFERED_SLOTS[] = "local_log_num_buffered_slots";
const char LOCAL_LOG_NUM_BUFFERED_BATCHES_PER_QUEUE[] = "local_log_num_buffered_batches_per_queue";
const char GLOBAL_LOG_NUM_BUFFERED_SLOTS_PER_REGION[] = "global_log_num_buffered_slots_per_region";
const char GLOBAL_LOG_NUM_BUFFERED_BATCHES_PER_REGION[] = "global_log_num_buffered_batches_per_region";

} // namespace slog