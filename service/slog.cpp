#include <memory>
#include <vector>
#include <fcntl.h>

#include "common/configuration.h"
#include "common/constants.h"
#include "common/offline_data_reader.h"
#include "common/service_utils.h"
#include "common/types.h"
#include "connection/broker.h"
#include "module/consensus.h"
#include "module/forwarder.h"
#include "module/multi_home_orderer.h"
#include "module/scheduler.h"
#include "module/server.h"
#include "module/sequencer.h"
#include "module/ticker.h"
#include "proto/internal.pb.h"
#include "proto/offline_data.pb.h"
#include "storage/mem_only_storage.h"

DEFINE_string(config, "slog.conf", "Path to the configuration file");
DEFINE_string(address, "", "Address of the local machine");
DEFINE_uint32(replica, 0, "Replica number of the local machine");
DEFINE_uint32(partition, 0, "Partition number of the local machine");
DEFINE_string(data_dir, "", "Directory containing intial data");

using slog::Broker;
using slog::ConfigurationPtr;
using slog::Key;
using slog::Record;
using slog::Metadata;
using slog::MakeRunnerFor;

using std::make_shared;

void LoadData(
    slog::Storage<Key, Record>& storage,
    const ConfigurationPtr& config,
    const string& data_dir) {
  if (data_dir.empty()) {
    LOG(INFO) << "No initial data directory specified. Starting with an empty storage.";
    return;
  }

  auto data_file = data_dir + "/" + 
      std::to_string(config->GetLocalPartition()) + ".dat";

  auto fd = open(data_file.c_str(), O_RDONLY);
  if (fd < 0) {
    LOG(ERROR) << "Error while loading \"" << data_file << "\": "
               << strerror(errno)
               << ". Starting with an empty storage.";
    return;
  }

  slog::OfflineDataReader reader(fd);
  LOG(INFO) << "Loading " << reader.GetNumDatums() << " datums...";

  VLOG(1) << "First 10 datums are: ";
  int c = 10;
  while (reader.HasNextDatum()) {
    auto datum = reader.GetNextDatum();
    if (c > 0) {
      VLOG(1) << datum.key() << " " << datum.record() << " " << datum.master();
      c--;
    }

    CHECK(config->KeyIsInLocalPartition(datum.key()))
        << "Key " << datum.key()
        << " does not belong to partition " << config->GetLocalPartition();

    CHECK_LT(datum.master(), config->GetNumReplicas())
        << "Master number exceeds number of replicas";

    // Write to storage
    Record record(datum.record(), datum.master());
    storage.Write(datum.key(), record);
  }
  close(fd);
}

void GenerateData(slog::Storage<Key, Record>& storage, const ConfigurationPtr& config) {
  auto simple_partitioning = config->GetSimplePartitioning();
  auto num_records = simple_partitioning->num_records();
  auto num_threads = config->GetNumWorkers();
  auto num_partitions = config->GetNumPartitions();
  auto partition = config->GetLocalPartition();
  
  // Create a value of specified size by repeating the character 'a'
  string value(simple_partitioning->record_size_bytes(), 'a');

  LOG(INFO) << "Generating ~" << num_records/num_partitions << " records using " << num_threads << " threads. "
            << "Record size = " << simple_partitioning->record_size_bytes() << " bytes";

  auto GenerateFn = [&](uint64_t from_key, uint64_t to_key) {
    uint64_t start_key = from_key + (partition + num_partitions - from_key % num_partitions) % num_partitions;
    uint64_t end_key = std::min(to_key, num_records);
    uint64_t counter = 0;
    for (uint64_t key = start_key; key < end_key; key += num_partitions) {
      int master = config->GetMasterOfKey(key);
      Record record(value, master);
      storage.Write(std::to_string(key), record);
      counter++;
    }
  };
  std::vector<std::thread> threads;
  uint64_t range = num_records / num_threads + 1;
  for (uint32_t i = 0; i < num_threads; i++) {
    threads.emplace_back(GenerateFn, i * range, (i + 1) * range);
  }
  for (auto& t : threads) {
    t.join();
  }
}

int main(int argc, char* argv[]) {
  slog::InitializeService(&argc, &argv);
  
  auto zmq_version = zmq::version();
  LOG(INFO) << "ZMQ version "
            << std::get<0>(zmq_version) << "."
            << std::get<1>(zmq_version) << "."
            << std::get<2>(zmq_version);

#ifdef REMASTER_PROTOCOL_SIMPLE
  LOG(INFO) << "Simple remaster protocol";
#elif defined REMASTER_PROTOCOL_PER_KEY
  LOG(INFO) << "Per key remaster protocol";
#elif defined REMASTER_PROTOCOL_COUNTERLESS
  LOG(INFO) << "Counterless remaster protocol";
#else
  #error "Remaster protocol not defined"
#endif /* REMASTER_PROTOCOL_SIMPLE */

#ifdef ENABLE_DYNAMIC_REMASTER
  LOG(INFO) << "Dynamic remastering enabled";
#endif /* ENABLE_DYNAMIC_REMASTER */

#ifdef ENABLE_REPLICATION_DELAY
  LOG(INFO) << "Replication delay enabled";
#endif /* ENABLE_REPLICATION_DELAY */

  auto config = slog::Configuration::FromFile(
      FLAGS_config, 
      FLAGS_address,
      FLAGS_replica,
      FLAGS_partition);
  const auto& all_addresses = config->GetAllAddresses();
  CHECK(std::find(
      all_addresses.begin(),
      all_addresses.end(),
      config->GetLocalAddress()) != all_addresses.end())
      << "The configuration does not contain the provided "
      << "local machine ID: \"" << config->GetLocalAddress() << "\"";
  CHECK_LT(config->GetLocalReplica(), config->GetNumReplicas())
      << "Replica numbers must be within number of replicas";
  CHECK_LT(config->GetLocalPartition(), config->GetNumPartitions())
      << "Partition number must be within number of partitions";

  auto context = make_shared<zmq::context_t>(1);
  auto broker = make_shared<Broker>(config, context);

  // Create and initialize storage layer
  auto storage = make_shared<slog::MemOnlyStorage<Key, Record, Metadata>>();
  // If simple partitioning is used, generate the data;
  // otherwise, load data from an external file
  if (config->GetSimplePartitioning()) {
    GenerateData(*storage, config);
  } else {
    LoadData(*storage, config, FLAGS_data_dir);
  }

  // Create the server module. This is not added to the "modules" 
  // list below because it starts differently.
  auto server = MakeRunnerFor<slog::Server>(config, broker, storage);

  vector<unique_ptr<slog::ModuleRunner>> modules;
  modules.push_back(
      MakeRunnerFor<slog::Ticker>(*context, milliseconds(config->GetBatchDuration())));
  modules.push_back(
      MakeRunnerFor<slog::LocalPaxos>(config, broker));
  modules.push_back(
      MakeRunnerFor<slog::Forwarder>(config, broker));
  modules.push_back(
      MakeRunnerFor<slog::Sequencer>(config, broker));
  modules.push_back(
      MakeRunnerFor<slog::Scheduler>(config, broker, storage));
  
  // Only one partition per replica participates in the global paxos process
  if (config->GetLeaderPartitionForMultiHomeOrdering() 
      == config->GetLocalPartition()) {
    modules.push_back(
        MakeRunnerFor<slog::GlobalPaxos>(config, broker));
    modules.push_back(
        MakeRunnerFor<slog::MultiHomeOrderer>(config, broker));
  }

  // New modules cannot be bound to the broker after it starts so start 
  // the Broker only after it is used to initialized all modules.
  broker->StartInNewThread();
  
  // Start modules in their own threads
  for (auto& module : modules) {
    module->StartInNewThread();
  }

  // Run the server in the current main thread so that the whole process
  // does not immediately terminate after this line.
  server->Start();

  return 0;
}