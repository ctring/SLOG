project(test)

# Don't forget to add all tests here so that they are linked properly
set(ALL_TESTS
  batch_log_test
  batch_interleaver_test
  broker_and_sender_test
  commands_test
  concurrent_hash_map_test
  deterministic_lock_manager_test
  deterministic_lock_manager_deprecated_test
  dynamic_remasterer_test
  e2e_test
  forwarder_test
  mem_only_storage_test
  mmessage_test
  paxos_test
  per_key_remaster_manager_test
  proto_utils_test
  scheduler_test
  server_test
  sequencer_test
  simple_remaster_manager_test
  string_utils_test
  transaction_holder_test)

add_executable(batch_log_test data_structure/batch_log_test.cpp)
target_link_libraries(batch_log_test batch_log proto)
  
add_executable(batch_interleaver_test
  module/scheduler_components/batch_interleaver_test.cpp)
target_link_libraries(batch_interleaver_test scheduler_components proto)

add_executable(broker_and_sender_test connection/broker_and_sender_test.cpp)
target_link_libraries(broker_and_sender_test test_utils)

add_executable(commands_test module/scheduler_components/commands_test.cpp)
target_link_libraries(commands_test scheduler_components test_utils)

add_executable(concurrent_hash_map_test data_structure/concurrent_hash_map_test.cpp)

add_executable(deterministic_lock_manager_test 
  module/scheduler_components/deterministic_lock_manager_test.cpp)
target_link_libraries(deterministic_lock_manager_test test_utils)

add_executable(deterministic_lock_manager_deprecated_test 
  module/scheduler_components/deterministic_lock_manager_deprecated_test.cpp)
target_link_libraries(deterministic_lock_manager_deprecated_test test_utils)

add_executable(dynamic_remasterer_test module/dynamic_remasterer_test.cpp)
target_link_libraries(dynamic_remasterer_test test_utils)

add_executable(e2e_test e2e/e2e_test.cpp)
target_link_libraries(e2e_test test_utils)

add_executable(forwarder_test module/forwarder_test.cpp)
target_link_libraries(forwarder_test test_utils)

add_executable(mem_only_storage_test storage/mem_only_storage_test.cpp)
target_link_libraries(mem_only_storage_test)

add_executable(mmessage_test common/mmessage_test.cpp)
target_link_libraries(mmessage_test test_utils)

add_executable(paxos_test paxos/paxos_test.cpp)
target_link_libraries(paxos_test test_utils paxos)

add_executable(per_key_remaster_manager_test
  module/scheduler_components/per_key_remaster_manager_test.cpp)
target_link_libraries(per_key_remaster_manager_test test_utils)

add_executable(proto_utils_test common/proto_utils_test.cpp)
target_link_libraries(proto_utils_test common proto)

add_executable(scheduler_test module/scheduler_test.cpp)
target_link_libraries(scheduler_test test_utils)

add_executable(server_test module/server_test.cpp)
target_link_libraries(server_test test_utils)

add_executable(sequencer_test module/sequencer_test.cpp)
target_link_libraries(sequencer_test test_utils)

add_executable(simple_remaster_manager_test 
  module/scheduler_components/simple_remaster_manager_test.cpp)
target_link_libraries(simple_remaster_manager_test test_utils)

add_executable(string_utils_test common/string_utils_test.cpp)
target_link_libraries(string_utils_test string_utils)

add_executable(transaction_holder_test common/transaction_holder_test.cpp)
target_link_libraries(transaction_holder_test common test_utils)

foreach(TEST ${ALL_TESTS})
  target_link_libraries(${TEST} ${EXTERNAL_LIBS} GTest::GTest GTest::Main)
  add_test(
    NAME ${TEST}
    COMMAND ${TEST})
endforeach(TEST)