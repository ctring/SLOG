project(module)

add_library(base_module 
  base/networked_module.cpp
  base/module.cpp)
target_link_libraries(base_module
  common
  connection
  proto)

add_library(scheduler_components
  scheduler_components/batch_interleaver.cpp
  scheduler_components/commands.cpp
  scheduler_components/deterministic_lock_manager.cpp
  scheduler_components/deterministic_lock_manager_deprecated.cpp
  scheduler_components/per_key_remaster_manager.cpp
  scheduler_components/simple_remaster_manager.cpp
  scheduler_components/worker.cpp)
target_link_libraries(scheduler_components common string_utils)

add_library(module
  consensus.cpp
  dynamic_remasterer.cpp
  forwarder.cpp
  multi_home_orderer.cpp
  server.cpp
  sequencer.cpp
  scheduler.cpp)
target_link_libraries(module
  batch_log
  base_module
  paxos
  scheduler_components
  ticker)

add_library(ticker ticker.cpp)
target_link_libraries(ticker base_module common)