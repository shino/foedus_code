#include <iostream>

#include "foedus/debugging/stop_watch.hpp"
#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/epoch.hpp"
#include "foedus/proc/proc_manager.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/array/array_metadata.hpp"
#include "foedus/storage/array/array_storage.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/xct/xct_manager.hpp"

const uint16_t kPayload = 16;
const uint32_t kRecords = 1 << 20;
const char* kName = "myarray";
const char* kWriteProc = "my_write_proc";
const char* kReadProc  = "my_read_proc";

struct WriteInput {
  uint64_t key_count;
};

foedus::ErrorStack my_write_proc(const foedus::proc::ProcArguments& args) {
  foedus::thread::Thread* context = args.context_;
  foedus::Engine* engine = args.engine_;
  foedus::storage::array::ArrayStorage array(engine, kName);
  foedus::xct::XctManager* xct_manager = engine->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, foedus::xct::kSerializable));
  const WriteInput* input = reinterpret_cast<const WriteInput*>(args.input_buffer_);
  std::cout << "write key_count=" << input->key_count << std::endl;
  for(uint64_t key=0ULL; key < input->key_count; ++key){
    WRAP_ERROR_CODE(array.overwrite_record(context, key, "xxxxx", 0, 5));
  }
  foedus::Epoch commit_epoch;
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return foedus::kRetOk;
}

foedus::ErrorStack my_read_proc(const foedus::proc::ProcArguments& args) {
  foedus::thread::Thread* context = args.context_;
  foedus::Engine* engine = args.engine_;
  foedus::storage::array::ArrayStorage array(engine, kName);
  foedus::xct::XctManager* xct_manager = engine->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, foedus::xct::kSerializable));
  const uint64_t* key_count = reinterpret_cast<const uint64_t*>(args.input_buffer_);
  std::cout << "read key_count=" << *key_count << std::endl;
  uint64_t total = 0;
  char buf[kPayload];
  for(uint64_t key=0ULL; key < *key_count; ++key){
    WRAP_ERROR_CODE(array.get_record(context, key, buf));
  // std::cout << "*************** key=" << key << std::endl;
    total += 1;
  }
  std::cout << "result_count=" << total << std::endl;
  foedus::Epoch commit_epoch;
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return foedus::kRetOk;
}

int main(int argc, char** argv) {
  // std::cout << "argc : " << argc << std::endl;
  // for(int i=0; i<argc; ++i){
  //   std::cout << "argv : " << argv[i] << std::endl;
  // }
  if(argc != 2) {
    std::cout << "Usage: big_read <key-count>" << std::endl;
    return EXIT_FAILURE;
  }
  uint64_t key_count = std::stoi(argv[1]);
  std::cout << "key_count: " << key_count << std::endl;
  foedus::EngineOptions options;
  options.cache_.snapshot_cache_size_mb_per_node_ = 1 << 6;
  options.memory_.page_pool_size_mb_per_node_ = 1 << 6;
  options.xct_.max_read_set_size_ = 1U << 20;
  options.xct_.max_write_set_size_ = 1U << 20;
  foedus::Engine engine(options);
  engine.get_proc_manager()->pre_register(kWriteProc, my_write_proc);
  engine.get_proc_manager()->pre_register(kReadProc,  my_read_proc);
  COERCE_ERROR(engine.initialize());

  foedus::UninitializeGuard guard(&engine);
  foedus::Epoch create_epoch;
  foedus::storage::array::ArrayMetadata meta(kName, kPayload, kRecords);
  COERCE_ERROR(engine.get_storage_manager()->create_storage(&meta, &create_epoch));

  foedus::thread::ImpersonateSession write_session;
  const WriteInput input = {key_count};
  foedus::debugging::StopWatch write_duration;
  bool ret_write =
    engine.get_thread_pool()->impersonate(kWriteProc, &input, sizeof(input), &write_session);
  ASSERT_ND(ret_write);
  const foedus::ErrorStack result_write = write_session.get_result();
  uint64_t write_nanosec = write_duration.stop();
  // std::cout << "write duration [usec]=" << write_nanosec / 1000ULL << std::endl;
  std::cout << "write duration [msec]=" << write_nanosec / 1000000ULL << std::endl;
  std::cout << "result_write=" << result_write << std::endl;
  write_session.release();

  foedus::thread::ImpersonateSession read_session;
  foedus::debugging::StopWatch read_duration;
  bool ret_read = engine.get_thread_pool()->impersonate(kReadProc, &key_count, sizeof(key_count), &read_session);
  ASSERT_ND(ret_read);
  foedus::ErrorStack result_read = read_session.get_result();
  uint64_t read_nanosec = read_duration.stop();
  // std::cout << "read duration [usec]=" << read_nanosec / 1000ULL << std::endl;
  std::cout << "read duration [msec]=" << read_nanosec / 1000000ULL << std::endl;
  std::cout << "result_read=" << result_read << std::endl;
  std::cout << "output_size=" << read_session.get_output_size() << std::endl;
  const char* record = reinterpret_cast<const char*>(read_session.get_raw_output_buffer());
  std::cout << "record read=" << record << " (";
  for(int i=0; i < read_session.get_output_size(); ++i) {
    std::cout << static_cast<uint64_t>(record[i]) << ", ";
  }
  std::cout << ")" << std::endl;
  read_session.release();

  COERCE_ERROR(engine.uninitialize());

  return 0;
}
