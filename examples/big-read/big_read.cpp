#include <iostream>

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
// const uint32_t kCount = 2;
// const uint32_t kCount = 1000;    // 1k
const uint32_t kCount = 1000000; // 1m
const char* kName = "myarray";
const char* kWriteProc = "my_write_proc";
const char* kReadProc  = "my_read_proc";

struct WriteInput {
  uint64_t key;
  const char* payload;
  uint16_t    payload_size;
};

foedus::ErrorStack my_write_proc(const foedus::proc::ProcArguments& args) {
  foedus::thread::Thread* context = args.context_;
  foedus::Engine* engine = args.engine_;
  foedus::storage::array::ArrayStorage array(engine, kName);
  foedus::xct::XctManager* xct_manager = engine->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, foedus::xct::kSerializable));
  const WriteInput* input = reinterpret_cast<const WriteInput*>(args.input_buffer_);
  for(uint64_t key=0ULL; key<kCount; ++key){
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
  const uint64_t* key = reinterpret_cast<const uint64_t*>(args.input_buffer_);
  uint64_t total = 0;
  char buf[kPayload];
  for(uint64_t key=0ULL; key<kCount; ++key){
    WRAP_ERROR_CODE(array.get_record(context, key, buf));
  std::cout << "*************** key=" << key << std::endl;
    total += (key + 1);
  }
  std::cout << "result_total=" << total << std::endl;
  foedus::Epoch commit_epoch;
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return foedus::kRetOk;
}

int main(int argc, char** argv) {
  foedus::EngineOptions options;
  options.cache_.snapshot_cache_size_mb_per_node_ = 1 << 6;
  options.memory_.page_pool_size_mb_per_node_ = 1 << 6;
  foedus::Engine engine(options);
  engine.get_proc_manager()->pre_register(kWriteProc, my_write_proc);
  engine.get_proc_manager()->pre_register(kReadProc,  my_read_proc);
  COERCE_ERROR(engine.initialize());

  foedus::UninitializeGuard guard(&engine);
  foedus::Epoch create_epoch;
  foedus::storage::array::ArrayMetadata meta(kName, kPayload, kRecords);
  COERCE_ERROR(engine.get_storage_manager()->create_storage(&meta, &create_epoch));

  foedus::thread::ImpersonateSession write_session;
  const WriteInput input = {123, "abcXYZ", 7};
  bool ret_write =
    engine.get_thread_pool()->impersonate(kWriteProc, &input, sizeof(input), &write_session);
  ASSERT_ND(ret_write);
  const foedus::ErrorStack result_write = write_session.get_result();
  std::cout << "result_write=" << result_write << std::endl;
  write_session.release();

  foedus::thread::ImpersonateSession read_session;
  bool ret_read = engine.get_thread_pool()->impersonate(kReadProc, &input.key, sizeof(input.key), &read_session);
  ASSERT_ND(ret_read);
  foedus::ErrorStack result_read = read_session.get_result();
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
