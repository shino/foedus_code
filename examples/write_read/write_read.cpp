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
const char* kName = "myarray";
const char* kWriteProc = "my_write_proc";
const char* kReadProc  = "my_read_proc";

foedus::ErrorStack my_write_proc(const foedus::proc::ProcArguments& args) {
  foedus::thread::Thread* context = args.context_;
  foedus::Engine* engine = args.engine_;
  foedus::storage::array::ArrayStorage array(engine, kName);
  foedus::xct::XctManager* xct_manager = engine->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, foedus::xct::kSerializable));
  char buf[kPayload] = "abcXYZ";
  WRAP_ERROR_CODE(array.overwrite_record(context, 123, buf, 0, 7));
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
  char buf[kPayload];
  WRAP_ERROR_CODE(array.get_record(context, 123, buf));
  std::cout << "record = ";
  for(int i=0; i<kPayload; ++i) {
    std::cout << buf[i];
  }
  std::cout << std::endl;
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
  foedus::ErrorStack result_write = engine.get_thread_pool()->impersonate_synchronous(kWriteProc);
  std::cout << "result_write=" << result_write << std::endl;
  foedus::ErrorStack result_read = engine.get_thread_pool()->impersonate_synchronous(kReadProc);
  std::cout << "result_read=" << result_read << std::endl;
  COERCE_ERROR(engine.uninitialize());

  return 0;
}

