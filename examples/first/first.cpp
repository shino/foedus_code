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
const char* kProc = "myproc";

foedus::ErrorStack my_proc(const foedus::proc::ProcArguments& args) {
  foedus::Engine* engine = args.engine_;
  foedus::storage::array::ArrayStorage array(engine, kName);
  foedus::xct::XctManager* xct_manager = engine->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, foedus::xct::kSerializable));
  char buf[kPayload];
  WRAP_ERROR_CODE(array.get_record(context, 123, buf));
  foedus::Epoch commit_epoch;
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return foedus::kRetOk;
}

int main(int argc, char argv) {
  foedus::EngineOptions options;
  foedus::Engine engine(options);
  engine.get_proc_manager()->pre_register(kProc, my_proc);
  COERCE_ERROR(engine.initialize());

  foedus::UninitializeGuard guard(&engine);
  foedus::Epoch create_epoch;
  foedus::storage::array::ArrayMetadata meta(kName, kPayload, kRecords);
  COERCE_ERROR(engine.get_storage_manager()->create_storage(&meta, &create_epoch));
  foedus::ErrorStack result = engine.get_thread_pool()->impersonate_synchronous(kProc);
  std::cout << "result=" << result << std::endl;
  COERCE_ERROR(engine.uninitialize());

  return 0;
}
