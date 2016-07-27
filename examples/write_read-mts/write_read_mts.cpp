#include <iostream>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/epoch.hpp"
#include "foedus/proc/proc_manager.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/masstree/masstree_metadata.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/xct/xct_manager.hpp"

const char* kMasstreeName = "write_read_mts";
const char* kInsertProc = "my_insert_proc";
const char* kReadProc  = "my_read_proc";

struct TreeRecord {
  uint64_t    key;
  const char* data;
  uint16_t    data_size;
};

foedus::ErrorStack my_insert_proc(const foedus::proc::ProcArguments& args) {
  foedus::thread::Thread* context = args.context_;
  foedus::storage::masstree::MasstreeStorage masstree =
    context->get_engine()->get_storage_manager()->get_masstree(kMasstreeName);
  foedus::xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, foedus::xct::kSerializable));
  const TreeRecord* input = reinterpret_cast<const TreeRecord*>(args.input_buffer_);

  foedus::storage::masstree::KeySlice key = foedus::storage::masstree::normalize_primitive(input->key);
  WRAP_ERROR_CODE(masstree.insert_record_normalized(context, key, input->data, input->data_size));
  std::cout << "data written = " << input->data << std::endl;
  std::cout << "data size = " << input->data_size << std::endl;
  foedus::Epoch commit_epoch;
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return foedus::kRetOk;
}

foedus::ErrorStack my_read_proc(const foedus::proc::ProcArguments& args) {
  std::cout << "output_buffer_size_: " << args.output_buffer_size_ << std::endl;
  foedus::thread::Thread* context = args.context_;
  foedus::Engine* engine = args.engine_;
  foedus::storage::masstree::MasstreeStorage masstree =
    engine->get_storage_manager()->get_masstree(kMasstreeName);
  foedus::xct::XctManager* xct_manager = engine->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, foedus::xct::kSerializable));

  const uint64_t* key = reinterpret_cast<const uint64_t*>(args.input_buffer_);
  foedus::storage::masstree::KeySlice normalizedKey =  foedus::storage::masstree::normalize_primitive(*key);
  uint16_t data_size = 0;
  foedus::ErrorCode e1 = masstree.get_record_normalized(context, normalizedKey, nullptr, &data_size, true);
  std::cout << "read 1 ErrorCode = " << std::hex << e1 << std::dec << std::endl;
  std::cout << "This should be kErrorCodeStrTooSmallPayloadBuffer = "
            << std::hex << foedus::kErrorCodeStrTooSmallPayloadBuffer << std::dec << std::endl;
  ASSERT_ND( e1 == foedus::kErrorCodeStrTooSmallPayloadBuffer );

  uint16_t stored_data_size = data_size;
  std::cout << "read 1: data_size (= stored data size) = " << data_size << std::endl;
  ASSERT_ND(args.output_buffer_size_ >= stored_data_size);
  WRAP_ERROR_CODE(masstree.get_record_normalized(context, normalizedKey, args.output_buffer_, &data_size, true));
  std::cout << "read 2: data_size (should be unchanged) = " << data_size << std::endl;
  ASSERT_ND( data_size == stored_data_size );
  *args.output_used_ = data_size;

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

  engine.get_proc_manager()->pre_register(kInsertProc, my_insert_proc);
  engine.get_proc_manager()->pre_register(kReadProc,  my_read_proc);
  COERCE_ERROR(engine.initialize());

  {
    foedus::UninitializeGuard guard(&engine);
    foedus::storage::masstree::MasstreeMetadata meta(kMasstreeName);
    foedus::storage::masstree::MasstreeStorage storage;
    foedus::Epoch create_epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_masstree(&meta, &storage, &create_epoch));

    const uint64_t data_size = 1024;
    char* insert_data = reinterpret_cast<char*>(malloc(data_size));
    ::memset(insert_data, '@', data_size - 1);
    insert_data[data_size - 1] = '\0';
    const TreeRecord input = {123, insert_data, data_size};

    foedus::thread::ImpersonateSession insert_session;
    bool ret_insert =
      engine.get_thread_pool()->impersonate(kInsertProc, &input, sizeof(input), &insert_session);
    ASSERT_ND(ret_insert);
    const foedus::ErrorStack result_insert = insert_session.get_result();
    std::cout << "result_insert=" << result_insert << std::endl;
    insert_session.release();

    foedus::thread::ImpersonateSession read_session;
    bool ret_read = engine.get_thread_pool()->impersonate(kReadProc, &input.key, sizeof(input.key), &read_session);
    ASSERT_ND(ret_read);
    foedus::ErrorStack result_read = read_session.get_result();
    std::cout << "*********** output size = " << read_session.get_output_size() << std::endl;
    std::cout << "*********** read result = " << result_read << std::endl;
    const char* record = reinterpret_cast<const char*>(read_session.get_raw_output_buffer());
    std::cout << "*********** record read = " << record << std::endl;
    // for(int i=0; i < read_session.get_output_size(); ++i) {
    //   std::cout << static_cast<uint64_t>(record[i]) << ", ";
    // }
    // std::cout << ")" << std::endl;
    read_session.release();

    COERCE_ERROR(engine.uninitialize());
  }

  return 0;
}
