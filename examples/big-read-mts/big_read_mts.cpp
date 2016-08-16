#include <iostream>

#include "foedus/debugging/stop_watch.hpp"
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

const uint16_t kPayload = 1024;
// const uint16_t kPayload = 10;
const char* kMasstreeName = "tab1_main";
const char* kWriteProc = "my_write_proc";
const char* kReadProc  = "my_read_proc";

struct WriteInput {
  uint64_t key_count;
  const char* data;
  uint16_t    data_size;
};

foedus::ErrorStack my_write_proc(const foedus::proc::ProcArguments& args) {
  foedus::thread::Thread* context = args.context_;
  foedus::storage::masstree::MasstreeStorage masstree =
    context->get_engine()->get_storage_manager()->get_masstree(kMasstreeName);
  foedus::xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, foedus::xct::kSerializable));
  const WriteInput* input = reinterpret_cast<const WriteInput*>(args.input_buffer_);
  std::cout << "write key_count=" << input->key_count << std::endl;

  foedus::storage::masstree::KeySlice keySlice;
  foedus::Epoch commit_epoch;
  for(uint64_t key=0ULL; key < input->key_count; ++key){
    keySlice = foedus::storage::masstree::normalize_primitive(key);
    WRAP_ERROR_CODE(masstree.insert_record_normalized(context, keySlice, input->data, input->data_size));
    if(key % 1000 == 0) {
      WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
      WRAP_ERROR_CODE(xct_manager->begin_xct(context, foedus::xct::kSerializable));
    }
  }
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return foedus::kRetOk;
}

foedus::ErrorStack my_read_proc(const foedus::proc::ProcArguments& args) {
  foedus::thread::Thread* context = args.context_;
  foedus::storage::masstree::MasstreeStorage masstree =
    context->get_engine()->get_storage_manager()->get_masstree(kMasstreeName);
  foedus::xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, foedus::xct::kSerializable));
  const uint64_t* key_count = reinterpret_cast<const uint64_t*>(args.input_buffer_);
  std::cout << "read key_count=" << *key_count << std::endl;
  uint64_t total = 0;
  char buf[kPayload];
  for(uint64_t key=0ULL; key < *key_count; ++key){

    foedus::storage::masstree::KeySlice normalizedKey =  foedus::storage::masstree::normalize_primitive(key);
    // ** For 2-step read, get data_size first then actual binary **
    // uint16_t data_size = 0;
    // foedus::ErrorCode e1 = masstree.get_record_normalized(context, normalizedKey, nullptr, &data_size, true);
    // if (e1 != foedus::kErrorCodeStrTooSmallPayloadBuffer) {
    //   std::cout << "Unexpected error: e1 = " << e1 << std::endl;
    // }
    // ASSERT_ND( e1 == foedus::kErrorCodeStrTooSmallPayloadBuffer );

    // uint16_t stored_data_size = data_size;
    // // std::cout << "read 1: data_size (= stored data size) = " << data_size << std::endl;
    // WRAP_ERROR_CODE(masstree.get_record_normalized(context, normalizedKey, buf, &stored_data_size, true));
    // // std::cout << "read 2: data_size (should be unchanged) = " << data_size << std::endl;
    // ASSERT_ND( data_size == stored_data_size );
    // ASSERT_ND( stored_data_size == kPayload );

    // ** For 1-step read, use constant data_size **
    uint16_t data_size = kPayload;
    WRAP_ERROR_CODE(masstree.get_record_normalized(context, normalizedKey, buf, &data_size, true));
    // std::cout << "read 2: data_size (should be unchanged) = " << data_size << std::endl;
    ASSERT_ND( data_size == kPayload );

    total += 1;
    if(key % 1000 == 0) {
      std::cout << "processed keys: " << key << std::endl;
    }
  }
  std::cout << "result_count=" << total << std::endl;
  foedus::Epoch commit_epoch;
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return foedus::kRetOk;
}

int main(int argc, char** argv) {
  if(argc != 2) {
    std::cout << "Usage: big-read-mts <key-count>" << std::endl;
    return EXIT_FAILURE;
  }
  uint64_t key_count = std::stoi(argv[1]);
  std::cout << "key_count: " << key_count << std::endl;
  foedus::EngineOptions options;
  options.thread_.thread_count_per_group_ = 1;
  options.cache_.snapshot_cache_size_mb_per_node_ = 1 << 10;
  // In writing 1M records, kErrorCodeMemoryNoFreePages(769):MEMORY happened
  // when this item is set to '1 << 10'.
  options.memory_.page_pool_size_mb_per_node_ = 1 << 11;
  options.xct_.max_read_set_size_ = 1U << 21;
  options.xct_.max_write_set_size_ = 1U << 20;
  foedus::Engine engine(options);
  engine.get_proc_manager()->pre_register(kWriteProc, my_write_proc);
  engine.get_proc_manager()->pre_register(kReadProc,  my_read_proc);
  COERCE_ERROR(engine.initialize());

  foedus::UninitializeGuard guard(&engine);
  foedus::Epoch create_epoch;
  foedus::storage::masstree::MasstreeMetadata meta(kMasstreeName);
  foedus::storage::masstree::MasstreeStorage storage;
  COERCE_ERROR(engine.get_storage_manager()->create_masstree(&meta, &storage, &create_epoch));

  foedus::thread::ImpersonateSession write_session;
  WriteInput input;
  input.key_count = key_count;
  input.data_size = kPayload;
  char* insert_data = reinterpret_cast<char*>(malloc(input.data_size));
  ::memset(insert_data, '@', input.data_size - 1);
  insert_data[input.data_size - 1] = '\0';
  input.data = insert_data;
  foedus::debugging::StopWatch write_duration;
  bool ret_write =
    engine.get_thread_pool()->impersonate(kWriteProc, &input, sizeof(input), &write_session);
  ASSERT_ND(ret_write);
  const foedus::ErrorStack result_write = write_session.get_result();
  uint64_t write_nanosec = write_duration.stop();
  // std::cout << "write duration [usec]=" << write_nanosec / 1000ULL << std::endl;
  std::cout << "write duration [msec]=" << write_nanosec / 1000000ULL << std::endl;
  std::cout << "result_write=" << result_write << std::endl;
  ASSERT_ND( !result_write.is_error() );
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
  ASSERT_ND( !result_read.is_error() );
  // std::cout << "output_size=" << read_session.get_output_size() << std::endl;
  // const char* record = reinterpret_cast<const char*>(read_session.get_raw_output_buffer());
  // std::cout << "record read=" << record << " (";
  // for(int i=0; i < read_session.get_output_size(); ++i) {
  //   std::cout << static_cast<uint64_t>(record[i]) << ", ";
  // }
  // std::cout << ")" << std::endl;
  read_session.release();

  COERCE_ERROR(engine.uninitialize());

  return 0;
}
