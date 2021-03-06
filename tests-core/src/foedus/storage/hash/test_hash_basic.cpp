/*
 * Copyright (c) 2014-2015, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
 */
#include <gtest/gtest.h>

#include <cstring>
#include <iostream>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/epoch.hpp"
#include "foedus/test_common.hpp"
#include "foedus/proc/proc_manager.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/hash/hash_metadata.hpp"
#include "foedus/storage/hash/hash_storage.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace storage {
namespace hash {
DEFINE_TEST_CASE_PACKAGE(HashBasicTest, foedus.storage.hash);

TEST(HashBasicTest, Create) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    HashMetadata meta("test", 8);
    HashStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_hash(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    COERCE_ERROR(storage.verify_single_thread(&engine));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

ErrorStack query_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  HashStorage hash = context->get_engine()->get_storage_manager()->get_hash("test2");
  char buf[16];
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  CHECK_ERROR(xct_manager->begin_xct(context, xct::kSerializable));
  char key[100];
  std::memset(key, 0, 100);
  uint16_t payload_capacity = 16;
  ErrorCode result = hash.get_record(context, key, 100, buf, &payload_capacity, true);
  if (result == kErrorCodeStrKeyNotFound) {
    std::cout << "Key not found!" << std::endl;
  } else if (result != kErrorCodeOk) {
    return ERROR_STACK(result);
  }
  Epoch commit_epoch;
  CHECK_ERROR(xct_manager->precommit_xct(context, &commit_epoch));
  CHECK_ERROR(xct_manager->wait_for_commit(commit_epoch));
  return foedus::kRetOk;
}

TEST(HashBasicTest, CreateAndQuery) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  engine.get_proc_manager()->pre_register(proc::ProcAndName("query_task", query_task));
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    HashMetadata meta("test2", 8);
    HashStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_hash(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("query_task"));
    COERCE_ERROR(storage.verify_single_thread(&engine));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

ErrorStack insert_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  HashStorage hash = context->get_engine()->get_storage_manager()->get_hash("ggg");
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  CHECK_ERROR(xct_manager->begin_xct(context, xct::kSerializable));
  uint64_t key = 12345ULL;
  uint64_t data = 897565433333126ULL;
  CHECK_ERROR(hash.insert_record(context, &key, sizeof(key), &data, sizeof(data)));
  Epoch commit_epoch;
  CHECK_ERROR(xct_manager->precommit_xct(context, &commit_epoch));
  CHECK_ERROR(xct_manager->wait_for_commit(commit_epoch));
  return foedus::kRetOk;
}

TEST(HashBasicTest, CreateAndInsert) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  engine.get_proc_manager()->pre_register(proc::ProcAndName("insert_task", insert_task));
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    HashMetadata meta("ggg", 8);
    HashStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_hash(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("insert_task"));
    COERCE_ERROR(storage.verify_single_thread(&engine));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

ErrorStack insert_and_read_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  HashStorage hash = context->get_engine()->get_storage_manager()->get_hash("ggg");
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  CHECK_ERROR(xct_manager->begin_xct(context, xct::kSerializable));
  uint64_t key = 12345ULL;
  uint64_t data = 897565433333126ULL;
  CHECK_ERROR(hash.insert_record(context, &key, sizeof(key), &data, sizeof(data)));
  Epoch commit_epoch;
  CHECK_ERROR(xct_manager->precommit_xct(context, &commit_epoch));

  uint64_t data2;
  CHECK_ERROR(xct_manager->begin_xct(context, xct::kSerializable));
  uint16_t data_capacity = sizeof(data2);
  CHECK_ERROR(hash.get_record(context, &key, sizeof(key), &data2, &data_capacity, true));
  EXPECT_EQ(data, data2);
  CHECK_ERROR(xct_manager->precommit_xct(context, &commit_epoch));

  CHECK_ERROR(xct_manager->wait_for_commit(commit_epoch));
  return foedus::kRetOk;
}

TEST(HashBasicTest, CreateAndInsertAndRead) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  engine.get_proc_manager()->pre_register("insert_and_read_task", insert_and_read_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    HashMetadata meta("ggg", 8);
    HashStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_hash(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("insert_and_read_task"));
    COERCE_ERROR(storage.verify_single_thread(&engine));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

ErrorStack overwrite_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  HashStorage hash = context->get_engine()->get_storage_manager()->get_hash("ggg");
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  CHECK_ERROR(xct_manager->begin_xct(context, xct::kSerializable));
  uint64_t key = 12345ULL;
  uint64_t data = 897565433333126ULL;
  CHECK_ERROR(hash.insert_record(context, &key, sizeof(key), &data, sizeof(data)));
  Epoch commit_epoch;
  CHECK_ERROR(xct_manager->precommit_xct(context, &commit_epoch));

  CHECK_ERROR(xct_manager->begin_xct(context, xct::kSerializable));
  uint64_t data2 = 321654987ULL;
  CHECK_ERROR(hash.overwrite_record(context, key, &data2, 0, sizeof(data2)));
  CHECK_ERROR(xct_manager->precommit_xct(context, &commit_epoch));

  uint64_t data3;
  CHECK_ERROR(xct_manager->begin_xct(context, xct::kSerializable));
  CHECK_ERROR(hash.get_record_primitive(context, key, &data3, 0, true));
  EXPECT_EQ(data2, data3);
  CHECK_ERROR(xct_manager->precommit_xct(context, &commit_epoch));

  CHECK_ERROR(xct_manager->wait_for_commit(commit_epoch));
  return foedus::kRetOk;
}

TEST(HashBasicTest, Overwrite) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  engine.get_proc_manager()->pre_register("overwrite_task", overwrite_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    HashMetadata meta("ggg", 8);
    HashStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_hash(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("overwrite_task"));
    COERCE_ERROR(storage.verify_single_thread(&engine));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}
TEST(HashBasicTest, CreateAndDrop) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    HashMetadata meta("dd", 8);
    HashStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_hash(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    COERCE_ERROR(engine.get_storage_manager()->drop_storage(storage.get_id(), &epoch));
    EXPECT_TRUE(!storage.exists());
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

ErrorStack expand_task_impl(const proc::ProcArguments& args, bool update_case) {
  thread::Thread* context = args.context_;
  HashStorage hash = context->get_engine()->get_storage_manager()->get_hash("ggg");
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  Epoch commit_epoch;

  const uint64_t kKey = 12345ULL;
  char data[512];
  for (uint16_t c = 0; c < sizeof(data); ++c) {
    data[c] = static_cast<char>(c);
  }

  const uint16_t kInitialLen = 6;
  const uint16_t kExpandLen = 5;
  const uint16_t kRep = 80;
  ASSERT_ND(kInitialLen + kExpandLen * kRep <= sizeof(data));

  CHECK_ERROR(xct_manager->begin_xct(context, xct::kSerializable));
  CHECK_ERROR(hash.insert_record(context, &kKey, sizeof(kKey), data, kInitialLen));
  CHECK_ERROR(xct_manager->precommit_xct(context, &commit_epoch));

  CHECK_ERROR(hash.verify_single_thread(context));

  // expand the record many times. this will create a few pages.
  uint16_t len = kInitialLen;
  for (uint16_t rep = 0; rep < kRep; ++rep) {
    len += kExpandLen;
    if (!update_case) {
      // in this case we move a deleted record, using insert
      CHECK_ERROR(xct_manager->begin_xct(context, xct::kSerializable));
      CHECK_ERROR(hash.delete_record(context, &kKey, sizeof(kKey)));
      CHECK_ERROR(xct_manager->precommit_xct(context, &commit_epoch));
      CHECK_ERROR(hash.verify_single_thread(context));

      CHECK_ERROR(xct_manager->begin_xct(context, xct::kSerializable));
      CHECK_ERROR(hash.insert_record(context, &kKey, sizeof(kKey), data, len));
      CHECK_ERROR(xct_manager->precommit_xct(context, &commit_epoch));
    } else {
      // in this case we move an active record, using upsert
      CHECK_ERROR(xct_manager->begin_xct(context, xct::kSerializable));
      CHECK_ERROR(hash.upsert_record(context, &kKey, sizeof(kKey), data, len));
      CHECK_ERROR(xct_manager->precommit_xct(context, &commit_epoch));
    }

    CHECK_ERROR(hash.verify_single_thread(context));
  }

  // verify that the record exists
  CHECK_ERROR(hash.verify_single_thread(context));

  CHECK_ERROR(xct_manager->begin_xct(context, xct::kSerializable));
  char retrieved[sizeof(data)];
  std::memset(retrieved, 42, sizeof(retrieved));
  uint16_t retrieved_capacity = sizeof(retrieved);
  CHECK_ERROR(hash.get_record(context, &kKey, sizeof(kKey), retrieved, &retrieved_capacity, true));
  CHECK_ERROR(xct_manager->precommit_xct(context, &commit_epoch));

  EXPECT_EQ(kInitialLen + kRep * kExpandLen, retrieved_capacity);
  for (uint16_t c = 0; c < retrieved_capacity; ++c) {
    EXPECT_EQ(static_cast<char>(c), retrieved[c]) << c;
  }
  for (uint16_t c = retrieved_capacity; c < sizeof(retrieved); ++c) {
    EXPECT_EQ(42, retrieved[c]) << c;
  }

  CHECK_ERROR(hash.verify_single_thread(context));

  // CHECK_ERROR(hash.debugout_single_thread(context->get_engine()));

  return foedus::kRetOk;
}

ErrorStack expand_insert(const proc::ProcArguments& args) {
  return expand_task_impl(args, false);
}
ErrorStack expand_update(const proc::ProcArguments& args) {
  return expand_task_impl(args, true);
}

void test_expand(bool update_case) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  engine.get_proc_manager()->pre_register("task", update_case ? expand_update : expand_insert);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    HashMetadata meta("ggg", 8);
    HashStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_hash(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("task"));
    COERCE_ERROR(storage.verify_single_thread(&engine));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

TEST(HashBasicTest, ExpandInsert) { test_expand(false); }
TEST(HashBasicTest, ExpandUpdate) { test_expand(true); }
// TASK(Hideaki): we don't have multi-thread cases here. it's not a "basic" test.
// no multi-key cases either. we have to make sure the keys hit the same bucket..

}  // namespace hash
}  // namespace storage
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(HashBasicTest, foedus.storage.hash);
