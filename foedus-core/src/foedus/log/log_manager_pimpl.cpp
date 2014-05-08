/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine.hpp>
#include <foedus/engine_options.hpp>
#include <foedus/assert_nd.hpp>
#include <foedus/error_stack_batch.hpp>
#include <foedus/assorted/atomic_fences.hpp>
#include <foedus/fs/path.hpp>
#include <foedus/log/log_id.hpp>
#include <foedus/log/log_manager_pimpl.hpp>
#include <foedus/log/log_options.hpp>
#include <foedus/log/logger_impl.hpp>
#include <foedus/memory/memory_id.hpp>
#include <foedus/thread/thread_id.hpp>
#include <foedus/thread/thread_pool.hpp>
#include <foedus/savepoint/savepoint.hpp>
#include <foedus/savepoint/savepoint_manager.hpp>
#include <glog/logging.h>
#include <string>
#include <vector>
namespace foedus {
namespace log {
ErrorStack LogManagerPimpl::initialize_once() {
    groups_ = engine_->get_options().thread_.group_count_;
    const std::vector< std::string > &log_paths = engine_->get_options().log_.log_paths_;
    const LoggerId total_loggers = log_paths.size();
    const uint16_t total_threads = engine_->get_options().thread_.get_total_thread_count();
    LOG(INFO) << "Initializing LogManager. #loggers=" << total_loggers
        << ", #NUMA-nodes=" << static_cast<int>(groups_) << ", #total_threads=" << total_threads;
    if (!engine_->get_thread_pool().is_initialized()
        || !engine_->get_savepoint_manager().is_initialized()) {
        return ERROR_STACK(ERROR_CODE_DEPEDENT_MODULE_UNAVAILABLE_INIT);
    }
    // see comments in LogOptions#log_paths_
    if (total_loggers % groups_ != 0 || total_threads % total_loggers != 0
        || total_loggers > total_threads) {
        return ERROR_STACK(ERROR_CODE_LOG_INVALID_LOGGER_COUNT);
    }

    // Initialize durable_global_epoch_
    durable_global_epoch_ = engine_->get_savepoint_manager().get_savepoint_fast().
        get_durable_epoch();
    LOG(INFO) << "durable_global_epoch_=" << durable_global_epoch_;

    // evenly distribute loggers to NUMA nodes, then to cores.
    const uint16_t loggers_per_group = total_loggers / groups_;
    const uint16_t cores_per_logger = total_threads / total_loggers;
    LoggerId current_logger_id = 0;
    for (thread::ThreadGroupId group = 0; group < groups_; ++group) {
        memory::ScopedNumaPreferred numa_scope(group);
        thread::ThreadLocalOrdinal current_ordinal = 0;
        for (auto j = 0; j < loggers_per_group; ++j) {
            std::vector< thread::ThreadId > assigned_thread_ids;
            for (auto k = 0; k < cores_per_logger; ++k) {
                assigned_thread_ids.push_back(thread::compose_thread_id(group, current_ordinal));
                current_ordinal++;
            }
            Logger* logger = new Logger(engine_, current_logger_id,
                fs::Path(log_paths[current_logger_id]), assigned_thread_ids);
            CHECK_OUTOFMEMORY(logger);
            loggers_.push_back(logger);
            CHECK_ERROR(logger->initialize());
            ++current_logger_id;
        }
        ASSERT_ND(current_ordinal == engine_->get_options().thread_.thread_count_per_group_);
    }
    ASSERT_ND(current_logger_id == total_loggers);
    ASSERT_ND(current_logger_id == loggers_.size());
    return RET_OK;
}

ErrorStack LogManagerPimpl::uninitialize_once() {
    LOG(INFO) << "Uninitializing LogManager..";
    ErrorStackBatch batch;
    if (!engine_->get_thread_pool().is_initialized()
        || !engine_->get_savepoint_manager().is_initialized()) {
        batch.emprace_back(ERROR_STACK(ERROR_CODE_DEPEDENT_MODULE_UNAVAILABLE_UNINIT));
    }
    batch.uninitialize_and_delete_all(&loggers_);
    return RET_OK;
}
void LogManagerPimpl::wakeup_loggers() {
    for (Logger* logger : loggers_) {
        logger->wakeup();
    }
}

ErrorStack LogManagerPimpl::refresh_global_durable_epoch() {
    assorted::memory_fence_acquire();
    Epoch min_durable_epoch;
    ASSERT_ND(!min_durable_epoch.is_valid());
    for (Logger* logger : loggers_) {
        min_durable_epoch.store_min(logger->get_durable_epoch());
    }
    ASSERT_ND(min_durable_epoch.is_valid());

    if (min_durable_epoch <= durable_global_epoch_) {
        VLOG(0) << "durable_global_epoch_ not advanced";
        return RET_OK;
    }

    LOG(INFO) << "Global durable epoch is about to advance from " << durable_global_epoch_
        << " to " << min_durable_epoch;
    {
        std::lock_guard<std::mutex> guard(durable_global_epoch_savepoint_mutex_);
        if (min_durable_epoch <= durable_global_epoch_) {
            LOG(INFO) << "oh, I lost the race.";
            return RET_OK;
        }

        CHECK_ERROR(engine_->get_savepoint_manager().take_savepoint(min_durable_epoch));
        durable_global_epoch_ = min_durable_epoch;
    }
    durable_global_epoch_advanced_.notify_all();
    return RET_OK;
}


ErrorStack LogManagerPimpl::wait_until_durable(Epoch commit_epoch, int64_t wait_microseconds) {
    assorted::memory_fence_acquire();
    if (commit_epoch <= durable_global_epoch_) {
        DVLOG(1) << "Already durable. commit_epoch=" << commit_epoch << ", durable_global_epoch_="
            << durable_global_epoch_;
        return RET_OK;
    }

    if (wait_microseconds == 0) {
        DVLOG(1) << "Conditional check: commit_epoch=" << commit_epoch << ", durable_global_epoch_="
            << durable_global_epoch_;
        return ERROR_STACK(ERROR_CODE_TIMEOUT);
    }

    std::chrono::high_resolution_clock::time_point now = std::chrono::high_resolution_clock::now();
    std::chrono::high_resolution_clock::time_point until
        = now + std::chrono::microseconds(wait_microseconds);
    while (commit_epoch > durable_global_epoch_) {
        for (Logger* logger : loggers_) {
            logger->wakeup_for_durable_epoch(commit_epoch);
        }
        std::unique_lock<std::mutex> the_lock(durable_global_epoch_advanced_mutex_);
        if (wait_microseconds > 0) {
            LOG(INFO) << "Synchronously waiting for commit_epoch " << commit_epoch;
            if (durable_global_epoch_advanced_.wait_until(the_lock, until)
                    == std::cv_status::timeout && commit_epoch > durable_global_epoch_) {
                LOG(WARNING) << "Timeout occurs. wait_microseconds=" << wait_microseconds;
                return ERROR_STACK(ERROR_CODE_TIMEOUT);
            }
        } else {
            durable_global_epoch_advanced_.wait(the_lock);
        }
    }

    LOG(INFO) << "durable epoch advanced. durable_global_epoch_=" << durable_global_epoch_;
    return RET_OK;
}


void LogManagerPimpl::copy_logger_states(savepoint::Savepoint* new_savepoint) {
    new_savepoint->current_log_files_.clear();
    new_savepoint->oldest_log_files_offset_begin_.clear();
    new_savepoint->current_log_files_.clear();
    new_savepoint->current_log_files_offset_durable_.clear();
    for (Logger* logger : loggers_) {
        logger->copy_logger_state(new_savepoint);
    }
}

}  // namespace log
}  // namespace foedus