//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// 标识：src/storage/disk/disk_scheduler.cpp
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库研究组
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include <vector>
#include "common/macros.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager, size_t num_workers)
    : disk_manager_(disk_manager), num_workers_(num_workers), queues_(num_workers) {
  // 启动工作线程（每个线程处理一个Channel）
  for (size_t i = 0; i < num_workers; ++i) {
    workers_.emplace_back(&DiskScheduler::StartWorkerThread, this, i);
  }
}
DiskScheduler::~DiskScheduler() {
  running_ = false;
  // 向每个Channel发送终止信号（std::nullopt）
  for (size_t i = 0; i < num_workers_; ++i) {
    queues_[i].Put(std::nullopt);
  }
  // 等待所有线程结束
  for (auto &worker : workers_) {
    if (worker.joinable()) {
      worker.join();
    }
  }
}

/**
 * TODO(P1)：添加实现
 *
 * @brief 为 DiskManager 调度要执行的请求。
 *
 * @param requests 要调度的请求。
 */
void DiskScheduler::Schedule(std::vector<DiskRequest> &requests) {
  for (auto &req : requests) {
    size_t thread_id = static_cast<size_t>(req.page_id_) % num_workers_;
    queues_[thread_id].Put(std::move(req));  // 利用Channel的Put线程安全特性
  }
}

/**
 * TODO(P1)：添加实现
 *
 * @brief 处理已调度请求的后台工作线程函数。
 *
 * 后台线程需要在 DiskScheduler 存在时处理请求，即，此函数应在 ~DiskScheduler() 被调用之前
 * 不会返回。此时，您需要确保该函数确实返回。
 */
void DiskScheduler::StartWorkerThread(size_t thread_id) {
  while (true) {
    auto opt_req = queues_[thread_id].Get();
    if (!opt_req.has_value()) {
      break;
    }

    DiskRequest req = std::move(opt_req.value());

    try {
      if (req.is_write_) {
        disk_manager_->WritePage(req.page_id_, const_cast<const char *>(req.data_));
      } else {
        disk_manager_->ReadPage(req.page_id_, req.data_);
      }
      req.callback_.set_value(true);
    } catch (...) {
      req.callback_.set_exception(std::current_exception());
    }
  }
}

}  // namespace bustub