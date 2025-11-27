//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.h
//
// 标识：src/include/storage/disk/disk_scheduler.h
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库研究组
//
//===----------------------------------------------------------------------===//

#pragma once

#include <future>  // NOLINT
#include <optional>
#include <thread>  // NOLINT
#include <vector>

#include "common/channel.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

/**
 * @brief 表示 DiskManager 要执行的写或读请求。
 */
struct DiskRequest {
  /** 标记该请求是写操作还是读操作的标志。 */
  bool is_write_;

  /**
   * 内存位置的起始指针，该页面的用途为：
   *   1. 从磁盘读入时（读操作），数据会被读入到这里。
   *   2. 写入磁盘时（写操作），数据会从这里写出。
   */
  char *data_;

  /** 要从磁盘读入或写入磁盘的页面 ID。 */
  page_id_t page_id_;

  /** 用于在请求完成时向请求发起者发送信号的回调。 */
  std::promise<bool> callback_;
};

/**
 * @brief DiskScheduler 用于调度磁盘的读写操作。
 *
 * 通过调用 DiskScheduler::Schedule() 并传入相应的 DiskRequest 对象来调度请求。调度器
 * 维护一个后台工作线程，该线程使用磁盘管理器处理已调度的请求。后台线程在 DiskScheduler
 * 的构造函数中创建，并在其析构函数中 join。
 */
class DiskScheduler {
 public:
  explicit DiskScheduler(DiskManager *disk_manager, size_t num_workers = 4);
  ~DiskScheduler();

  void Schedule(std::vector<DiskRequest> &requests);

  void StartWorkerThread(size_t thread_id);

  using DiskSchedulerPromise = std::promise<bool>;

  /**
   * @brief 创建一个 Promise 对象。如果您想实现自己的 promise 版本，可以修改此函数，
   * 以便我们的测试用例可以使用您的 promise 实现。
   *
   * @return std::promise<bool>
   */
  auto CreatePromise() -> DiskSchedulerPromise { return {}; };

  /**
   * @brief 在磁盘上释放一个页面。
   *
   * 注意：在使用此方法之前，您应该查看 `BufferPoolManager` 中 `DeletePage` 的文档。
   *
   * @param page_id 要从磁盘上释放的页面的页面 ID。
   */
  void DeallocatePage(page_id_t page_id) { disk_manager_->DeletePage(page_id); }

 private:
  /** 指向磁盘管理器的指针。 */
  DiskManager *disk_manager_;
  size_t num_workers_;
  /** 用于并发调度和处理请求的共享队列。当 DiskScheduler 的析构函数被调用时，
   * 会将 `std::nullopt` 放入队列，以向后台线程发出停止执行的信号。 */
  std::vector<Channel<std::optional<DiskRequest>>> queues_;
  std::vector<std::thread> workers_;
  /** 负责向磁盘管理器发出已调度请求的后台线程。 */
  std::atomic<bool> running_{true};
};
}  // namespace bustub