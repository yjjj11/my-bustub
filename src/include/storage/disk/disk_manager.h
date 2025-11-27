//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_manager.h
//
// 标识：src/include/storage/disk/disk_manager.h
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库研究组
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <filesystem>
#include <fstream>
#include <future>  // NOLINT
#include <mutex>   // NOLINT
#include <string>
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

/**
 * DiskManager 负责数据库内页面的分配与回收。它处理页面与磁盘之间的读写操作，
 * 在数据库管理系统的上下文中提供逻辑文件层。
 *
 * DiskManager 采用延迟分配策略，即只在首次访问时才在磁盘上分配空间。
 * 它维护页面 ID 与其在数据库文件中对应偏移量的映射。当页面被删除时，
 * 会被标记为空闲，可供未来分配使用。
 */
class DiskManager {
 public:
  explicit DiskManager(const std::filesystem::path &db_file);

  /** 仅用于测试/排行榜，供 DiskManagerMemory 使用 */
  DiskManager() = default;

  virtual ~DiskManager() = default;

  void ShutDown();

  /**
   * 将页面写入数据库文件。
   * @param page_id 页面 ID
   * @param page_data 原始页面数据
   */
  virtual void WritePage(page_id_t page_id, const char *page_data);

  /**
   * 从数据库文件读取页面。
   * @param page_id 页面 ID
   * @param[out] page_data 输出缓冲区
   */
  virtual void ReadPage(page_id_t page_id, char *page_data);

  /**
   * 从数据库文件删除页面，回收磁盘空间。
   * @param page_id 页面 ID
   */
  virtual void DeletePage(page_id_t page_id);

  void WriteLog(char *log_data, int size);

  auto ReadLog(char *log_data, int size, int offset) -> bool;

  auto GetNumFlushes() const -> int;

  auto GetFlushState() const -> bool;

  auto GetNumWrites() const -> int;

  auto GetNumDeletes() const -> int;

  /**
   * 设置用于检查非阻塞刷新的 future。
   * @param f 非阻塞刷新检查
   */
  inline void SetFlushLogFuture(std::future<void> *f) { flush_log_f_ = f; }

  /** 检查是否已设置非阻塞刷新 future */
  inline auto HasFlushLogFuture() -> bool { return flush_log_f_ != nullptr; }

  /** @brief 返回日志文件名 */
  inline auto GetLogFileName() const -> std::filesystem::path { return log_file_name_; }

  /** @brief 返回已使用的磁盘空间大小 */
  auto GetDbFileSize() -> size_t {
    auto file_size = GetFileSize(db_file_name_);
    if (file_size < 0) {
      LOG_DEBUG("I/O 错误：获取数据库文件大小失败");
      return -1;
    }
    return static_cast<size_t>(file_size);
  }

 protected:
  int num_flushes_{0};  // 刷新次数
  int num_writes_{0};   // 写入次数
  int num_deletes_{0};  // 删除次数

  /** @brief 用于磁盘存储的文件容量 */
  size_t page_capacity_{DEFAULT_DB_IO_SIZE};

 private:
  auto GetFileSize(const std::string &file_name) -> int;

  auto AllocatePage() -> size_t;

  // 日志文件写入流
  std::fstream log_io_;
  std::filesystem::path log_file_name_;
  // 数据库文件写入流
  std::fstream db_io_;
  std::filesystem::path db_file_name_;

  // 记录每个页面在数据库文件中的偏移量
  std::unordered_map<page_id_t, size_t> pages_;
  // 记录数据库文件中已删除页面的空闲槽位（用偏移量表示）
  std::vector<size_t> free_slots_;

  bool flush_log_{false};
  std::future<void> *flush_log_f_{nullptr};
  // 多缓冲池实例时，需要保护文件访问
  std::mutex db_io_latch_;
};

}  // namespace bustub