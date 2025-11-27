//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_manager.cpp
//
// 标识：src/storage/disk/disk_manager.cpp
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库研究组
//
//===----------------------------------------------------------------------===//

#include <sys/stat.h>
#include <cassert>
#include <cstring>
#include <iostream>
#include <mutex>  // NOLINT
#include <string>
#include <thread>  // NOLINT

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

static char *buffer_used;

/**
 * 创建一个新的磁盘管理器，用于写入指定的数据库文件。
 * @param db_file 要写入的数据库文件名称
 */
DiskManager::DiskManager(const std::filesystem::path &db_file) : db_file_name_(db_file) {
  log_file_name_ = db_file_name_.filename().stem().string() + ".log";

  log_io_.open(log_file_name_, std::ios::binary | std::ios::in | std::ios::app | std::ios::out);
  // 目录或文件不存在
  if (!log_io_.is_open()) {
    log_io_.clear();
    // 创建新文件
    log_io_.open(log_file_name_, std::ios::binary | std::ios::trunc | std::ios::out | std::ios::in);
    if (!log_io_.is_open()) {
      throw Exception("无法打开日志文件");
    }
  }

  std::scoped_lock scoped_db_io_latch(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // 目录或文件不存在
  if (!db_io_.is_open()) {
    db_io_.clear();
    // 创建新文件
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out | std::ios::in);
    if (!db_io_.is_open()) {
      throw Exception("无法打开数据库文件");
    }
  }

  // 初始化数据库文件
  std::filesystem::resize_file(db_file, (page_capacity_ + 1) * BUSTUB_PAGE_SIZE);
  assert(static_cast<size_t>(GetFileSize(db_file_name_)) >= page_capacity_ * BUSTUB_PAGE_SIZE);

  buffer_used = nullptr;
}

/**
 * 关闭磁盘管理器并释放所有文件资源。
 */
void DiskManager::ShutDown() {
  {
    std::scoped_lock scoped_db_io_latch(db_io_latch_);
    db_io_.close();
  }
  log_io_.close();
}

/**
 * 将指定页面的内容写入磁盘文件
 */
void DiskManager::WritePage(page_id_t page_id, const char *page_data) {
  // std::cerr<<"进入了writepage函数 page_id ="<<page_id<<std::flush;
  // fprintf(stderr, "DiskManager::WritePage: 进入写页函数，page_id=%d\n", page_id);
  std::scoped_lock scoped_db_io_latch(db_io_latch_);
  size_t offset;
  if (pages_.find(page_id) != pages_.end()) {
    // std::cout<<"页面已存在，覆盖它\n";
    offset = pages_[page_id];
  } else {
    // std::cout<<"页面不存在，分配新页面。优先使用空闲槽位\n";
    offset = AllocatePage();
  }

  // 将写指针定位到页面偏移量
  db_io_.seekp(offset);
  db_io_.write(page_data, BUSTUB_PAGE_SIZE);
  if (db_io_.bad()) {
    LOG_DEBUG("写入页面 %d 时发生 I/O 错误", page_id);
    return;
  }

  num_writes_ += 1;
  pages_[page_id] = offset;

  // 将写入刷新到磁盘
  db_io_.flush();
}

/**
 * 将指定页面的内容读入给定的内存区域
 */
void DiskManager::ReadPage(page_id_t page_id, char *page_data) {
  // std::cerr<<"进入了ReadPage"<<std::flush;
  std::scoped_lock scoped_db_io_latch(db_io_latch_);
  size_t offset;
  if (pages_.find(page_id) != pages_.end()) {
    offset = pages_[page_id];
  } else {
    // 页面不存在，分配新页面。优先使用空闲槽位
    offset = AllocatePage();
  }

  // 检查是否读取超出文件长度
  int file_size = GetFileSize(db_file_name_);
  if (file_size < 0) {
    LOG_DEBUG("I/O 错误：获取数据库文件大小失败");
    return;
  }
  if (offset > static_cast<size_t>(file_size)) {
    LOG_DEBUG("I/O 错误：读取页面 %d 超出文件末尾，偏移量 %lu", page_id, offset);
    return;
  }

  pages_[page_id] = offset;

  // 将读指针定位到页面偏移量
  db_io_.seekg(offset);
  db_io_.read(page_data, BUSTUB_PAGE_SIZE);

  if (db_io_.bad()) {
    LOG_DEBUG("读取页面 %d 时发生 I/O 错误", page_id);
    return;
  }

  // 检查文件是否在读完整个页面之前结束
  int read_count = db_io_.gcount();
  if (read_count < BUSTUB_PAGE_SIZE) {
    LOG_DEBUG("I/O 错误：读取页面 %d 到达文件末尾，偏移量 %lu，缺失 %d 字节", page_id, offset,
              BUSTUB_PAGE_SIZE - read_count);
    db_io_.clear();
    memset(page_data + read_count, 0, BUSTUB_PAGE_SIZE - read_count);
  }
}

/**
 * 注意：目前这是一个空操作，因为没有更复杂的数据结构来跟踪已释放的页面。
 */
void DiskManager::DeletePage(page_id_t page_id) {
  std::scoped_lock scoped_db_io_latch(db_io_latch_);
  if (pages_.find(page_id) == pages_.end()) {
    return;
  }

  size_t offset = pages_[page_id];
  free_slots_.push_back(offset);
  pages_.erase(page_id);
  num_deletes_ += 1;
}

/**
 * 将日志内容写入磁盘文件
 * 仅在同步完成后返回，且只执行顺序写入
 * @param log_data 原始日志数据
 * @param size 日志条目的大小
 */
void DiskManager::WriteLog(char *log_data, int size) {
  // 强制交换日志缓冲区
  assert(log_data != buffer_used);
  buffer_used = log_data;

  if (size == 0) {  // 如果日志缓冲区为空，不影响 num_flushes_
    return;
  }

  flush_log_ = true;

  if (flush_log_f_ != nullptr) {
    // 用于检查非阻塞刷新
    assert(flush_log_f_->wait_for(std::chrono::seconds(10)) == std::future_status::ready);
  }

  num_flushes_ += 1;
  // 顺序写入
  log_io_.write(log_data, size);

  // 检查 I/O 错误
  if (log_io_.bad()) {
    LOG_DEBUG("写入日志时发生 I/O 错误");
    return;
  }
  // 需要刷新以保持磁盘文件同步
  log_io_.flush();
  flush_log_ = false;
}

/**
 * 将日志内容读入给定的内存区域
 * 始终从开头读取并执行顺序读取
 * @param[out] log_data 输出缓冲区
 * @param size 日志条目的大小
 * @param offset 日志条目在文件中的偏移量
 * @return 如果读取成功则返回 true，否则返回 false
 */
auto DiskManager::ReadLog(char *log_data, int size, int offset) -> bool {
  if (offset >= GetFileSize(log_file_name_)) {
    // LOG_DEBUG("日志文件末尾");
    // LOG_DEBUG("文件大小为 %d", GetFileSize(log_name_));
    return false;
  }
  log_io_.seekp(offset);
  log_io_.read(log_data, size);

  if (log_io_.bad()) {
    LOG_DEBUG("读取日志时发生 I/O 错误");
    return false;
  }
  // 如果日志文件在读取 "size" 字节前结束
  int read_count = log_io_.gcount();
  if (read_count < size) {
    log_io_.clear();
    memset(log_data + read_count, 0, size - read_count);
  }

  return true;
}

/** @return 磁盘刷新次数 */
auto DiskManager::GetNumFlushes() const -> int { return num_flushes_; }

/** @return 若内存内容尚未刷新则返回 true */
auto DiskManager::GetFlushState() const -> bool { return flush_log_; }

/** @return 磁盘写入次数 */
auto DiskManager::GetNumWrites() const -> int { return num_writes_; }

/** @return 删除次数 */
auto DiskManager::GetNumDeletes() const -> int { return num_deletes_; }

/**
 * 获取磁盘文件大小的私有辅助函数
 */
auto DiskManager::GetFileSize(const std::string &file_name) -> int {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? static_cast<int>(stat_buf.st_size) : -1;
}

/**
 * 在空闲槽位中分配页面。如果没有空闲槽位，则追加到文件末尾。
 * @return 分配的页面的偏移量
 */
auto DiskManager::AllocatePage() -> size_t {
  // 查找文件中是否有空闲槽位
  if (!free_slots_.empty()) {
    auto offset = free_slots_.back();
    free_slots_.pop_back();
    return offset;
  }

  // 必要时增加文件大小
  if (pages_.size() + 1 >= page_capacity_) {
    page_capacity_ *= 2;
    std::filesystem::resize_file(db_file_name_, (page_capacity_ + 1) * BUSTUB_PAGE_SIZE);
  }
  return pages_.size() * BUSTUB_PAGE_SIZE;
}

}  // namespace bustub
