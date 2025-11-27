//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.h
//
// 标识：src/include/buffer/buffer_pool_manager.h
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库研究组
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <memory>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "buffer/arc_replacer.h"
#include "common/config.h"
#include "recovery/log_manager.h"
#include "storage/disk/disk_scheduler.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

class BufferPoolManager;
class ReadPageGuard;
class WritePageGuard;

/**
 * @brief 是`BufferPoolManager`的辅助类，用于管理内存帧及其相关元数据。
 *
 * 该类表示`BufferPoolManager`用于存储数据页的内存帧的头部。注意，实际的内存帧并非直接存储在`FrameHeader`内部，而是`FrameHeader`存储指向帧的指针，并与帧分开存储。
 *
 * ---
 *
 * 你可能（或可能不）感兴趣的一点是，为什么`data_`字段被存储为动态分配的向量，而不是直接指向某个预分配内存块的指针。
 *
 * 在传统的生产级缓冲池管理器中，缓冲池要管理的所有内存都预先分配在一个大的连续数组中（想象一个非常大的`malloc`调用，预先分配几GB的内存）。然后，这个大的连续内存块被划分为连续的帧。换句话说，帧由数组基址的页大小（4KB）间隔偏移量定义。
 *
 * 在BusTub中，我们改为为每个帧单独分配内存（通过`std::vector<char>`），以便使用地址清理器轻松检测缓冲区溢出。由于C++没有内存安全的概念，如果所有页都是连续的，很容易将页的数据指针转换为某种大型数据类型并开始覆盖其他页的数据。
 *
 * 如果你想为缓冲池管理器尝试使用更高效的数据结构，你可以自由选择。但是，在未来的项目中（尤其是项目2），你可能会从检测缓冲区溢出中显著受益。
 */
class FrameHeader {
  friend class BufferPoolManager;
  friend class ReadPageGuard;
  friend class WritePageGuard;

 public:
  explicit FrameHeader(frame_id_t frame_id);

 private:
  auto GetData() const -> const char *;
  auto GetDataMut() -> char *;
  void Reset();

  /** @brief 该头部所代表的帧的帧ID/索引。 */
  const frame_id_t frame_id_;

  /** @brief 该帧的读者/写者锁。 */
  std::shared_mutex rwlatch_;

  /** @brief 保持页在内存中的该帧的pin计数。 */
  std::atomic<size_t> pin_count_;

  /** @brief 脏标记。 */
  bool is_dirty_;

  /**
   * @brief 该帧所保存的页数据的指针。
   *
   * 如果该帧不保存任何页数据，则该帧包含全零字节。
   */
  std::vector<char> data_;

  /**
   * TODO(P1)：你可以在此处添加任何你认为必要的字段或辅助函数。
   *
   * 一个潜在的优化是存储`FrameHeader`当前存储的页的可选页ID。这可能允许你跳过在缓冲池管理器的其他地方搜索对应的（页ID，帧ID）对...
   */
};

/**
 * @brief `BufferPoolManager`类的声明。
 *
 * 如说明文档所述，缓冲池负责将数据的物理页在主存缓冲区和持久存储之间来回移动。它还充当缓存，将频繁使用的页保存在内存中以加快访问速度，并将未使用或冷的页逐出到存储中。
 *
 * 在尝试实现缓冲池管理器之前，请确保完整阅读说明文档。你还需要完成`ArcReplacer`和`DiskManager`类的实现。
 */
class BufferPoolManager {
 public:
  BufferPoolManager(size_t num_frames, DiskManager *disk_manager, LogManager *log_manager = nullptr);
  ~BufferPoolManager();

  auto Size() const -> size_t;
  auto NewPage() -> page_id_t;
  auto DeletePage(page_id_t page_id) -> bool;
  auto CheckedWritePage(page_id_t page_id, AccessType access_type = AccessType::Unknown)
      -> std::optional<WritePageGuard>;
  auto CheckedReadPage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> std::optional<ReadPageGuard>;
  auto WritePage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> WritePageGuard;
  auto ReadPage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> ReadPageGuard;
  auto FlushPageUnsafe(page_id_t page_id) -> bool;
  auto FlushPage(page_id_t page_id) -> bool;
  void FlushAllPagesUnsafe();
  void FlushAllPages();
  auto GetPinCount(page_id_t page_id) -> std::optional<size_t>;

  size_t GetFreeFrameCount() { return free_frames_.size(); }

 private:
  /** @brief 缓冲池中的帧数。 */
  const size_t num_frames_;

  /** @brief 下一个要分配的页ID。  */
  std::atomic<page_id_t> next_page_id_;

  /**
   * @brief 保护缓冲池内部数据结构的锁。
   *
   * TODO(P1) 我们建议用关于该锁实际保护内容的细节替换此注释。
   */
  std::shared_ptr<std::mutex> bpm_latch_;

  /** @brief 该缓冲池管理的帧的帧头。 */
  std::vector<std::shared_ptr<FrameHeader>> frames_;

  /** @brief 跟踪页和缓冲池帧之间映射的页表。 */
  std::unordered_map<page_id_t, frame_id_t> page_table_;
  std::unordered_map<frame_id_t, page_id_t> frame_table_;
  /** @brief 不保存任何页数据的空闲帧列表。 */
  std::list<frame_id_t> free_frames_;

  /** @brief 用于查找未固定/候选驱逐页的替换器。 */
  std::shared_ptr<ArcReplacer> replacer_;

  /** @brief 指向磁盘调度器的指针。与页保护共享以进行刷新。 */
  std::shared_ptr<DiskScheduler> disk_scheduler_;

  /**
   * @brief 指向日志管理器的指针。
   *
   * 注意：请在P1中忽略此内容。
   */
  LogManager *log_manager_ __attribute__((__unused__));

  /**
   * TODO(P1)：如果你觉得有必要，可以添加其他私有成员和辅助函数。
   *
   * 不同的页访问模式之间可能会有很多代码重复。
   *
   * 我们建议实现一个辅助函数，返回一个空闲且内部没有存储任何内容的帧的ID。此外，你可能还需要实现一个辅助函数，返回已存储页数据的`FrameHeader`的共享指针或该`FrameHeader`的索引。
   */
};
}  // namespace bustub