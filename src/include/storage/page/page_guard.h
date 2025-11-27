//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard.h
//
// 标识：src/include/storage/page/page_guard.h
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库研究组
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>

#include "buffer/arc_replacer.h"
#include "buffer/buffer_pool_manager.h"
#include "storage/disk/disk_scheduler.h"
#include "storage/page/page.h"

namespace bustub {

class BufferPoolManager;
class FrameHeader;

/**
 * @brief 一个 RAII 对象，用于对数据页提供线程安全的读访问。
 *
 * BusTub 系统与缓冲池页数据交互的**唯一**方式是通过页保护。由于 `ReadPageGuard` 是 RAII
 * 对象，系统无需手动锁定和解锁页的锁。
 *
 * 使用 `ReadPageGuard` 时，多个线程可以共享对页数据的读访问。但任何 `ReadPageGuard`
 * 的存在意味着没有线程可以修改该页的数据。
 */
class ReadPageGuard {
  /** @brief 只有缓冲池管理器被允许构造有效的 `ReadPageGuard`。 */
  friend class BufferPoolManager;

 public:
  /**
   * @brief `ReadPageGuard` 的默认构造函数。
   *
   * 注意，我们**绝不**希望使用仅被默认构造的保护。我们定义此默认构造函数的唯一原因是允许在栈上放置一个“未初始化”的保护，之后可以通过
   * `=` 进行移动赋值。
   *
   * **使用未初始化的页保护是未定义行为。**
   *
   * 换句话说，获取有效 `ReadPageGuard` 的唯一方式是通过缓冲池管理器。
   */
  ReadPageGuard() = default;

  ReadPageGuard(const ReadPageGuard &) = delete;
  auto operator=(const ReadPageGuard &) -> ReadPageGuard & = delete;
  ReadPageGuard(ReadPageGuard &&that) noexcept;
  auto operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard &;
  auto GetPageId() const -> page_id_t;
  auto GetData() const -> const char *;
  template <class T>
  auto As() const -> const T * {
    return reinterpret_cast<const T *>(GetData());
  }
  auto IsDirty() const -> bool;
  void Flush();
  void Drop();
  ~ReadPageGuard();

 private:
  /** @brief 只有缓冲池管理器被允许构造有效的 `ReadPageGuard`。 */
  explicit ReadPageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame, std::shared_ptr<ArcReplacer> replacer,
                         std::shared_ptr<std::mutex> bpm_latch, std::shared_ptr<DiskScheduler> disk_scheduler);

  /** @brief 我们正在保护的页的页ID。 */
  page_id_t page_id_;

  /**
   * @brief 保存我们要保护的页的帧。
   *
   * 此页保护的几乎所有操作都应通过此指向 `FrameHeader` 的共享指针完成。
   */
  std::shared_ptr<FrameHeader> frame_;

  /**
   * @brief 指向缓冲池管理器替换器的共享指针。
   *
   * 由于缓冲池无法知道此 `ReadPageGuard` 何时被析构，我们维护一个指向缓冲池替换器的指针，以便在析构时将帧设置为可驱逐。
   */
  std::shared_ptr<ArcReplacer> replacer_;

  /**
   * @brief 指向缓冲池管理器锁的共享指针。
   *
   * 由于缓冲池无法知道此 `ReadPageGuard`
   * 何时被析构，我们维护一个指向缓冲池锁的指针，以便在需要更新缓冲池替换器中帧的驱逐状态时使用。
   */
  std::shared_ptr<std::mutex> bpm_latch_;

  /**
   * @brief 指向缓冲池管理器磁盘调度器的共享指针。
   *
   * 用于将页刷新到磁盘时。
   */
  std::shared_ptr<DiskScheduler> disk_scheduler_;

  /**
   * @brief 此 `ReadPageGuard` 的有效性标志。
   *
   * 由于我们必须允许构造无效的页保护（参见上面的文档），我们必须维护某种状态来告诉我们此页保护是否有效。注意，默认构造函数会自动将此字段设置为
   * `false`。
   *
   * 如果我们不维护此标志，移动构造函数/移动赋值运算符可能会尝试析构或 `Drop()` 无效的成员，从而导致段错误。
   */
  bool is_valid_{false};

  /**
   * TODO(P1)：你可以在此处添加任何你认为必要的字段。
   *
   * 如果你想要额外的（不存在的）风格分数，并且想要更花哨一点，你可以研究 `std::shared_lock`
   * 类型，并使用它来实现锁机制，而不是手动调用 `lock` 和 `unlock`。
   */
};

/**
 * @brief 一个 RAII 对象，用于对数据页提供线程安全的写访问。
 *
 * BusTub 系统与缓冲池页数据交互的**唯一**方式是通过页保护。由于 `WritePageGuard` 是 RAII
 * 对象，系统无需手动锁定和解锁页的锁。
 *
 * 使用 `WritePageGuard` 时，只有一个线程可以独占该页的数据。这意味着 `WritePageGuard` 的所有者可以随意修改页的数据。但
 * `WritePageGuard` 的存在意味着同一页不能同时存在其他 `WritePageGuard` 或任何 `ReadPageGuard`。
 */
class WritePageGuard {
  /** @brief 只有缓冲池管理器被允许构造有效的 `WritePageGuard`。 */
  friend class BufferPoolManager;

 public:
  /**
   * @brief `WritePageGuard` 的默认构造函数。
   *
   * 注意，我们**绝不**希望使用仅被默认构造的保护。我们定义此默认构造函数的唯一原因是允许在栈上放置一个“未初始化”的保护，之后可以通过
   * `=` 进行移动赋值。
   *
   * **使用未初始化的页保护是未定义行为。**
   *
   * 换句话说，获取有效 `WritePageGuard` 的唯一方式是通过缓冲池管理器。
   */
  WritePageGuard() = default;

  WritePageGuard(const WritePageGuard &) = delete;
  auto operator=(const WritePageGuard &) -> WritePageGuard & = delete;
  WritePageGuard(WritePageGuard &&that) noexcept;
  auto operator=(WritePageGuard &&that) noexcept -> WritePageGuard &;
  auto GetPageId() const -> page_id_t;
  auto GetData() const -> const char *;
  template <class T>
  auto As() const -> const T * {
    return reinterpret_cast<const T *>(GetData());
  }
  auto GetDataMut() -> char *;
  template <class T>
  auto AsMut() -> T * {
    return reinterpret_cast<T *>(GetDataMut());
  }
  auto IsDirty() const -> bool;
  void Flush();
  void Drop();
  ~WritePageGuard();

 private:
  /** @brief 只有缓冲池管理器被允许构造有效的 `WritePageGuard`。 */
  explicit WritePageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame, std::shared_ptr<ArcReplacer> replacer,
                          std::shared_ptr<std::mutex> bpm_latch, std::shared_ptr<DiskScheduler> disk_scheduler);

  /** @brief 我们正在保护的页的页ID。 */
  page_id_t page_id_;

  /**
   * @brief 保存我们要保护的页的帧。
   *
   * 此页保护的几乎所有操作都应通过此指向 `FrameHeader` 的共享指针完成。
   */
  std::shared_ptr<FrameHeader> frame_;

  /**
   * @brief 指向缓冲池管理器替换器的共享指针。
   *
   * 由于缓冲池无法知道此 `WritePageGuard`
   * 何时被析构，我们维护一个指向缓冲池替换器的指针，以便在析构时将帧设置为可驱逐。
   */
  std::shared_ptr<ArcReplacer> replacer_;

  /**
   * @brief 指向缓冲池管理器锁的共享指针。
   *
   * 由于缓冲池无法知道此 `WritePageGuard`
   * 何时被析构，我们维护一个指向缓冲池锁的指针，以便在需要更新缓冲池替换器中帧的驱逐状态时使用。
   */
  std::shared_ptr<std::mutex> bpm_latch_;

  /**
   * @brief 指向缓冲池管理器磁盘调度器的共享指针。
   *
   * 用于将页刷新到磁盘时。
   */
  std::shared_ptr<DiskScheduler> disk_scheduler_;

  /**
   * @brief 此 `WritePageGuard` 的有效性标志。
   *
   * 由于我们必须允许构造无效的页保护（参见上面的文档），我们必须维护某种状态来告诉我们此页保护是否有效。注意，默认构造函数会自动将此字段设置为
   * `false`。
   *
   * 如果我们不维护此标志，移动构造函数/移动赋值运算符可能会尝试析构或 `Drop()` 无效的成员，从而导致段错误。
   */
  bool is_valid_{false};

  /**
   * TODO(P1)：你可以在此处添加任何你认为必要的字段。
   *
   * 如果你想要额外的（不存在的）风格分数，并且想要更花哨一点，你可以研究 `std::unique_lock`
   * 类型，并使用它来实现锁机制，而不是手动调用 `lock` 和 `unlock`。
   */
};

}  // namespace bustub