//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// 标识：src/buffer/buffer_pool_manager.cpp
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库研究组
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include "buffer/arc_replacer.h"
#include "common/config.h"
#include "common/macros.h"

namespace bustub {

/**
 * @brief `FrameHeader`的构造函数，将所有字段初始化为默认值。
 *
 * 有关`FrameHeader`的更多信息，请参阅"buffer/buffer_pool_manager.h"中的文档。
 *
 * @param frame_id 我们要为其创建头部的帧的帧ID/索引。
 */
FrameHeader::FrameHeader(frame_id_t frame_id) : frame_id_(frame_id), data_(BUSTUB_PAGE_SIZE, 0) { Reset(); }

/**
 * @brief 获取帧数据的原始常量指针。
 *
 * @return const char* 指向帧存储的不可变数据的指针。
 */
auto FrameHeader::GetData() const -> const char * { return data_.data(); }

/**
 * @brief 获取帧数据的原始可变指针。
 *
 * @return char* 指向帧存储的可变数据的指针。
 */
auto FrameHeader::GetDataMut() -> char * { return data_.data(); }

/**
 * @brief 重置`FrameHeader`的成员字段。
 */
void FrameHeader::Reset() {
  std::fill(data_.begin(), data_.end(), 0);
  pin_count_.store(0);
  is_dirty_ = false;
}

/**
 * @brief 创建一个新的`BufferPoolManager`实例并初始化所有字段。
 *
 * 有关`BufferPoolManager`的更多信息，请参阅"buffer/buffer_pool_manager.h"中的文档。
 *
 * ### 实现
 *
 * 我们已经为你实现了构造函数，使其与我们的参考解决方案相契合。如果它不符合你的实现，你可以自由修改这里的任何内容。
 *
 * 不过要注意！如果你偏离我们的指导太远，我们将很难帮助你。我们的建议是，首先使用我们提供的垫脚石实现缓冲池管理器。
 *
 * 一旦你有了一个完全可行的解决方案（所有Gradescope测试用例都通过），然后你可以尝试更有趣的东西！
 *
 * @param num_frames 缓冲池的大小。
 * @param disk_manager 磁盘管理器。
 * @param log_manager 日志管理器。请在P1中忽略此内容。
 */
BufferPoolManager::BufferPoolManager(size_t num_frames, DiskManager *disk_manager, LogManager *log_manager)
    : num_frames_(num_frames),
      next_page_id_(0),
      bpm_latch_(std::make_shared<std::mutex>()),
      replacer_(std::make_shared<ArcReplacer>(num_frames)),
      disk_scheduler_(std::make_shared<DiskScheduler>(disk_manager)),
      log_manager_(log_manager) {
  // 并非严格必要...
  std::scoped_lock latch(*bpm_latch_);

  // 将单调递增计数器初始化为0。
  next_page_id_.store(0);

  // 预先分配所有内存中的帧。
  frames_.reserve(num_frames_);

  // 页表应具有恰好`num_frames_`个槽，对应恰好`num_frames_`个帧。
  page_table_.reserve(num_frames_);
  frame_table_.reserve(num_frames_);
  // 初始化所有帧头，并将空闲帧列表填充所有可能的帧ID（因为所有帧最初都是空闲的）。
  for (size_t i = 0; i < num_frames_; i++) {
    frames_.push_back(std::make_shared<FrameHeader>(i));
    free_frames_.push_back(static_cast<int>(i));
  }
}

/**
 * @brief 销毁`BufferPoolManager`，释放缓冲池使用的所有内存。
 */
BufferPoolManager::~BufferPoolManager() = default;

/**
 * @brief 返回该缓冲池管理的帧数。
 */
auto BufferPoolManager::Size() const -> size_t { return num_frames_; }

/**
 * @brief 在磁盘上分配一个新页。
 *
 * ### 实现
 *
 * 你将维护一个线程安全的、单调递增的计数器，形式为`std::atomic<page_id_t>`。
 * 有关[原子类型](https://en.cppreference.com/w/cpp/atomic/atomic)的更多信息，请参阅文档。
 *
 * TODO(P1)：添加实现。
 *
 * @return 新分配页的页ID。
 */
auto BufferPoolManager::NewPage() -> page_id_t {
  std::future<bool> future;
  frame_id_t frame_id;
  page_id_t new_page_id;
  std::shared_ptr<bustub::FrameHeader> new_frame;

  {
    std::scoped_lock lock(*bpm_latch_);

    if (!free_frames_.empty()) {
      // std::cout<<"有空闲帧\n";
      frame_id = free_frames_.front();
      free_frames_.pop_front();
    } else {
      auto evict_opt = replacer_->Evict();
      // std::cout<<"调用了Evict\n";
      if (!evict_opt.has_value()) {
        std::cout << "无可用驱逐帧，无法创建新页\n";
        return INVALID_PAGE_ID;
      }
      frame_id = evict_opt.value();

      // 若被驱逐帧是脏页，先刷盘
      auto &evict_frame = frames_[frame_id];
      std::scoped_lock ls(evict_frame->rwlatch_);
      if (evict_frame->is_dirty_) {
        page_id_t old_page_id = frame_table_[frame_id];
        // std::cout<<"驱逐走的页的id为: "<<old_page_id<<"\n";
        FlushPageUnsafe(old_page_id);
        evict_frame->is_dirty_ = false;
      }

      // 清理旧页映射
      page_id_t old_page_id = frame_table_[frame_id];
      page_table_.erase(old_page_id);
      frame_table_.erase(frame_id);
    }

    // 4. 分配新页ID()
    new_page_id = next_page_id_.load();
    next_page_id_.fetch_add(1);
    // 5. 初始化帧
    new_frame = frames_[frame_id];
    std::scoped_lock rwlock(new_frame->rwlatch_);
    new_frame->Reset();

    // 6. 建立新映射
    page_table_[new_page_id] = frame_id;
    frame_table_[frame_id] = new_page_id;

    // 7. 磁盘创建新页（写入全零数据）
    // std::cout<<"刷全零数据到磁盘中文件页中去\n";
    std::vector<DiskRequest> requests;
    auto promise = disk_scheduler_->CreatePromise();
    future = std::move(promise.get_future());
    requests.emplace_back(DiskRequest{true, new_frame->GetDataMut(), new_page_id, std::move(promise)});
    disk_scheduler_->Schedule(requests);
    // if(future.get())std::cout<<"get newpage successfully, page= "<<next_page_id_.load()<<"\n";
    // else std::cout<<"failed to get newpage\n";
  }

  if (future.get()) {
    return new_page_id;
  } else {
    std::scoped_lock lock(*bpm_latch_);  // 重新加锁
    page_table_.erase(new_page_id);
    frame_table_.erase(frame_id);  // 注意 frame_id 也需移到外部定义
    free_frames_.push_back(frame_id);
    return INVALID_PAGE_ID;
  }
}

/**
 * @brief 从数据库中删除页，包括磁盘和内存中的页。
 *
 * 如果页在缓冲池中被固定，此函数不执行任何操作并返回`false`。否则，此函数从磁盘和内存中删除页（如果它仍在缓冲池中），返回`true`。
 *
 * ### 实现
 *
 * 考虑页或页的元数据可能存在的所有位置，并以此指导你实现此函数。你可能希望在实现`CheckedReadPage`和`CheckedWritePage`之后实现此函数。
 *
 * 你应该调用磁盘调度器中的`DeallocatePage`以使新页可以使用该空间。
 *
 * TODO(P1)：添加实现。
 *
 * @param page_id 我们要删除的页的页ID。
 * @return 如果页存在但无法删除，返回`false`；如果页不存在或删除成功，返回`true`。
 */
auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::scoped_lock lock(*bpm_latch_);

  // 1. 检查页是否在内存中
  if (page_table_.count(page_id) == 0) {
    // 页不在内存，直接删除磁盘页
    disk_scheduler_->DeallocatePage(page_id);
    return true;
  }

  // 2. 检查页是否被固
  frame_id_t frame_id = page_table_[page_id];
  auto &frame = frames_[frame_id];
  if (frame->pin_count_.load(std::memory_order_relaxed) > 0) {
    return false;
  }

  if (frame->is_dirty_) {
    FlushPageUnsafe(page_id);
    frame->is_dirty_ = false;
  }

  page_table_.erase(page_id);
  frame_table_.erase(frame_id);
  frame->Reset();
  free_frames_.push_back(frame_id);

  // 5. 删除磁盘页
  disk_scheduler_->DeallocatePage(page_id);

  return true;
}

/**
 * @brief 获取对数据页的可选写锁保护。如果需要，用户可以指定`AccessType`。
 *
 * 如果无法将数据页带入内存，此函数将返回`std::nullopt`。
 *
 * 页数据只能通过页保护来访问。此`BufferPoolManager`的用户应根据他们想要访问数据的模式获取`ReadPageGuard`或`WritePageGuard`，以确保任何数据访问都是线程安全的。
 *
 * 同一时间只能有一个`WritePageGuard`读取/写入同一页。这允许数据访问既是不可变的又是可变的，这意味着拥有`WritePageGuard`的线程可以按其需要操作页的数据。如果用户希望多个线程同时读取同一页，他们必须使用`CheckedReadPage`获取`ReadPageGuard`。
 *
 * ### 实现
 *
 * 你需要实现三种主要情况。前两种相对简单：一种是有大量可用内存的情况，另一种是我们实际上不需要执行任何额外I/O的情况。思考这两种情况具体意味着什么。
 *
 * 第三种情况最棘手，即我们没有任何“容易”获得的内存。缓冲池的任务是找到可用于引入数据页的内存，使用你之前实现的替换算法找到候选驱逐帧。
 *
 * 一旦缓冲池确定了要驱逐的帧，可能需要几个I/O操作才能将我们想要的页数据引入该帧。
 *
 * 这两个函数是本项目的关键，因此我们不会给你更多提示。祝你好运！
 *
 * TODO(P1)：添加实现。
 *
 * @param page_id 我们要写入的页的ID。
 * @param access_type 页访问类型。
 * @return std::optional<WritePageGuard>
 * 可选的锁保护，如果没有更多空闲帧（内存不足）则返回`std::nullopt`；否则，返回确保对页数据进行独占和可变访问的`WritePageGuard`。
 */
auto BufferPoolManager::CheckedWritePage(page_id_t page_id, AccessType access_type) -> std::optional<WritePageGuard> {
  if (page_id < 0) {
    return std::nullopt;
  }

  std::shared_ptr<FrameHeader> frame;
  frame_id_t frame_id;
  std::future<bool> future;
  bool need_load = false;

  {
    std::scoped_lock lock(*bpm_latch_);

    if (page_table_.count(page_id) > 0) {
      // std::cout<<"1. 页已在内存中\n";
      frame_id = page_table_[page_id];
      frame = frames_[frame_id];
      replacer_->RecordAccess(frame_id, page_id, access_type);
    } else {
      need_load = true;
      if (!free_frames_.empty()) {
        // std::cout<<"2. 页不在内存，有空闲帧\n";
        frame_id = free_frames_.front();
        free_frames_.pop_front();
        frame = frames_[frame_id];
      } else {
        // std::cout<<"3. 无空闲帧，驱逐冷页\n";
        auto evict_opt = replacer_->Evict();
        if (!evict_opt.has_value()) {
          // std::cout<<"无空闲帧可驱逐\n";
          return std::nullopt;
        }
        frame_id = evict_opt.value();
        // std::cout<<"驱逐走帧= "<<frame_id<<"\n";

        frame = frames_[frame_id];
        std::scoped_lock ls(frame->rwlatch_);
        if (frame->is_dirty_) {
          // std::cout<<"脏页，需要刷盘\n";
          page_id_t old_page_id = frame_table_[frame_id];
          // std::cout<<"驱逐走的页的id为: "<<old_page_id<<"\n";
          FlushPageUnsafe(old_page_id);
          frame->is_dirty_ = false;
        }

        // 5. 清理旧映射
        page_id_t old_page_id = frame_table_[frame_id];
        page_table_.erase(old_page_id);
        frame_table_.erase(frame_id);
      }

      // std::cout<<"6. 从磁盘加载页数据\n";
      std::vector<DiskRequest> read_reqs;
      auto promise = disk_scheduler_->CreatePromise();
      future = std::move(promise.get_future());

      read_reqs.emplace_back(DiskRequest{false, const_cast<char *>(frame->GetData()), page_id, std::move(promise)});
      disk_scheduler_->Schedule(read_reqs);
    }
  }

  if (need_load) {
    bool success = future.get();

    {
      std::scoped_lock lock(*bpm_latch_);
      if (!success) {
        free_frames_.push_back(frame_id);
        return std::nullopt;
      }
      page_table_[page_id] = frame_id;
      frame_table_[frame_id] = page_id;
      // 8. 记录页访问
      replacer_->RecordAccess(frame_id, page_id, access_type);
    }
  }
  // 9. 返回写保护
  WritePageGuard res(page_id, frame, replacer_, bpm_latch_, disk_scheduler_);
  // std::cout<<"返回 "<<page_id<<" 的 ReadPageGuard\n";
  return res;
}

/**
 * @brief 获取对数据页的可选读锁保护。如果需要，用户可以指定`AccessType`。
 *
 * 如果无法将数据页带入内存，此函数将返回`std::nullopt`。
 *
 * 页数据只能通过页保护来访问。此`BufferPoolManager`的用户应根据他们想要访问数据的模式获取`ReadPageGuard`或`WritePageGuard`，以确保任何数据访问都是线程安全的。
 *
 * 可以有任意数量的`ReadPageGuard`同时跨不同线程读取同一页数据。但是，所有数据访问必须是不可变的。如果用户想要修改页的数据，他们必须使用`CheckedWritePage`获取`WritePageGuard`。
 *
 * ### 实现
 *
 * 请参阅`CheckedWritePage`的实现细节。
 *
 * TODO(P1)：添加实现。
 *
 * @param page_id 我们要读取的页的ID。
 * @param access_type 页访问类型。
 * @return std::optional<ReadPageGuard>
 * 可选的锁保护，如果没有更多空闲帧（内存不足）则返回`std::nullopt`；否则，返回确保对页数据进行共享和只读访问的`ReadPageGuard`。
 */
auto BufferPoolManager::CheckedReadPage(page_id_t page_id, AccessType access_type) -> std::optional<ReadPageGuard> {
  if (page_id < 0) {
    return std::nullopt;
  }

  std::shared_ptr<FrameHeader> frame;
  frame_id_t frame_id;
  std::future<bool> future;
  bool need_load = false;

  {
    std::scoped_lock lock(*bpm_latch_);

    if (page_table_.count(page_id) > 0) {
      // std::cout<<"1. 页已在内存中\n";
      frame_id = page_table_[page_id];
      frame = frames_[frame_id];
      replacer_->RecordAccess(frame_id, page_id, access_type);
    } else {
      need_load = true;
      if (!free_frames_.empty()) {
        // std::cout<<"2. 页不在内存，有空闲帧\n";
        frame_id = free_frames_.front();
        free_frames_.pop_front();
        frame = frames_[frame_id];
      } else {
        // std::cout<<"3. 无空闲帧，驱逐冷页\n";
        auto evict_opt = replacer_->Evict();

        if (!evict_opt.has_value()) {
          return std::nullopt;
        }
        frame_id = evict_opt.value();
        // std::cout<<"驱逐走的帧的id为: "<<frame_id<<"\n";
        frame = frames_[frame_id];

        frame->rwlatch_.lock_shared();
        if (frame->is_dirty_) {
          // std::cout<<"驱逐前脏页刷盘\n";
          page_id_t old_page_id = frame_table_[frame_id];
          // std::cout<<"驱逐走的页的id为: "<<old_page_id<<"\n";
          FlushPageUnsafe(old_page_id);
          frame->is_dirty_ = false;
        }
        frame->rwlatch_.unlock_shared();
        // 5. 清理旧映射
        page_id_t old_page_id = frame_table_[frame_id];
        page_table_.erase(old_page_id);
        frame_table_.erase(frame_id);
      }

      std::vector<DiskRequest> read_reqs;
      auto promise = disk_scheduler_->CreatePromise();  // 创建 promise
      future = std::move(promise.get_future());

      // 创建 DiskRequest 并移动 promise
      read_reqs.emplace_back(DiskRequest{false, const_cast<char *>(frame->GetData()), page_id, std::move(promise)});

      // 调度请求
      disk_scheduler_->Schedule(read_reqs);
    }
  }

  if (need_load) {
    bool success = future.get();

    {
      std::scoped_lock lock(*bpm_latch_);
      if (!success) {
        free_frames_.push_back(frame_id);  // 不成功就不占用帧，但是仍然被驱逐了
        return std::nullopt;
      }
      page_table_[page_id] = frame_id;
      frame_table_[frame_id] = page_id;

      replacer_->RecordAccess(frame_id, page_id, access_type);
    }
  }
  ReadPageGuard res(page_id, frame, replacer_, bpm_latch_, disk_scheduler_);
  return res;
}

/**
 * @brief 是`CheckedWritePage`的包装器，如果内部值存在则解包。
 *
 * 如果`CheckedWritePage`返回`std::nullopt`，**此函数将中止整个过程。**
 *
 * 此函数应**仅**用于测试和便利性。如果缓冲池管理器有可能耗尽内存，则使用`CheckedPageWrite`以允许你处理该情况。
 *
 * 有关`CheckedPageWrite`的实现细节，请参阅其文档。
 *
 * @param page_id 我们要读取的页的ID。
 * @param access_type 页访问类型。
 * @return WritePageGuard 确保对页数据进行独占和可变访问的页保护。
 */
auto BufferPoolManager::WritePage(page_id_t page_id, AccessType access_type) -> WritePageGuard {
  // std::cout<<"进入了WritePage函数\n";
  auto guard_opt = CheckedWritePage(page_id, access_type);

  if (!guard_opt.has_value()) {
    // std::cout<<"CheckedWritePage` 未能引入页 \n";
    fmt::println(stderr, "\n`CheckedWritePage` 未能引入页 {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief 是`CheckedReadPage`的包装器，如果内部值存在则解包。
 *
 * 如果`CheckedReadPage`返回`std::nullopt`，**此函数将中止整个过程。**
 *
 * 此函数应**仅**用于测试和便利性。如果缓冲池管理器有可能耗尽内存，则使用`CheckedPageWrite`以允许你处理该情况。
 *
 * 有关`CheckedPageRead`的实现细节，请参阅其文档。
 *
 * @param page_id 我们要读取的页的ID。
 * @param access_type 页访问类型。
 * @return ReadPageGuard 确保对页数据进行共享和只读访问的页保护。
 */
auto BufferPoolManager::ReadPage(page_id_t page_id, AccessType access_type) -> ReadPageGuard {
  auto guard_opt = CheckedReadPage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedReadPage` 未能引入页 {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief 不安全地将页数据刷新到磁盘。
 *
 * 如果页已被修改，此函数将把页数据写入磁盘。如果给定的页不在内存中，此函数将返回`false`。
 *
 * 在此函数中，你不应获取页的锁。
 * 这意味着你应仔细考虑何时切换`is_dirty_`位。
 *
 * ### 实现
 *
 * 你可能希望在完成`CheckedReadPage`和`CheckedWritePage`之后再实现此函数，因为这样可能更容易理解该做什么。
 *
 * TODO(P1)：添加实现
 *
 * @param page_id 要刷新的页的页ID。
 * @return 如果在页表中找不到该页，返回`false`；否则返回`true`。
 */
auto BufferPoolManager::FlushPageUnsafe(page_id_t page_id) -> bool {
  // 不获取bpm_latch_，由调用者保证线程安全
  // std::cout<<"刷盘第 "<<page_id<<" 页\n";
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }

  frame_id_t frame_id = it->second;
  auto &frame = frames_[frame_id];

  // 调度磁盘写请求
  std::vector<DiskRequest> requests;
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  requests.emplace_back(DiskRequest{true, const_cast<char *>(frame->GetData()), page_id, std::move(promise)});
  disk_scheduler_->Schedule(requests);

  // 等待写操作完成
  try {
    bool success = future.get();
    if (success) {
      frame->is_dirty_ = false;  // 刷盘成功后重置脏标记
    }
    return success;
  } catch (...) {
    return false;
  }
}

/**
 * @brief 安全地将页数据刷新到磁盘。
 *
 * 如果页已被修改，此函数将把页数据写入磁盘。如果给定的页不在内存中，此函数将返回`false`。
 *
 * 在此函数中，你应获取页的锁，以确保一致的状态被刷新到磁盘。
 *
 * ### 实现
 *
 * 你可能希望在完成`CheckedReadPage`、`CheckedWritePage`和页保护中的`Flush`之后再实现此函数，因为这样可能更容易理解该做什么。
 *
 * TODO(P1)：添加实现
 *
 * @param page_id 要刷新的页的页ID。
 * @return 如果在页表中找不到该页，返回`false`；否则返回`true`。
 */
auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::scoped_lock lock(*bpm_latch_);  // 确保页表操作线程安全

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }

  frame_id_t frame_id = it->second;
  auto &frame = frames_[frame_id];

  // 获取写锁，确保刷新期间数据不被修改
  std::unique_lock<std::shared_mutex> page_lock(frame->rwlatch_);

  // 仅刷写脏页
  if (!frame->is_dirty_) {
    return true;
  }

  // 调度磁盘写请求
  std::vector<DiskRequest> requests;
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  requests.emplace_back(DiskRequest{true, const_cast<char *>(frame->GetData()), page_id, std::move(promise)});
  disk_scheduler_->Schedule(requests);

  try {
    bool success = future.get();
    if (success) {
      frame->is_dirty_ = false;
    }
    return success;
  } catch (...) {
    return false;
  }
}

/**
 * @brief 不安全地将内存中的所有页数据刷新到磁盘。
 *
 * 在此函数中，你不应获取页的锁。
 * 这意味着你应仔细考虑何时切换`is_dirty_`位。
 *
 * ### 实现
 *
 * 你可能希望在完成`CheckedReadPage`、`CheckedWritePage`和`FlushPage`之后再实现此函数，因为这样可能更容易理解该做什么。
 *
 * TODO(P1)：添加实现
 */
void BufferPoolManager::FlushAllPagesUnsafe() {
  for (const auto &entry : page_table_) {
    page_id_t page_id = entry.first;
    FlushPageUnsafe(page_id);
  }
}
/**
 * @brief 安全地将内存中的所有页数据刷新到磁盘。
 *
 * 在此函数中，你应获取页的锁，以确保一致的状态被刷新到磁盘。
 *
 * ### 实现
 *
 * 你可能希望在完成`CheckedReadPage`、`CheckedWritePage`和`FlushPage`之后再实现此函数，因为这样可能更容易理解该做什么。
 *
 * TODO(P1)：添加实现
 */
void BufferPoolManager::FlushAllPages() {
  std::scoped_lock lock(*bpm_latch_);  // 确保线程安全
  for (const auto &entry : page_table_) {
    page_id_t page_id = entry.first;
    FlushPage(page_id);  // 调用安全刷盘方法
  }
}

/**
 * @brief 检索页的pin计数。如果页不存在于内存中，返回`std::nullopt`。
 *
 * 此函数是线程安全的。调用者可以在多线程环境中调用此函数，其中多个线程访问同一页。
 *
 * 此函数仅用于测试目的。如果此函数实现不正确，肯定会导致测试套件和自动评分器出现问题。
 *
 * # 实现
 *
 * 我们将使用此函数测试你的缓冲池管理器是否正确管理pin计数。由于`FrameHeader`中的`pin_count_`字段是原子类型，你不需要获取保存我们要查看的页的帧的锁。相反，你可以简单地使用原子`load`来安全地加载存储的值。但是，你仍然需要获取缓冲池锁。
 *
 * 再次提醒，如果你不熟悉原子类型，请参阅官方C++文档[这里](https://en.cppreference.com/w/cpp/atomic/atomic)。
 *
 * TODO(P1)：添加实现
 *
 * @param page_id 我们要获取其pin计数的页的页ID。
 * @return std::optional<size_t> 如果页存在，返回pin计数；否则返回`std::nullopt`。
 */
auto BufferPoolManager::GetPinCount(page_id_t page_id) -> std::optional<size_t> {
  std::scoped_lock lock(*bpm_latch_);

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return std::nullopt;
  }

  frame_id_t frame_id = it->second;
  auto frame = frames_[frame_id];  // frames_存储的是shared_ptr<FrameHeader>

  return frame->pin_count_.load(std::memory_order_seq_cst);
}

}  // namespace bustub
