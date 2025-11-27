//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard.cpp
//
// 标识：src/storage/page/page_guard.cpp
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库研究组
//
//===----------------------------------------------------------------------===//

#include "storage/page/page_guard.h"
#include <memory>
#include "buffer/arc_replacer.h"
#include "common/macros.h"

namespace bustub {

ReadPageGuard::ReadPageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                             std::shared_ptr<ArcReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch,
                             std::shared_ptr<DiskScheduler> disk_scheduler)
    : page_id_(page_id),
      frame_(std::move(frame)),
      replacer_(replacer),
      bpm_latch_(bpm_latch),
      disk_scheduler_(disk_scheduler),
      is_valid_(true) {
  frame_->pin_count_.fetch_add(1, std::memory_order_relaxed);
  frame_->rwlatch_.lock_shared();

  {
    std::scoped_lock lock(*bpm_latch_);
    replacer_->SetEvictable(frame_->frame_id_, false);
  }
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept
    : page_id_(that.page_id_),
      frame_(std::move(that.frame_)),
      replacer_(that.replacer_),
      bpm_latch_(that.bpm_latch_),
      disk_scheduler_(that.disk_scheduler_),
      is_valid_(that.is_valid_) {
  that.is_valid_ = false;
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    Drop();  // 释放当前资源
    // 转移资源
    page_id_ = that.page_id_;
    frame_ = std::move(that.frame_);
    replacer_ = that.replacer_;
    bpm_latch_ = that.bpm_latch_;
    disk_scheduler_ = that.disk_scheduler_;
    is_valid_ = that.is_valid_;

    that.is_valid_ = false;
  }
  return *this;
}

auto ReadPageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "尝试使用无效的读保护");
  return page_id_;
}

auto ReadPageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "尝试使用无效的读保护");
  return frame_->GetData();
}

auto ReadPageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "尝试使用无效的读保护");
  return frame_->is_dirty_;
}

void ReadPageGuard::Flush() {
  BUSTUB_ENSURE(is_valid_, "尝试刷新无效的读保护");
  std::scoped_lock lock(*bpm_latch_);
  if (frame_->is_dirty_) {
    std::vector<DiskRequest> requests;
    auto promise = disk_scheduler_->CreatePromise();
    requests.emplace_back(DiskRequest{true, const_cast<char *>(frame_->GetData()), page_id_, std::move(promise)});
    disk_scheduler_->Schedule(requests);

    frame_->is_dirty_ = false;
  }
}

void ReadPageGuard::Drop() {
  if (!is_valid_) {
    return;
  }
  // std::cout<<"进入了"<<frame_->frame_id_<<" 的drop\n";
  frame_->rwlatch_.unlock_shared();

  frame_->pin_count_.fetch_sub(1, std::memory_order_relaxed);

  if (frame_->pin_count_ == 0) {
    std::scoped_lock lock(*bpm_latch_);
    replacer_->SetEvictable(frame_->frame_id_, true);
    // std::cout<<frame_->frame_id_<<"页 被设置为可驱赶\n";
  }
  // std::cout << "page=" << page_id_ << "的读锁被释放掉了\n";
  // std::cout << "page=" << page_id_ << "的pin_count_=" << frame_->pin_count_ << "\n";
  is_valid_ = false;
}

ReadPageGuard::~ReadPageGuard() { Drop(); }

/**********************************************************************************************************************/
/* WritePageGuard 实现 */
/**********************************************************************************************************************/
WritePageGuard::WritePageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                               std::shared_ptr<ArcReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch,
                               std::shared_ptr<DiskScheduler> disk_scheduler)
    : page_id_(page_id),
      frame_(std::move(frame)),
      replacer_(replacer),
      bpm_latch_(bpm_latch),
      disk_scheduler_(disk_scheduler),
      is_valid_(true) {
  frame_->pin_count_.fetch_add(1, std::memory_order_relaxed);
  frame_->rwlatch_.lock();  // 获取写锁
  std::scoped_lock lock(*bpm_latch_);
  replacer_->SetEvictable(frame_->frame_id_, false);
}

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept
    : page_id_(that.page_id_),
      frame_(std::move(that.frame_)),
      replacer_(that.replacer_),
      bpm_latch_(that.bpm_latch_),
      disk_scheduler_(that.disk_scheduler_),
      is_valid_(that.is_valid_) {
  that.is_valid_ = false;
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    Drop();  // 释放当前资源
    // 转移资源
    page_id_ = that.page_id_;
    frame_ = std::move(that.frame_);
    replacer_ = that.replacer_;
    bpm_latch_ = that.bpm_latch_;
    disk_scheduler_ = that.disk_scheduler_;
    is_valid_ = that.is_valid_;

    that.is_valid_ = false;
  }
  return *this;
}

auto WritePageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "尝试使用无效的写保护");
  return page_id_;
}

auto WritePageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "尝试使用无效的写保护");
  return frame_->GetData();
}

auto WritePageGuard::GetDataMut() -> char * {
  BUSTUB_ENSURE(is_valid_, "尝试使用无效的写保护");
  frame_->is_dirty_ = true;
  return frame_->GetDataMut();
}

auto WritePageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "尝试使用无效的写保护");
  return frame_->is_dirty_;
}

void WritePageGuard::Flush() {
  BUSTUB_ENSURE(is_valid_, "尝试刷新无效的写保护");
  std::scoped_lock lock(*bpm_latch_);
  if (frame_->is_dirty_) {
    try {
      // 构造写请求并调度刷盘
      std::vector<DiskRequest> requests;
      auto promise = disk_scheduler_->CreatePromise();
      requests.emplace_back(DiskRequest{true, const_cast<char *>(frame_->GetData()), page_id_, std::move(promise)});
      disk_scheduler_->Schedule(requests);

      bool success = requests[0].callback_.get_future().get();
      if (success) {
        frame_->is_dirty_ = false;  // 刷盘成功才重置脏标记
      }
    } catch (...) {
      std::cerr << "警告：WritePageGuard::Flush 刷盘失败，页ID=" << page_id_ << std::endl;
    }
  }
}

void WritePageGuard::Drop() {
  if (!is_valid_) {
    return;
  }
  // std::cout<<"进入了"<<frame_->frame_id_<<" 的drop\n";
  frame_->rwlatch_.unlock();

  frame_->pin_count_.fetch_sub(1, std::memory_order_relaxed);

  if (frame_->pin_count_ == 0) {
    std::scoped_lock lock(*bpm_latch_);
    replacer_->SetEvictable(frame_->frame_id_, true);
    // std::cout<<frame_->frame_id_<<"页 被设置为可驱赶\n";
  }
  // std::cout << "page=" << page_id_ << "的写锁被释放了\n";

  is_valid_ = false;
}

WritePageGuard::~WritePageGuard() {
  // std::cout<<"析沟函数调用了\n";
  Drop();
}

}  // namespace bustub
