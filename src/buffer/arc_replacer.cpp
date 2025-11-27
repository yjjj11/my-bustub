// :bustub-keep-private:
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// arc_replacer.cpp
//
// 标识：src/buffer/arc_replacer.cpp
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库研究组
//
//===----------------------------------------------------------------------===//

#include "buffer/arc_replacer.h"
#include <optional>
#include "common/config.h"

namespace bustub {

/**
 *
 * TODO(P1)：添加实现
 *
 * @brief 新建一个 ArcReplacer，初始化所有列表为空，目标大小为 0
 * @param num_frames ArcReplacer 需要缓存的最大帧数
 */
ArcReplacer::ArcReplacer(size_t num_frames) : curr_size_(0), mru_target_size_(0), replacer_size_(num_frames) {}

void ArcReplacer::RemoveFromAliveList(frame_id_t frame_id, ArcStatus status) {
  if (status == ArcStatus::MRU) {
    auto iter_it = mru_iter_map_.find(frame_id);
    if (iter_it != mru_iter_map_.end()) {
      mru_.erase(iter_it->second);
      mru_iter_map_.erase(iter_it);
    }
  } else if (status == ArcStatus::MFU) {
    auto iter_it = mfu_iter_map_.find(frame_id);
    if (iter_it != mfu_iter_map_.end()) {
      mfu_.erase(iter_it->second);
      mfu_iter_map_.erase(iter_it);
    }
  }
}

void ArcReplacer::RemoveFromGhostList(page_id_t page_id, ArcStatus status) {
  if (status == ArcStatus::MRU_GHOST) {
    auto iter_it = mru_ghost_iter_map_.find(page_id);
    if (iter_it != mru_ghost_iter_map_.end()) {
      mru_ghost_.erase(iter_it->second);
      mru_ghost_iter_map_.erase(iter_it);
    }
  } else if (status == ArcStatus::MFU_GHOST) {
    auto iter_it = mfu_ghost_iter_map_.find(page_id);
    if (iter_it != mfu_ghost_iter_map_.end()) {
      mfu_ghost_.erase(iter_it->second);
      mfu_ghost_iter_map_.erase(iter_it);
    }
  }
  ghost_map_.erase(page_id);  // 从幽灵状态表中移除
}
/**
 * TODO(P1)：添加实现
 *
 * @brief 执行替换操作，根据平衡策略从 mfu_ 或 mru_ 中驱逐页到对应的幽灵列表
 *
 * 如果你想参考原始的 ARC 论文，请注意我们的实现有两处修改：
 * 1. 当 mru_ 的大小等于目标大小时，我们不会像论文中那样在决定驱逐哪个列表时检查最后一次访问。
 * 这是合理的，因为原始决策被说明为任意的。
 * 2. 不可驱逐的条目会被跳过。如果所需侧（mru_ / mfu_）的所有条目都被固定，我们会尝试从另一侧（mfu_ / mru_）选择受害者，
 * 并将其移动到对应的幽灵列表（mfu_ghost_ / mru_ghost_）。
 *
 * @return 被驱逐帧的帧 ID，如果无法驱逐则返回 std::nullopt
 */
auto ArcReplacer::Evict() -> std::optional<frame_id_t> {
  std::scoped_lock lock(latch_);

  frame_id_t victim = INVALID_FRAME_ID;
  ArcStatus victim_status = ArcStatus::MRU;

  // 优先驱逐策略：根据MRU目标大小选择驱逐源
  if (mru_.size() >= mru_target_size_) {
    // std::cout<<"mru_.size() >= mru_target_size\n";
    // std::cout<<" 1. 尝试从MRU驱逐\n";
    // std::cout<<mru_.size()<<"\n";
    for (auto it = mru_.rbegin(); it != mru_.rend(); ++it) {
      frame_id_t frame = *it;
      auto status_iter = alive_map_.find(frame);
      // std::cout<<"page_id= "<<status_iter->second->page_id_<<"  是否可驱逐？ =
      // "<<status_iter->second->evictable_<<'\n';
      if (status_iter != alive_map_.end() && status_iter->second->evictable_) {
        victim = frame;
        victim_status = ArcStatus::MRU;
        RemoveFromAliveList(victim, victim_status);
        break;
      }
    }
    // std::cout<<" 2. MRU无候选，尝试从MFU驱逐\n";
    // std::cout<<mfu_.size()<<"\n";
    if (victim == INVALID_FRAME_ID) {
      for (auto it = mfu_.rbegin(); it != mfu_.rend(); ++it) {
        frame_id_t frame = *it;
        auto status_iter = alive_map_.find(frame);
        // std::cout<<"page_id= "<<status_iter->second->page_id_<<"  是否可驱逐？ =
        // "<<status_iter->second->evictable_<<'\n';
        if (status_iter != alive_map_.end() && status_iter->second->evictable_) {
          victim = frame;
          victim_status = ArcStatus::MFU;
          RemoveFromAliveList(victim, victim_status);
          break;
        }
      }
    }
  } else {
    // std::cout<<"mru_.size() < mru_target_size\n";
    // 1. 尝试从MFU驱逐
    for (auto it = mfu_.rbegin(); it != mfu_.rend(); ++it) {
      frame_id_t frame = *it;
      auto status_iter = alive_map_.find(frame);
      // std::cout<<"page_id= "<<status_iter->second->page_id_<<"  是否可驱逐？ =
      // "<<status_iter->second->evictable_<<'\n';
      if (status_iter != alive_map_.end() && status_iter->second->evictable_) {
        victim = frame;
        victim_status = ArcStatus::MFU;
        RemoveFromAliveList(victim, victim_status);
        break;
      }
    }
    // 2. MFU无候选，尝试从MRU驱逐
    if (victim == INVALID_FRAME_ID) {
      for (auto it = mru_.rbegin(); it != mru_.rend(); ++it) {
        frame_id_t frame = *it;
        auto status_iter = alive_map_.find(frame);
        // std::cout<<"page_id= "<<status_iter->second->page_id_<<"  是否可驱逐？ =
        // "<<status_iter->second->evictable_<<'\n';
        if (status_iter != alive_map_.end() && status_iter->second->evictable_) {
          victim = frame;
          victim_status = ArcStatus::MRU;
          RemoveFromAliveList(victim, victim_status);
          break;
        }
      }
    }
  }

  if (victim == INVALID_FRAME_ID) {
    return std::nullopt;
  }

  // 驱逐后：将页加入对应幽灵列表
  auto status = alive_map_[victim];
  page_id_t page_id = status->page_id_;
  if (victim_status == ArcStatus::MRU) {
    mru_ghost_.push_front(page_id);
    auto iter = mru_ghost_.begin();
    mru_ghost_iter_map_[page_id] = iter;
    ghost_map_[page_id] = std::make_shared<FrameStatus>(page_id, victim, false, ArcStatus::MRU_GHOST);
  } else {
    mfu_ghost_.push_front(page_id);
    auto iter = mfu_ghost_.begin();
    mfu_ghost_iter_map_[page_id] = iter;
    ghost_map_[page_id] = std::make_shared<FrameStatus>(page_id, victim, false, ArcStatus::MFU_GHOST);
  }

  alive_map_.erase(victim);
  curr_size_--;

  return victim;
}

/**
 * TODO(P1)：添加实现
 *
 * @brief 记录对帧的访问，相应地调整 ARC 的记录——如果被访问的页存在于任何列表中，则将其移到 mfu_ 的前端；
 * 如果不存在，则移到 mru_ 的前端。
 *
 * 执行原始论文中描述的除 REPLACE 之外的操作，REPLACE 操作由 `Evict()` 处理。
 *
 * 考虑以下四种情况，并分别处理：
 * 1. 访问命中 mru_ 或 mfu_
 * 2/3. 访问命中 mru_ghost_ / mfu_ghost_
 * 4. 访问未命中所有列表
 *
 * 此函数对四个列表执行所有必要的更改，为 `Evict()` 做好准备，使其可以简单地找到并驱逐受害者到幽灵列表。
 *
 * 注意，frame_id 用作活跃页的标识符，page_id 用作幽灵页的标识符，因为 page_id 在页"死亡"后是其唯一标识符。
 * 对于活跃页，使用 page_id 应该也是一样的，因为它们是一一映射的，但使用 frame_id 稍微更直观。
 *
 * @param frame_id 接收新访问的帧的 ID
 * @param page_id 映射到该帧的页的 ID
 * @param access_type 接收的访问类型。此参数仅用于排行榜测试。
 */
void ArcReplacer::RecordAccess(frame_id_t frame_id, page_id_t page_id, [[maybe_unused]] AccessType access_type) {
  std::scoped_lock lock(latch_);

  // 校验帧ID有效性
  if (frame_id < 0 || static_cast<size_t>(frame_id) > replacer_size_) {
    throw std::invalid_argument("Invalid frame ID: out of range");
  }

  // 情况1：访问命中活跃列表（MRU/MFU）
  if (alive_map_.count(frame_id)) {
    auto status = alive_map_[frame_id];
    RemoveFromAliveList(frame_id, status->arc_status_);

    mfu_.push_front(frame_id);
    auto mfu_iter = mfu_.begin();
    mfu_iter_map_[frame_id] = mfu_iter;
    status->arc_status_ = ArcStatus::MFU;
    return;
  }

  // 情况2：访问命中MRU幽灵列表
  if (ghost_map_.count(page_id) && ghost_map_[page_id]->arc_status_ == ArcStatus::MRU_GHOST) {
    auto status = ghost_map_[page_id];
    RemoveFromGhostList(page_id, ArcStatus::MRU_GHOST);

    size_t mru_ghost_size = mru_ghost_.size();
    size_t mfu_ghost_size = mfu_ghost_.size();
    if (mru_ghost_size >= mfu_ghost_size) {
      mru_target_size_ = std::min(mru_target_size_ + 1, replacer_size_);
    } else {
      size_t add = mfu_ghost_size / std::max(mru_ghost_size, 1UL);
      mru_target_size_ = std::min(mru_target_size_ + add, replacer_size_);
    }

    mfu_.push_front(frame_id);
    auto mfu_iter = mfu_.begin();
    mfu_iter_map_[frame_id] = mfu_iter;
    alive_map_[frame_id] = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MFU);
    return;
  }

  // 情况3：访问命中MFU幽灵列表
  if (ghost_map_.count(page_id) && ghost_map_[page_id]->arc_status_ == ArcStatus::MFU_GHOST) {
    auto status = ghost_map_[page_id];
    RemoveFromGhostList(page_id, ArcStatus::MFU_GHOST);

    size_t mru_ghost_size = mru_ghost_.size();
    size_t mfu_ghost_size = mfu_ghost_.size();
    if (mfu_ghost_size >= mru_ghost_size) {
      mru_target_size_ = (mru_target_size_ > 0) ? mru_target_size_ - 1 : 0;
    } else {
      size_t decrease = mru_ghost_size / std::max(mfu_ghost_size, 1UL);
      mru_target_size_ = (decrease < mru_target_size_) ? mru_target_size_ - decrease : 0;
    }

    mfu_.push_front(frame_id);
    auto mfu_iter = mfu_.begin();
    mfu_iter_map_[frame_id] = mfu_iter;
    alive_map_[frame_id] = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MFU);
    return;
  }

  // 情况4：访问未命中任何列表
  size_t mru_total = mru_.size() + mru_ghost_.size();
  size_t total_all = mru_total + mfu_.size() + mfu_ghost_.size();

  // 按ARC规则清理幽灵列表，确保总大小不超限
  if (mru_total == replacer_size_) {
    if (!mru_ghost_.empty()) {
      page_id_t old_page = mru_ghost_.back();
      mru_ghost_.pop_back();
      mru_ghost_iter_map_.erase(old_page);
      ghost_map_.erase(old_page);
    }
  } else if (total_all >= 2 * replacer_size_) {
    if (!mfu_ghost_.empty()) {
      page_id_t old_page = mfu_ghost_.back();
      mfu_ghost_.pop_back();
      mfu_ghost_iter_map_.erase(old_page);
      ghost_map_.erase(old_page);
    }
  }

  mru_.push_front(frame_id);
  auto mru_iter = mru_.begin();
  mru_iter_map_[frame_id] = mru_iter;
  alive_map_[frame_id] = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MRU);
}

/**
 * TODO(P1)：添加实现
 *
 * @brief 切换帧的可驱逐状态。此函数还控制替换器的大小。注意，大小等于可驱逐条目的数量。
 *
 * 如果一个帧之前是可驱逐的，现在被设置为不可驱逐，则大小应递减。如果一个帧之前是不可驱逐的，现在被设置为可驱逐，则大小应递增。
 *
 * 如果帧 ID 无效，抛出异常或终止进程。
 *
 * 对于其他场景，此函数应不做修改地终止。
 *
 * @param frame_id 要修改"可驱逐"状态的帧的 ID
 * @param set_evictable 该帧是否可驱逐
 */
void ArcReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock lock(latch_);

  if (frame_id < 0 || static_cast<size_t>(frame_id) > replacer_size_) {
    throw std::invalid_argument("Invalid frame ID");
  }

  if (alive_map_.count(frame_id)) {
    auto status = alive_map_[frame_id];
    if (status->evictable_ != set_evictable) {
      status->evictable_ = set_evictable;
      curr_size_ += (set_evictable ? 1 : -1);
    }
  }
}

/**
 * TODO(P1)：添加实现
 *
 * @brief 从替换器中移除一个可驱逐的帧。如果移除成功，此函数还应递减替换器的大小。
 *
 * 注意，这与驱逐帧不同，驱逐帧总是移除由 ARC 算法决定的帧。
 *
 * 如果对不可驱逐的帧调用 Remove，抛出异常或终止进程。
 *
 * 如果指定的帧未找到，直接从此函数返回。
 *
 * @param frame_id 要移除的帧的 ID
 */
void ArcReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock lock(latch_);

  if (frame_id < 0 || static_cast<size_t>(frame_id) > replacer_size_) {
    throw std::invalid_argument("Invalid frame ID: out of range");
  }

  auto status_iter = alive_map_.find(frame_id);
  if (status_iter == alive_map_.end()) {
    return;
  }

  auto status = status_iter->second;
  if (!status->evictable_) {
    throw std::runtime_error("Cannot remove non-evictable frame");
  }

  RemoveFromAliveList(frame_id, status->arc_status_);

  page_id_t page_id = status->page_id_;
  if (status->arc_status_ == ArcStatus::MRU) {
    mru_ghost_.push_front(page_id);
    auto iter = mru_ghost_.begin();
    mru_ghost_iter_map_[page_id] = iter;
  } else {
    mfu_ghost_.push_front(page_id);
    auto iter = mfu_ghost_.begin();
    mfu_ghost_iter_map_[page_id] = iter;
  }
  ghost_map_[page_id] = status;

  // 清理活跃帧数据
  alive_map_.erase(frame_id);
  curr_size_--;
}
/**
 * TODO(P1)：添加实现
 *
 * @brief 返回替换器的大小，即跟踪的可驱逐帧的数量。
 *
 * @return size_t
 */
auto ArcReplacer::Size() -> size_t {
  std::scoped_lock lock(latch_);
  return curr_size_;
}

}  // namespace bustub