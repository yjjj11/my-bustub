//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// 标识：src/buffer/lru_k_replacer.cpp
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库研究组
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"
namespace bustub {

/**
 *
 * TODO(P1)：添加实现
 *
 * @brief 新建一个 LRUKReplacer。
 * @param num_frames LRUKReplacer 需要存储的最大帧数量
 */
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  curr_size_ = 0;
  current_timestamp_ = 0;
}

/**
 * TODO(P1)：添加实现
 *
 * @brief 找出反向 k-距离最大的帧并淘汰该帧。只有被标记为“可淘汰”的帧才是淘汰的候选帧。
 *
 * 历史引
 * 如果多个帧的反向 k-距离都是正无穷，则淘汰时间戳最久（即最早的访问时间离现在最远）的帧。
 *
 * 成功淘汰一个帧后，应减少替换器的大小并移除该帧的访问历史。
 *
 * @return 如果成功淘汰一个帧，返回该帧的 ID；如果没有可淘汰的帧，返回 `std::nullopt`。
 */
auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  std::lock_guard<std::mutex> lock(latch_);

  frame_id_t evict_frame = INVALID_FRAME_ID;
  size_t max_backward_k = 0;
  size_t inf_k = std::numeric_limits<size_t>::max();
  for (auto &entry : node_store_) {
    LRUKNode &node = entry.second;
    frame_id_t fid = entry.first;

    if (!node.is_evictable_) {
      continue;
    }

    size_t backward_k;
    if (node.history_.size() < k_) {
      backward_k = std::numeric_limits<size_t>::max();
    } else {
      backward_k = current_timestamp_ - node.history_.back();
    }

    if (max_backward_k < backward_k) {
      evict_frame = fid;
      max_backward_k = backward_k;
    }

    if (backward_k == std::numeric_limits<size_t>::max()) {
      if (inf_k > node.history_.front()) {
        evict_frame = fid;
        inf_k = node.history_.front();
      }
    }
  }

  // 无可用帧可淘汰
  if (evict_frame == INVALID_FRAME_ID) {
    return std::nullopt;
  }

  // 淘汰选中的帧：清理历史、删除节点、更新可淘汰数量
  auto it = node_store_.find(evict_frame);
  if (it != node_store_.end()) {
    it->second.history_.clear();
    node_store_.erase(it);
    curr_size_--;
  }

  return evict_frame;
}

/**
 * TODO(P1)：添加实现
 *
 * @brief 记录给定帧 ID 在当前时间戳被访问的事件。如果该帧 ID 之前未被记录过，则为其创建新的访问历史条目。
 *
 * 如果帧 ID 无效（例如，大于 replacer_size_），抛出异常。你也可以使用 BUSTUB_ASSERT 来终止进程（如果帧 ID 无效）。
 *
 * @param frame_id 被访问的帧的 ID。
 * @param access_type 被接收的访问类型。该参数仅在排行榜测试中需要。
 */
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  if (frame_id < 0 || (size_t)frame_id >= replacer_size_) {
    throw std::invalid_argument("invalid frame_id: out of valid range [0, " + std::to_string(replacer_size_) + ")");
  }

  std::lock_guard<std::mutex> lock(latch_);
  auto [it, inserted] = node_store_.try_emplace(frame_id);  // C++17及以上支持
  LRUKNode &node = it->second;

  if (inserted) {
    node.fid_ = frame_id;
    node.k_ = k_;
    node.is_evictable_ = false;
  }

  node.history_.push_back(current_timestamp_);
  if (node.history_.size() > k_) {
    node.history_.pop_front();
  }
  current_timestamp_++;
}

/**
 * TODO(P1)：添加实现
 *
 * @brief 切换一个帧是“可淘汰”还是“不可淘汰”状态。该函数还会控制替换器的大小。注意，大小等于可淘汰条目的数量。
 *
 * 如果一个帧之前是可淘汰的，现在要设置为不可淘汰，那么大小应减少。如果一个帧之前是不可淘汰的，现在要设置为可淘汰，
 * 那么大小应增加。
 *
 * 如果帧 ID 无效，抛出异常或终止进程。
 *
 * 对于其他场景，该函数应直接终止，不修改任何内容。
 *
 * @param frame_id 其“可淘汰”状态将被修改的帧的 ID
 * @param set_evictable 该帧是否可被淘汰
 */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (frame_id < 0 || (size_t)frame_id >= replacer_size_) {
    throw std::invalid_argument("invalid frame_id: out of valid range [0, " + std::to_string(replacer_size_) + ")");
  }

  std::lock_guard<std::mutex> lock(latch_);

  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;
  }
  LRUKNode &node = it->second;
  if (node.is_evictable_ != set_evictable) {
    node.is_evictable_ = set_evictable;
    curr_size_ += set_evictable ? 1 : -1;
  }
}

/**
 * TODO(P1)：添加实现
 *
 * @brief 从替换器中移除一个可淘汰的帧及其访问历史。如果移除成功，该函数还应减少替换器的大小。
 *
 * 注意，这与淘汰帧不同，淘汰帧总是移除反向 k-距离最大的帧。该函数移除指定的帧 ID，无论其反向 k-距离是多少。
 *
 * 如果对不可淘汰的帧调用 Remove，抛出异常或终止进程。
 *
 * 如果指定的帧未找到，直接从该函数返回。
 *
 * @param frame_id 要被移除的帧的 ID
 */
void LRUKReplacer::Remove(frame_id_t frame_id) {
  if (frame_id < 0 || (size_t)frame_id >= replacer_size_) {
    throw std::invalid_argument("invalid frame_id: out of valid range [0, " + std::to_string(replacer_size_) + ")");
  }

  std::lock_guard<std::mutex> lock(latch_);
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;
  } else if (it->second.is_evictable_ == false) {
    throw std::invalid_argument("invalid frame_id: frame is un_evictable");
  }

  it->second.history_.clear();
  node_store_.erase(it);
  curr_size_--;
}

/**
 * TODO(P1)：添加实现
 *
 * @brief 返回替换器的大小，该大小跟踪可淘汰帧的数量。
 *
 * @return size_t
 */
auto LRUKReplacer::Size() -> size_t { return curr_size_; }
}  // namespace bustub