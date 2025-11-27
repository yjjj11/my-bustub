//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// 标识：src/include/buffer/lru_k_replacer.h
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库研究组
//
//===----------------------------------------------------------------------===//

#pragma once

#include <chrono>
#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <optional>
#include <unordered_map>
#include <vector>
#include "buffer/arc_replacer.h"
#include "common/config.h"
#include "common/macros.h"

namespace bustub {

class LRUKNode {
 public:
  /** 该页的最近 K 次访问时间的历史记录。最久的时间戳存储在前端。 */
  // 如果你开始使用这些变量，请移除 maybe_unused。你可以根据需要修改成员变量。
  std::list<size_t> history_;
  size_t k_;
  frame_id_t fid_;
  bool is_evictable_{false};
};

/**
 * LRUKReplacer 实现了 LRU-k 替换策略。
 *
 * LRU-k 算法会淘汰反向 k-距离最大的帧。反向 k-距离的计算方式是当前时间戳与第 k 次历史访问时间戳的差值。
 *
 * 历史访问次数少于 k 次的帧，其反向 k-距离被视为正无穷。当多个帧的反向 k-距离都是正无穷时，
 * 将使用经典的 LRU 算法来选择淘汰的帧。
 */
class LRUKReplacer {
 public:
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1)：添加实现
   *
   * @brief 销毁 LRUReplacer。
   */
  ~LRUKReplacer() = default;

  auto Evict() -> std::optional<frame_id_t>;

  void RecordAccess(frame_id_t frame_id, AccessType access_type = AccessType::Unknown);

  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  void Remove(frame_id_t frame_id);

  auto Size() -> size_t;

 private:
  // TODO(学生)：实现这个部分！你可以根据需要替换这些成员变量。
  // 如果你开始使用这些变量，请移除 maybe_unused。
  std::unordered_map<frame_id_t, LRUKNode> node_store_;  // 存储所有帧的元数据节点
  size_t current_timestamp_{0};                          // 当前时间戳，用于记录访问时间
  size_t curr_size_{0};                                  // 当前可淘汰帧的数量
  size_t replacer_size_;                                 // 替换器的总容量（帧的数量）
  size_t k_;                                             // LRU-K 中的 k 值
  std::mutex latch_;                                     // 互斥锁，保证线程安全
};

}  // namespace bustub