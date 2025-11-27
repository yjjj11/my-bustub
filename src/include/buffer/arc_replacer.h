//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// arc_replacer.h
//
// 标识：src/include/buffer/arc_replacer.h
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库研究组
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <unordered_map>

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

enum class AccessType { Unknown = 0, Lookup, Scan, Index };

enum class ArcStatus { MRU, MFU, MRU_GHOST, MFU_GHOST };

// 帧状态结构体：存储帧的元数据
struct FrameStatus {
  page_id_t page_id_;
  frame_id_t frame_id_;
  bool evictable_;
  ArcStatus arc_status_;
  FrameStatus(page_id_t pid, frame_id_t fid, bool ev, ArcStatus st)
      : page_id_(pid), frame_id_(fid), evictable_(ev), arc_status_(st) {}
};

/**
 * ArcReplacer 实现了 ARC 替换策略。
 */
class ArcReplacer {
 public:
  explicit ArcReplacer(size_t num_frames);

  DISALLOW_COPY_AND_MOVE(ArcReplacer);

  ~ArcReplacer() = default;

  auto Evict() -> std::optional<frame_id_t>;
  void RecordAccess(frame_id_t frame_id, page_id_t page_id, AccessType access_type = AccessType::Unknown);
  void SetEvictable(frame_id_t frame_id, bool set_evictable);
  void Remove(frame_id_t frame_id);
  auto Size() -> size_t;

 private:
  // 辅助函数：从活跃列表（MRU/MFU）中移除帧
  void RemoveFromAliveList(frame_id_t frame_id, ArcStatus status);
  // 辅助函数：从幽灵列表（MRU_GHOST/MFU_GHOST）中移除页
  void RemoveFromGhostList(page_id_t page_id, ArcStatus status);

  // 活跃列表：存储当前在内存中的帧ID
  std::list<frame_id_t> mru_;  // 最近使用一次的帧
  std::list<frame_id_t> mfu_;  // 最近使用多次的帧
  // 幽灵列表：存储最近被驱逐的页ID
  std::list<page_id_t> mru_ghost_;  // 从MRU驱逐的页
  std::list<page_id_t> mfu_ghost_;  // 从MFU驱逐的页

  // 哈希表：帧ID -> 帧状态（活跃帧）
  std::unordered_map<frame_id_t, std::shared_ptr<FrameStatus>> alive_map_;
  // 哈希表：页ID -> 帧状态（幽灵页）
  std::unordered_map<page_id_t, std::shared_ptr<FrameStatus>> ghost_map_;

  // 迭代器哈希表：快速定位帧在活跃列表中的位置（O(1)删除）
  std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> mru_iter_map_;  // frame_id -> mru_迭代器
  std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> mfu_iter_map_;  // frame_id -> mfu_迭代器
  // 迭代器哈希表：快速定位页在幽灵列表中的位置（O(1)删除）
  std::unordered_map<page_id_t, std::list<page_id_t>::iterator> mru_ghost_iter_map_;  // page_id -> mru_ghost_迭代器
  std::unordered_map<page_id_t, std::list<page_id_t>::iterator> mfu_ghost_iter_map_;  // page_id -> mfu_ghost_迭代器

  size_t curr_size_;        // 可驱逐的活跃帧数量
  size_t mru_target_size_;  // MRU列表的目标大小（论文中的p）
  size_t replacer_size_;    // 替换器最大容量（论文中的c）
  std::mutex latch_;        // 线程安全锁
};

}  // namespace bustub