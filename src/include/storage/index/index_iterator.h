#pragma once
#include <utility>
#include "buffer/traced_buffer_pool_manager.h"  // 确保引入 TracedBufferPoolManager
#include "common/config.h"
#include "common/macros.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/page_guard.h"  // 引入 ReadPageGuard

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator, NumTombs>
#define SHORT_INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

FULL_INDEX_TEMPLATE_ARGUMENTS_DEFN
class IndexIterator {
 public:
  // 保留3参数构造函数（兼容旧逻辑）
  IndexIterator(TracedBufferPoolManager *bpm, page_id_t leaf_page_id, int index);
  ~IndexIterator() = default;  // NOLINT

  auto IsEnd() -> bool;
  auto operator*() -> std::pair<const KeyType, const ValueType>;
  auto operator++() -> IndexIterator &;
  auto operator==(const IndexIterator &itr) const -> bool;
  auto operator!=(const IndexIterator &itr) const -> bool;

 private:
  // 核心修改：将 BufferPoolManager* 改为 TracedBufferPoolManager*
  TracedBufferPoolManager *bpm_;
  page_id_t leaf_page_id_;
  int index_;
  // 新增 LeafPage 类型别名（解决未定义问题）
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
};

}  // namespace bustub
