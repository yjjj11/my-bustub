//===----------------------------------------------------------------------===//
//
//                         BusTub（巴士桶）
//
// b_plus_tree_leaf_page.cpp
//
// 标识：src/storage/page/b_plus_tree_leaf_page.cpp
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库组
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * 辅助方法与工具函数
 *****************************************************************************/

/**
 * @brief 新建叶子页后的初始化方法
 *
 * 从缓冲区池创建新的叶子页后，必须调用初始化方法设置默认值，
 * 包括设置页类型、设置当前大小为0、设置页ID/父页ID、设置
 * 下一页ID以及设置最大大小。
 *
 * @param max_size 叶子节点的最大容量
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetMaxSize(max_size);
  SetSize(0);
  SetPageType(IndexPageType::LEAF_PAGE);
  next_page_id_ = INVALID_PAGE_ID;
  num_tombstones_ = 0;
}

/**
 * @brief 获取页中墓碑信息的辅助函数。
 * @return 该页中待删除的最后 NumTombs 个键，按时间顺序排列（最早删除的在前面）。
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetTombstones() const -> std::vector<KeyType> {
  std::vector<KeyType> res;
  for (size_t i = 0; i < num_tombstones_; i++) {
    size_t key_index = tombstones_[i];
    assert(key_index < static_cast<size_t>(GetSize()) && "Tombstone: invalid key index");
    res.emplace_back(KeyAt(static_cast<int>(key_index)));
  }
  return res;
}

/**
 * 用于设置/获取下一页ID的辅助方法
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * 用于查找并返回指定索引（即数组偏移量）对应键的辅助方法
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  assert(index >= 0 && index < GetSize());
  return key_array_[index];
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  assert(index >= 0 && index < GetSize());
  return rid_array_[index];
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FindFirstGreaterOrEqual(const KeyType &key, const KeyComparator &comparator) const
    -> int {
  int left = 0;
  int right = GetSize() - 1;
  int result = GetSize();  // 默认返回数组大小，表示所有键都小于key

  while (left <= right) {
    int mid = left + (right - left) / 2;
    const KeyType &mid_key = KeyAt(mid);
    int cmp = comparator(mid_key, key);

    if (cmp >= 0) {
      // 当前键大于等于目标键，记录位置并继续在左侧查找
      result = mid;
      right = mid - 1;
    } else {
      // 当前键小于目标键，在右侧查找
      left = mid + 1;
    }
  }

  return result;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> bool {
  int insert_pos = FindFirstGreaterOrEqual(key, comparator);
  for (int i = GetSize() - 1; i >= insert_pos; --i) {
    key_array_[i + 1] = key_array_[i];
    rid_array_[i + 1] = rid_array_[i];
  }
  key_array_[insert_pos] = key;
  rid_array_[insert_pos] = value;

  size_++;
  return true;  // 插入成功
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertAt(const int &insert_pos, const KeyType &key, const ValueType &value) {
  for (int i = GetSize() - 1; i >= insert_pos; --i) {
    key_array_[i + 1] = key_array_[i];
    rid_array_[i + 1] = rid_array_[i];
  }
  key_array_[insert_pos] = key;
  rid_array_[insert_pos] = value;

  size_++;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Insert_Set_old(const int &split_idx,
                                                std::vector<std::pair<KeyType, ValueType>> &all_data) {
  size_ = 0;
  for (int i = 0; i < split_idx; ++i) {
    key_array_[i] = all_data[i].first;
    rid_array_[i] = all_data[i].second;
    size_++;
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Insert_Set_new(const int &split_idx,
                                                std::vector<std::pair<KeyType, ValueType>> &all_data) {
  size_ = 0;
  for (size_t i = split_idx; i < all_data.size(); ++i) {
    key_array_[i - split_idx] = all_data[i].first;
    rid_array_[i - split_idx] = all_data[i].second;
    size_++;
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAt(const int &index) {
  assert(index >= 0 && index < size_);  // 键索引从1开始
  for (int i = index; i < size_ - 1; i++) {
    key_array_[i] = key_array_[i + 1];
    rid_array_[i] = rid_array_[i + 1];
  }
  size_--;
}
// 显式实例化不同参数组合的叶子页类
template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub