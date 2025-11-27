//===----------------------------------------------------------------------===//
//
//                         BusTub（巴士桶）
//
// b_plus_tree_internal_page.cpp
//
// 标识：src/storage/page/b_plus_tree_internal_page.cpp
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库组
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * 辅助方法与工具函数
 *****************************************************************************/

/**
 * @brief 新建内部页后的初始化方法。
 *
 * 向新建的页写入必要的头部信息，包括设置页类型、设置当前大小、设置最大页大小。
 * 新建页后必须调用此方法，才能生成有效的 BPlusTreeInternalPage 对象。
 * （页ID和父页ID由缓冲区管理器在分配物理页时设置，此处不涉及）
 *
 * @param max_size 页的最大容量（可存储的键的最大数量）
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetMaxSize(max_size);
  SetSize(1);
  SetPageType(IndexPageType::INTERNAL_PAGE);
}

/**
 * @brief 获取指定索引位置的键。
 *
 * @param index 要获取的键的索引，必须满足 1 ≤ index ≤ GetSize()（跳过无效的第一个键）
 * @return 索引位置对应的键
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  assert(index > 0 && index < GetSize());
  return key_array_[index];
}

/**
 * @brief 在指定索引位置设置键。
 *
 * @param index 要设置的键的索引，必须满足 1 ≤ index ≤ GetSize()
 * @param key 新的键值
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  assert(index > 0 && index < GetSize());
  key_array_[index] = key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &val) {
  assert(index >= 0 && index < GetSize());
  page_id_array_[index] = val;
}
/**
 * @brief 获取指定索引位置的子节点页ID。
 *
 * @param index 要获取的子节点页ID的索引，必须满足 0 ≤ index ≤ GetSize()
 * @return 索引位置对应的子节点页ID
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  assert(index >= 0 && index < GetSize());
  return page_id_array_[index];
}

/**
 * @brief 查找指定子节点页ID对应的索引位置。
 *
 * @param value 要查找的子节点页ID
 * @return 对应的索引位置；若未找到返回 -1
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  for (int i = 0; i < GetSize(); ++i) {
    if (page_id_array_[i] == value) {
      return i;
    }
  }
  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindPage(const KeyType &key, const KeyComparator &comparator) const -> ValueType {
  assert(GetSize() >= 2);
  int left = 1;
  int right = GetSize() - 1;
  int mid;
  int compareResult;
  int targetIndex;
  while (left <= right) {
    mid = left + (right - left) / 2;
    compareResult = comparator(key_array_[mid], key);
    if (compareResult == 0) {
      left = mid;
      break;
    } else if (compareResult < 0) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  targetIndex = left;

  if (targetIndex >= GetSize()) {
    return page_id_array_[GetSize() - 1];
  }

  if (comparator(key_array_[targetIndex], key) == 0) {
    return page_id_array_[targetIndex];
  } else {
    return page_id_array_[targetIndex - 1];
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindInsertPos(const KeyType &key, const KeyComparator &comparator) const -> int {
  int left = 1;                // 有效键起始索引
  int right = GetSize() - 1;   // 有效键结束索引
  int insert_pos = GetSize();  // 默认插入到末尾（所有键都小于目标键）

  while (left <= right) {
    int mid = left + (right - left) / 2;  // 中间索引（避免溢出）
    const KeyType &mid_key = KeyAt(mid);  // 中间键
    int cmp = comparator(mid_key, key);

    if (cmp > 0) {
      // 中间键大于目标键，插入位置在左侧
      insert_pos = mid;
      right = mid - 1;
    } else if (cmp < 0) {
      // 中间键小于目标键，插入位置在右侧
      left = mid + 1;
    } else {
      // 找到重复键，返回特殊标记（后续处理）
      return -1;
    }
  }

  return insert_pos;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertAt(const int &insert_pos, const KeyType &key, const ValueType &value)
    -> bool {
  for (int i = GetSize(); i > insert_pos; --i) {
    key_array_[i] = key_array_[i - 1];
    page_id_array_[i] = page_id_array_[i - 1];
  }

  // 步骤4：插入新键和对应子节点页IDx
  key_array_[insert_pos] = key;
  page_id_array_[insert_pos] = value;

  IncreaseSize();

  return true;
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertAt_head(const KeyType &key, const ValueType &value) {
  for (int i = GetSize(); i > 0; --i) {
    key_array_[i] = key_array_[i - 1];
    page_id_array_[i] = page_id_array_[i - 1];
  }

  // 步骤4：插入新键和对应子节点页IDx
  key_array_[1] = key;
  page_id_array_[0] = value;

  IncreaseSize();
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert_Set_old(const int &split_idx,
                                                    std::vector<std::pair<KeyType, ValueType>> &all_data) {
  size_ = 0;
  for (int i = 0; i < split_idx; ++i) {
    if (i > 0) {
      key_array_[i] = all_data[i].first;
    }
    page_id_array_[i] = all_data[i].second;
    std::cout << key_array_[i] << " " << page_id_array_[i] << "\n";
    IncreaseSize();
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert_Set_new(const int &split_idx,
                                                    std::vector<std::pair<KeyType, ValueType>> &all_data) {
  size_ = 0;
  for (size_t i = split_idx; i < all_data.size(); ++i) {
    int new_idx = i - split_idx;
    if (new_idx > 0) {
      key_array_[new_idx] = all_data[i].first;
    }
    page_id_array_[new_idx] = all_data[i].second;
    std::cout << key_array_[new_idx] << " " << page_id_array_[new_idx] << "\n";
    size_++;
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAt(const int &index) {  // 朝右边合并
  for (int i = index; i < GetSize() - 1; ++i) {
    key_array_[i] = key_array_[i + 1];
    page_id_array_[i] = page_id_array_[i + 1];
  }
  DecreaseSize();
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAt_head() {  // 朝右边合并
  page_id_array_[0] = page_id_array_[1];
  for (int i = 1; i < size_ - 1; ++i) {
    key_array_[i] = key_array_[i + 1];
    page_id_array_[i] = page_id_array_[i + 1];
  }
  DecreaseSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const int &insert_pos) -> bool {
  for (int i = GetSize() - 1; i >= insert_pos; --i) {
    key_array_[i + 1] = key_array_[i];
    page_id_array_[i + 1] = page_id_array_[i];
  }
  key_array_[insert_pos] = key;
  page_id_array_[insert_pos] = value;

  IncreaseSize();
  return true;
}
// 内部节点的值类型应为页ID类型（page_id_t）
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub