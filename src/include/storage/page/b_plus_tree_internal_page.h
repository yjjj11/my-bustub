//===----------------------------------------------------------------------===//
//
//                         BusTub（巴士桶）
//
// b_plus_tree_internal_page.h
//
// 标识：src/include/storage/page/b_plus_tree_internal_page.h
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库组
//
//===----------------------------------------------------------------------===//

#pragma once

#include <queue>
#include <string>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
#define INTERNAL_PAGE_HEADER_SIZE 12
#define INTERNAL_PAGE_SLOT_CNT \
  ((BUSTUB_PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / ((int)(sizeof(KeyType) + sizeof(ValueType))))  // NOLINT

/**
 * 内部页中存储 n 个索引键和 n+1 个子节点指针（页ID）。
 * 指针 PAGE_ID(i) 指向一个子树，该子树中所有键 K 满足：K(i) <= K < K(i+1)。
 * 注意：由于键的数量与子节点指针的数量不相等，key_array_ 中的第一个键始终无效。
 * 也就是说，任何查找操作都应忽略第一个键。
 *
 * 内部页格式（键按升序存储）：
 *  ---------
 * |  头部   |
 *  ---------
 *  ------------------------------------------
 * | 键(1)（无效） | 键(2) | ... | 键(n) |
 *  ------------------------------------------
 *  ---------------------------------------------
 * | 页ID(1) | 页ID(2) | ... | 页ID(n+1) |
 *  ---------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
 public:
  // 删除所有构造函数/析构函数以确保内存安全
  BPlusTreeInternalPage() = delete;
  BPlusTreeInternalPage(const BPlusTreeInternalPage &other) = delete;

  // 初始化内部页
  void Init(int max_size = INTERNAL_PAGE_SLOT_CNT);

  // 获取指定索引位置的键
  auto KeyAt(int index) const -> KeyType;

  // 在指定索引位置设置键
  void SetKeyAt(int index, const KeyType &key);

  void SetValueAt(int index, const ValueType &val);

  /**
   * @param value 要查找的值（子节点页ID）
   * @return 该值对应的索引位置
   */
  auto ValueIndex(const ValueType &value) const -> int;

  // 获取指定索引位置的子节点页ID
  auto ValueAt(int index) const -> ValueType;

  auto InsertAt(const int &insert_pos, const KeyType &key, const ValueType &value) -> bool;

  auto Insert(const KeyType &key, const ValueType &value, const int &insert_pos) -> bool;

  auto FindInsertPos(const KeyType &key, const KeyComparator &comparator) const -> int;

  void Insert_Set_old(const int &split_idx, std::vector<std::pair<KeyType, ValueType>> &all_data);

  void Insert_Set_new(const int &split_idx, std::vector<std::pair<KeyType, ValueType>> &all_data);

  auto FindPage(const KeyType &key, const KeyComparator &comparator) const -> ValueType;

  void RemoveAt(const int &index);

  void RemoveAt_head();

  void InsertAt_head(const KeyType &key, const ValueType &value);
  /**
   * @brief 仅用于测试，返回一个字符串表示当前内部页中的所有键，格式为 "(键1,键2,键3,...)"
   *
   * @return 当前内部页中所有键的字符串表示
   */
  auto ToString() const -> std::string {
    std::string kstr = "(";
    bool first = true;

    // 内部页的第一个键始终无效，从索引1开始遍历
    for (int i = 1; i < GetSize(); i++) {
      KeyType key = KeyAt(i);
      if (first) {
        first = false;
      } else {
        kstr.append(",");
      }

      kstr.append(std::to_string(key.ToString()));
    }
    kstr.append(")");

    return kstr;
  }

 private:
  // 存储页数据的数组
  KeyType key_array_[INTERNAL_PAGE_SLOT_CNT];        // 键数组
  ValueType page_id_array_[INTERNAL_PAGE_SLOT_CNT];  // 子节点页ID数组
  // （2025年春季学期）如果需要，可以在此处添加更多字段和辅助函数
};

}  // namespace bustub