//===----------------------------------------------------------------------===//
//
//                         BusTub（巴士桶）
//
// b_plus_tree_leaf_page.h
//
// 标识：src/include/storage/page/b_plus_tree_leaf_page.h
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库组
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <utility>
#include <vector>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_LEAF_PAGE_TYPE BPlusTreeLeafPage<KeyType, ValueType, KeyComparator, NumTombs>
#define LEAF_PAGE_HEADER_SIZE 16
#define LEAF_PAGE_DEFAULT_TOMB_CNT 0
#define LEAF_PAGE_TOMB_CNT ((NumTombs < 0) ? LEAF_PAGE_DEFAULT_TOMB_CNT : NumTombs)
#define LEAF_PAGE_SLOT_CNT                                                                               \
  ((BUSTUB_PAGE_SIZE - LEAF_PAGE_HEADER_SIZE - sizeof(size_t) - (LEAF_PAGE_TOMB_CNT * sizeof(size_t))) / \
   (sizeof(KeyType) + sizeof(ValueType)))  // NOLINT

/**
 * 叶子页中存储索引键和记录ID（记录ID由页ID和槽位ID组合而成，
 * 具体实现见 include/common/rid.h）。仅支持唯一键。
 *
 * 叶子页还包含一个固定大小的“墓碑”索引缓冲区，用于存储已删除的条目。
 *
 * 叶子页格式（键按顺序存储，墓碑的顺序可自行定义）：
 *  --------------------
 * |  头部  | 墓碑数量 | （其中“墓碑数量”对应 num_tombstones_）
 *  --------------------
 *  -----------------------------------
 * | 墓碑(0) | 墓碑(1) | ... | 墓碑(k) |
 *  -----------------------------------
 *  ---------------------------------
 * | 键(1) | 键(2) | ... | 键(n) |
 *  ---------------------------------
 *  ---------------------------------
 * | 记录ID(1) | 记录ID(2) | ... | 记录ID(n) |
 *  ---------------------------------
 *
 * 头部格式（字节大小，总共16字节）：
 *  -----------------------------------------------
 * | 页类型 (4) | 当前大小 (4) | 最大大小 (4) |
 *  -----------------------------------------------
 *  -----------------
 * | 下一页ID (4) |
 *  -----------------
 */
FULL_INDEX_TEMPLATE_ARGUMENTS_DEFN
class BPlusTreeLeafPage : public BPlusTreePage {
 public:
  // 删除所有构造函数/析构函数以确保内存安全
  BPlusTreeLeafPage() = delete;
  BPlusTreeLeafPage(const BPlusTreeLeafPage &other) = delete;

  // 初始化叶子页
  void Init(int max_size = LEAF_PAGE_SLOT_CNT);

  // 获取所有墓碑对应的键
  auto GetTombstones() const -> std::vector<KeyType>;

  // 辅助方法
  // 获取下一页的ID
  auto GetNextPageId() const -> page_id_t;
  // 设置下一页的ID
  void SetNextPageId(page_id_t next_page_id);
  // 获取指定索引位置的键
  auto KeyAt(int index) const -> KeyType;

  auto ValueAt(int index) const -> ValueType;

  void RemoveAt(const int &index);

  auto Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) -> bool;

  void Insert_Set_old(const int &split_idx, std::vector<std::pair<KeyType, ValueType>> &all_data);

  void Insert_Set_new(const int &split_idx, std::vector<std::pair<KeyType, ValueType>> &all_data);

  void InsertAt(const int &insert_pos, const KeyType &key, const ValueType &value);

  auto FindFirstGreaterOrEqual(const KeyType &key, const KeyComparator &comparator) const -> int;

  /**
   * @brief 仅用于测试，返回一个字符串表示当前叶子页中的所有键，格式为“(墓碑键1,墓碑键2,...|键1,键2,键3,...)”
   *
   * @return 字符串形式的键信息
   */
  auto ToString() const -> std::string {
    std::string kstr = "(";
    bool first = true;

    auto tombs = GetTombstones();
    for (size_t i = 0; i < tombs.size(); i++) {
      kstr.append(std::to_string(tombs[i].ToString()));
      if ((i + 1) < tombs.size()) {
        kstr.append(",");
      }
    }

    kstr.append("|");

    for (int i = 0; i < GetSize(); i++) {
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
  page_id_t next_page_id_;                 // 下一个叶子页的ID（用于叶子页链表）
  size_t num_tombstones_;                  // 当前墓碑的数量
  size_t tombstones_[LEAF_PAGE_TOMB_CNT];  // 固定大小的墓碑缓冲区（存储key_array_/rid_array_中的索引）
  KeyType key_array_[LEAF_PAGE_SLOT_CNT];    // 存储键的数组
  ValueType rid_array_[LEAF_PAGE_SLOT_CNT];  // 存储记录ID的数组
  // （2025年春季学期）如果需要，可以在此处添加更多字段和辅助函数
};

}  // namespace bustub