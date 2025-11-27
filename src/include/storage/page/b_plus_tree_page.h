//===----------------------------------------------------------------------===//
//
//                         BusTub（巴士桶）
//
// b_plus_tree_page.h
//
// 标识：src/include/storage/page/b_plus_tree_page.h
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库组
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cassert>
#include <climits>
#include <cstdlib>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "storage/index/generic_key.h"

namespace bustub {

#define MappingType std::pair<KeyType, ValueType>

#define FULL_INDEX_TEMPLATE_ARGUMENTS_DEFN \
  template <typename KeyType, typename ValueType, typename KeyComparator, ssize_t NumTombs = 0>
#define FULL_INDEX_TEMPLATE_ARGUMENTS \
  template <typename KeyType, typename ValueType, typename KeyComparator, ssize_t NumTombs>
#define INDEX_TEMPLATE_ARGUMENTS template <typename KeyType, typename ValueType, typename KeyComparator>

// 定义页类型枚举
enum class IndexPageType { INVALID_INDEX_PAGE = 0, LEAF_PAGE, INTERNAL_PAGE };

/**
 * 内部页和叶子页都继承自该页。
 *
 * 它实际上充当每个B+树页的头部部分，
 * 包含叶子页和内部页共有的信息。
 *
 * 头部格式（字节大小，总共12字节）：
 * ---------------------------------------------------------
 * | 页类型 (4) | 当前大小 (4) | 最大大小 (4) |  ...   |
 * ---------------------------------------------------------
 */
class BPlusTreePage {
 public:
  // 删除所有构造函数/析构函数以确保内存安全
  BPlusTreePage() = delete;
  BPlusTreePage(const BPlusTreePage &other) = delete;
  ~BPlusTreePage() = delete;

  // 判断是否为叶子页
  auto IsLeafPage() const -> bool;
  // 设置页类型
  void SetPageType(IndexPageType page_type);

  // 获取当前大小
  auto GetSize() const -> int;
  // 设置当前大小
  // 调整大小（增减指定数量）
  void ChangeSizeBy(int amount);

  // 获取最大大小
  auto GetMaxSize() const -> int;
  // 设置最大大小
  void SetMaxSize(int max_size);
  // 获取最小大小
  auto GetMinSize() const -> int;

  // 新增：直接设置size
  void SetSize(int size) { size_ = size; }
  // 新增：size++
  void IncreaseSize() { size_++; }
  // 新增：size--
  void DecreaseSize() { size_--; }
  /*
   * TODO(P2)：如果打算使用这些字段，请移除 __attribute__((__unused__))。
   */
 protected:
  // 成员变量，内部页和叶子页共有的属性
  IndexPageType page_type_;  // 页类型
  int size_;                 // 页中的键值对数量
  int max_size_;             // 页中最多可容纳的键值对数量
};

}  // namespace bustub
