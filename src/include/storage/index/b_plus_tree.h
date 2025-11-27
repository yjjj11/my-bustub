//===----------------------------------------------------------------------===//
//
//                         BusTub（巴士桶）
//
// b_plus_tree.h
//
// 标识：src/include/storage/index/b_plus_tree.h
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库组
//
//===----------------------------------------------------------------------===//

/**
 * b_plus_tree.h
 *
 * 简单 B+ 树数据结构的实现，其中内部页用于引导查找，叶子页包含实际数据。
 * （1）仅支持唯一键
 * （2）支持插入和删除操作
 * （3）结构应能动态收缩和增长
 * （4）实现索引迭代器用于范围扫描
 */
#pragma once

#include <algorithm>
#include <deque>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <queue>
#include <shared_mutex>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/macros.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

struct PrintableBPlusTree;

/**
 * @brief Context 类的定义。
 *
 * 提示：此类用于帮助跟踪正在修改或访问的页。
 */
class Context {
 public:
  // 当向 B+ 树插入或删除数据时，将头部页的写保护锁存储在此处。
  // 记住，当需要解锁所有页时，需释放头部页的保护锁并将其设为 nullopt。
  std::optional<WritePageGuard> header_page_{std::nullopt};

  // 将根页 ID 存储在此处，便于判断当前页是否为根页。
  page_id_t root_page_id_{INVALID_PAGE_ID};

  // 将正在修改的页的写保护锁存储在此处。
  std::deque<WritePageGuard> write_set_;

  // 获取值时可能会用到此容器，但非必需。
  std::deque<ReadPageGuard> read_set_;

  auto IsRootPage(page_id_t page_id) -> bool { return page_id == root_page_id_; }

  void Drop_head() {
    header_page_.value().Drop();
    header_page_ = std::nullopt;
  }

  void Drop_write_latch() {
    while (!write_set_.empty()) {
      write_set_.pop_front();
    }
  }
  void keep_last_write_latch() {  // 珊到只剩末尾为止
    while (write_set_.size() > 1) {
      write_set_.pop_front();
    }
  }
  void keep_last_read_latch(long unsigned int sizet) {  // 珊到只剩末尾为止
    while (read_set_.size() > sizet) {
      read_set_.pop_front();
    }
  }
};

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator, NumTombs>

// 提供交互式 B+ 树 API 的主类。
FULL_INDEX_TEMPLATE_ARGUMENTS_DEFN
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  explicit BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                     const KeyComparator &comparator, int leaf_max_size = LEAF_PAGE_SLOT_CNT,
                     int internal_max_size = INTERNAL_PAGE_SLOT_CNT);

  // 如果此 B+ 树没有键和值，则返回 true。
  auto IsEmpty() const -> bool;

  // 向此 B+ 树插入一个键值对。
  auto Insert(const KeyType &key, const ValueType &value) -> bool;

  auto Insert_write(const KeyType &key, const ValueType &value) -> bool;

  // 从 B+ 树中删除一个键及其对应的值。
  void Remove(const KeyType &key);

  void Remove_write(const KeyType &key);

  // 返回与给定键相关联的值。
  auto GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool;

  // 返回根节点的页 ID。
  auto GetRootPageId() -> page_id_t;

  // 索引迭代器
  auto Begin() -> INDEXITERATOR_TYPE;

  auto End() -> INDEXITERATOR_TYPE;

  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;

  void Print(BufferPoolManager *bpm);

  void Draw(BufferPoolManager *bpm, const std::filesystem::path &outf);

  auto DrawBPlusTree() -> std::string;

  // 从文件读取数据并逐个插入。
  void InsertFromFile(const std::filesystem::path &file_name);

  // 从文件读取数据并逐个删除。
  void RemoveFromFile(const std::filesystem::path &file_name);

  void BatchOpsFromFile(const std::filesystem::path &file_name);

  // 不要将此类型改为 BufferPoolManager！
  std::shared_ptr<TracedBufferPoolManager> bpm_;

 private:
  void ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out);

  void PrintTree(page_id_t page_id, const BPlusTreePage *page);

  auto ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree;

  // 创建新根节点（内部页）
  bool CreateNewRoot(page_id_t left_page_id, const KeyType &split_key, page_id_t right_page_id, Context &ctx);
  // 叶子页插入核心逻辑（处理满节点拆分）
  auto InsertIntoLeaf(Context &ctx, page_id_t leaf_page_id, const KeyType &key, const ValueType &value) -> bool;
  // 内部页插入核心逻辑（处理满节点拆分与递归上溢）
  auto InsertIntoInternal(Context &ctx, page_id_t internal_page_id, const KeyType &key, page_id_t child_page_id)
      -> bool;

  bool IsLeafUnderflow(LeafPage *leaf) const;

  bool IsInternalUnderflow(InternalPage *internal) const;

  bool FindLeafSibling(Context &ctx, page_id_t current_page_id, page_id_t *sibling_page_id_left,
                       page_id_t *sibling_page_id_right, int *current_idx, int *sibling_idx_left,
                       int *sibling_idx_right, InternalPage **parent_internal);

  void RedistributeLeaf(Context &ctx, LeafPage *current_leaf, LeafPage *sibling_leaf, int current_idx, int sibling_idx,
                        InternalPage *parent);

  void MergeLeaf(Context &ctx, LeafPage *current_leaf, LeafPage *sibling_leaf, int current_idx, int sibling_idx,
                 InternalPage *parent, page_id_t current_page_id);

  void HandleLeafUnderflow(Context &ctx, page_id_t current_page_id);

  bool RemoveFromLeaf(Context &ctx, page_id_t leaf_page_id, const KeyType &key);

  void RedistributeInternal(Context &ctx, InternalPage *current_internal, InternalPage *sibling_internal,
                            int current_idx, int sibling_idx, InternalPage *parent);

  bool FindInternalSibling(Context &ctx, page_id_t current_page_id, page_id_t *sibling_page_id_left,
                           page_id_t *sibling_page_id_right, int *current_idx, int *sibling_idx_left,
                           int *sibling_idx_right, InternalPage **parent_internal);

  void MergeInternal(Context &ctx, InternalPage *current_internal, InternalPage *sibling_internal, int current_idx,
                     int sibling_idx, InternalPage *parent, page_id_t current_page_id);

  void HandleInternalUnderflow(Context &ctx, page_id_t current_page_id);

  // 成员变量
  std::string index_name_;
  KeyComparator comparator_;
  std::vector<std::string> log;  // NOLINT
  int leaf_max_size_;
  int internal_max_size_;
  page_id_t header_page_id_;
};

/**
 * @brief 仅用于测试。PrintableBPlusTree 是可打印的 B+ 树。
 * 我们先将 B+ 树转换为可打印的 B+ 树，然后再打印它。
 */
struct PrintableBPlusTree {
  int size_;
  std::string keys_;
  std::vector<PrintableBPlusTree> children_;

  /**
   * @brief 广度优先遍历可打印的 B+ 树并将其打印到 out_buf 中。
   *
   * @param out_buf 输出缓冲区
   */
  void Print(std::ostream &out_buf) {
    std::vector<PrintableBPlusTree *> que = {this};
    while (!que.empty()) {
      std::vector<PrintableBPlusTree *> new_que;

      for (auto &t : que) {
        int padding = (t->size_ - t->keys_.size()) / 2;
        out_buf << std::string(padding, ' ');
        out_buf << t->keys_;
        out_buf << std::string(padding, ' ');

        for (auto &c : t->children_) {
          new_que.push_back(&c);
        }
      }
      out_buf << "\n";
      que = new_que;
    }
  }
};

}  // namespace bustub