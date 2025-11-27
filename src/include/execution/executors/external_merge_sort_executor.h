//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.h
//
// 标识：src/include/execution/executors/external_merge_sort_executor.h
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库研究组
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>
#include "buffer/buffer_pool_manager.h"  // 补充BPM的头文件依赖
#include "common/config.h"
#include "common/macros.h"
#include "execution/execution_common.h"  // 包含SortKey、SortEntry、TupleComparator
#include "execution/executors/abstract_executor.h"
#include "execution/plans/sort_plan.h"
#include "storage/page/intermediate_result_page.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * 外部归并排序中用于存储已排序元组的“运行”（Run）数据结构。
 * 元组可能存储在多个页中，且元组在单个页内和跨页都是有序的。
 */
class MergeSortRun {
 public:
  MergeSortRun() = default;
  MergeSortRun(std::vector<page_id_t> pages, BufferPoolManager *bpm) : pages_(std::move(pages)), bpm_(bpm) {}

  /** 获取运行包含的页数 */
  auto GetPageCount() -> size_t { return pages_.size(); }

  /** 用于遍历单个运行中已排序元组的迭代器 */
  class Iterator {
    friend class MergeSortRun;

   public:
    Iterator() = default;

    /**
     * 推进迭代器到下一个元组。若当前页的元组已耗尽，则移动到下一个页。
     * 与execution_common的SortKey/TupleComparator完全兼容，遍历的是已排序的元组
     */
    auto operator++() -> Iterator & {
      BUSTUB_ASSERT(run_ != nullptr, "Iterator is not associated with any MergeSortRun");
      BUSTUB_ASSERT(bpm_ != nullptr, "BufferPoolManager is null in MergeSortRun::Iterator");

      // 1. 读取当前页，判断元组是否耗尽
      if (page_idx_ < run_->pages_.size()) {
        ReadPageGuard guard = bpm_->ReadPage(run_->pages_[page_idx_]);
        const auto *page = guard.As<IntermediateResultPage>();
        size_t tuple_count = page->get_tuple_count();

        // 2. 当前页内的下一个元组
        tuple_idx_++;
        // 3. 若当前页元组耗尽，切换到下一页并重置元组索引
        if (tuple_idx_ >= tuple_count) {
          page_idx_++;
          tuple_idx_ = 0;
        }
      }

      return *this;
    }

    /**
     * 解引用迭代器，获取当前指向的已排序元组。
     * 直接从IntermediateResultPage读取元组，与SortKey的排序结果一致
     */
    auto operator*() -> Tuple {
      BUSTUB_ASSERT(run_ != nullptr, "Iterator is not associated with any MergeSortRun");
      BUSTUB_ASSERT(bpm_ != nullptr, "BufferPoolManager is null in MergeSortRun::Iterator");
      BUSTUB_ASSERT(page_idx_ < run_->pages_.size(), "Page index out of bounds in MergeSortRun::Iterator");

      // 1. 读取当前页
      ReadPageGuard guard = bpm_->ReadPage(run_->pages_[page_idx_]);
      const auto *page = guard.As<IntermediateResultPage>();
      BUSTUB_ASSERT(tuple_idx_ < page->get_tuple_count(), "Tuple index out of bounds in MergeSortRun::Iterator");

      // 2. 从页中读取元组并返回
      Tuple tuple;
      bool read_success = page->ReadTuple(tuple_idx_, tuple);
      BUSTUB_ASSERT(read_success, "Failed to read tuple from IntermediateResultPage");

      return tuple;
    }

    /**
     * 检查两个迭代器是否指向同一个运行中的同一个元组。
     */
    auto operator==(const Iterator &other) const -> bool {
      return (run_ == other.run_) && (page_idx_ == other.page_idx_) && (tuple_idx_ == other.tuple_idx_);
    }

    /**
     * 检查两个迭代器是否指向不同元组，或属于不同运行。
     */
    auto operator!=(const Iterator &other) const -> bool { return !(*this == other); }

   private:
    /**
     * 私有化构造函数：仅允许MergeSortRun创建迭代器
     * @param run 所属的MergeSortRun
     * @param bpm 缓冲池管理器（用于读取页）
     */
    explicit Iterator(const MergeSortRun *run, BufferPoolManager *bpm) : run_(run), bpm_(bpm) {
      page_idx_ = 0;
      tuple_idx_ = 0;
    }

    /** 迭代器所属的排序运行 */
    const MergeSortRun *run_ = nullptr;
    /** 缓冲池管理器（用于读取页数据） */
    BufferPoolManager *bpm_ = nullptr;
    /** 当前遍历的页索引 */
    size_t page_idx_ = 0;
    /** 当前页内的元组索引 */
    size_t tuple_idx_ = 0;
  };

  /**
   * 获取指向运行起始位置的迭代器（即第一个元组）。
   * 与execution_common的排序逻辑联动，返回已排序的首个元组
   */
  auto Begin() -> Iterator { return Iterator(this, bpm_); }

  /**
   * 获取指向运行结束位置的迭代器（即最后一个元组之后的位置）。
   */
  auto End() -> Iterator {
    Iterator iter(this, bpm_);
    iter.page_idx_ = pages_.size();  // 页索引超出范围表示结束
    iter.tuple_idx_ = 0;
    return iter;
  }

  /**
   * 向当前运行中添加一个页（用于归并时写入已排序的元组页）
   * @param page_id 待添加的页ID
   */
  void AddPage(page_id_t page_id) { pages_.push_back(page_id); }

  /**
   * 获取运行的页ID列表（只读）
   */
  auto GetPages() const -> const std::vector<page_id_t> & { return pages_; }

 private:
  /** 存储已排序元组的页ID列表（按排序顺序存储） */
  std::vector<page_id_t> pages_;
  /**
   * 用于读取/写入排序页的缓冲池管理器。
   * 缓冲池管理器负责在页不再需要时删除它们，与execution_common的元组存储逻辑兼容
   */
  BufferPoolManager *bpm_ = nullptr;
};

/**
 * ExternalMergeSortExecutor 执行外部归并排序。
 * 2025年春季学期仅要求实现2路外部归并排序，与execution_common的SortKey/TupleComparator深度联动
 * @tparam K 归并路数，P3要求K=2
 */
template <size_t K>
class ExternalMergeSortExecutor : public AbstractExecutor {
 public:
  /**
   * 构造外部归并排序执行器
   * @param exec_ctx 执行器上下文
   * @param plan 排序计划节点（包含ORDER BY规则）
   * @param child_executor 子执行器（提供待排序的元组）
   */
  ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                            std::unique_ptr<AbstractExecutor> &&child_executor);

  void Init() override;

  auto Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch, size_t batch_size)
      -> bool override;

  /** @return 外部归并排序的输出模式 */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /**
   * 辅助函数：生成一个新的中间结果页（用于存储排序后的元组）
   * @return 新页的ID，失败返回INVALID_PAGE_ID
   */
  auto NewIntermediatePage() -> page_id_t {
    page_id_t page_id = bpm_->NewPage();
    if (page_id == INVALID_PAGE_ID) {
      return INVALID_PAGE_ID;
    }
    // 初始化新页的元数据
    WritePageGuard guard = bpm_->WritePage(page_id);
    auto *page = guard.AsMut<IntermediateResultPage>();
    page->InitMetadata(BUSTUB_PAGE_SIZE);
    return page_id;
  }

  /**
   * 辅助函数：将元组写入中间结果页（满页则创建新页）
   * @param tuple 待写入的元组
   * @param pages 存储页ID的列表（会自动添加新页）
   * @return 写入是否成功
   */
  auto WriteTupleToPage(const Tuple &tuple, std::vector<page_id_t> &pages) -> bool {
    if (pages.empty()) {
      // 无页则创建新页
      page_id_t page_id = NewIntermediatePage();
      if (page_id == INVALID_PAGE_ID) {
        return false;
      }
      pages.push_back(page_id);
    }

    // 写入最后一个页
    page_id_t last_page_id = pages.back();
    WritePageGuard guard = bpm_->WritePage(last_page_id);
    auto *page = guard.AsMut<IntermediateResultPage>();
    if (page->WriteTuple(tuple)) {
      return true;
    }

    // 最后一页已满，创建新页并写入
    page_id_t new_page_id = NewIntermediatePage();
    if (new_page_id == INVALID_PAGE_ID) {
      return false;
    }
    pages.push_back(new_page_id);
    WritePageGuard new_guard = bpm_->WritePage(new_page_id);
    auto *new_page = new_guard.AsMut<IntermediateResultPage>();
    new_page->InitMetadata(BUSTUB_PAGE_SIZE);
    new_page->WriteTuple(tuple);
    return true;
  }

  /**
   * 阶段1：生成分段（Run）并进行内部排序
   * 从子执行器读取元组，按内存限制分块，用TupleComparator排序后写入磁盘
   */
  void GenerateSortedRuns();

  /**
   * 阶段2：2路归并排序
   * 将多个已排序的Run归并为一个最终的有序Run，与execution_common的比较器联动
   */
  void MergeRuns();

  MergeSortRun MergeTwoRuns(MergeSortRun &left, MergeSortRun &right);

  /** 要执行的排序计划节点 */
  const SortPlanNode *plan_;
  /** 子执行器（提供待排序的元组） */
  std::unique_ptr<AbstractExecutor> child_executor_;
  /** 基于ORDER BY规则的元组比较器（来自execution_common） */
  TupleComparator cmp_;
  /** 缓冲池管理器（用于操作中间结果页） */
  BufferPoolManager *bpm_;

  /** 归并排序的中间运行列表（阶段1生成的已排序Run） */
  std::vector<MergeSortRun> runs_;
  /** 最终归并后的结果运行（阶段2生成） */
  MergeSortRun merged_run_;
  /** 结果运行的迭代器（用于Next接口输出元组） */
  MergeSortRun::Iterator current_iter_;
};

}  // namespace bustub