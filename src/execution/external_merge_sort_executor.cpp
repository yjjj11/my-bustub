//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.cpp
//
// Identification: src/execution/external_merge_sort_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/external_merge_sort_executor.h"
#include <vector>
#include "common/macros.h"
#include "execution/plans/sort_plan.h"

namespace bustub {

template <size_t K>
ExternalMergeSortExecutor<K>::ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                                                        std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      cmp_(plan->GetOrderBy()),
      bpm_(exec_ctx->GetBufferPoolManager()) {
  // 核心校验：P3仅要求2路外部归并排序
  BUSTUB_ASSERT(K == 2, "ExternalMergeSortExecutor only supports 2-way merge sort (K=2) for P3");
  BUSTUB_ASSERT(exec_ctx != nullptr, "ExecutorContext cannot be null");
  BUSTUB_ASSERT(plan != nullptr, "SortPlanNode cannot be null");
  BUSTUB_ASSERT(bpm_ != nullptr, "BufferPoolManager cannot be null (from ExecutorContext)");
  std::cout << "确实进入了ExternalMergeSortExecutor\n";

  if (plan->GetOrderBy().empty()) {
    LOG_WARN("SortPlanNode has no ORDER BY rules; the output will be in original order");
  }
}

size_t GetTupleSize(const Tuple &tuple) { return tuple.GetLength() + 4; }

template <size_t K>
void ExternalMergeSortExecutor<K>::GenerateSortedRuns() {
  // 1. 定义内存限制：16KB
  size_t freeframe_count = bpm_->GetFreeFrameCount();
  std::cout << "freeframe_count=" << freeframe_count << "\n";
  size_t MEMORY_LIMIT = (freeframe_count - 1) * 1024;
  // 2. 缓冲区和内存计数变量
  std::vector<SortEntry> sort_buffer;
  size_t current_memory_usage = 0;  // 累计已使用的内存字节数

  std::vector<Tuple> tuple_batch;
  std::vector<RID> rid_batch;
  while (child_executor_->Next(&tuple_batch, &rid_batch, BUSTUB_BATCH_SIZE)) {
    for (const auto &tuple : tuple_batch) {
      size_t tuple_size = GetTupleSize(tuple);

      // 若添加该元组后超过内存限制，先排序并写入磁盘
      if (current_memory_usage + tuple_size > MEMORY_LIMIT && !sort_buffer.empty()) {
        std::cout << "外部归并排序执行器：缓冲区内存已满（已用" << current_memory_usage
                  << "字节），执行内部排序并写入磁盘" << std::endl;
        std::sort(sort_buffer.begin(), sort_buffer.end(), cmp_);
        // 写入磁盘生成Run
        std::vector<page_id_t> run_pages;
        for (const auto &entry : sort_buffer) {
          if (!WriteTupleToPage(entry.second, run_pages)) {
            std::cout << "写入元组到磁盘页失败\n";
          }
        }
        runs_.emplace_back(std::move(run_pages), bpm_);
        sort_buffer.clear();
        current_memory_usage = 0;
      }

      // 向缓冲区添加元组，并累计内存使用量
      SortKey sort_key = GenerateSortKey(tuple, plan_->GetOrderBy(), GetOutputSchema());
      sort_buffer.emplace_back(std::move(sort_key), tuple);
      current_memory_usage += tuple_size;
    }
    // 清空批次数据
    tuple_batch.clear();
    rid_batch.clear();
  }

  // 处理缓冲区中剩余的元组
  if (!sort_buffer.empty()) {
    std::cout << "外部归并排序执行器：处理缓冲区剩余元组（已用" << current_memory_usage
              << "字节），执行内部排序并写入磁盘" << std::endl;
    std::sort(sort_buffer.begin(), sort_buffer.end(), cmp_);
    std::vector<page_id_t> run_pages;
    for (const auto &entry : sort_buffer) {
      if (!WriteTupleToPage(entry.second, run_pages)) {
        std::cout << "处理缓冲区中剩余的元组,写入元组到磁盘页失败\n";
      }
    }
    runs_.emplace_back(std::move(run_pages), bpm_);
  }
}

template <size_t K>
MergeSortRun ExternalMergeSortExecutor<K>::MergeTwoRuns(MergeSortRun &left, MergeSortRun &right) {
  // 1. 初始化两个Run的迭代器
  auto left_it = left.Begin();
  auto left_end = left.End();
  auto right_it = right.Begin();
  auto right_end = right.End();

  // 2. 存储归并后的页列表
  std::vector<page_id_t> merged_pages;

  // 3. 二路归并核心：双指针遍历两个Run，选择较小的元组写入结果
  while (left_it != left_end && right_it != right_end) {
    // 获取两个迭代器指向的元组
    const Tuple &left_tuple = *left_it;
    const Tuple &right_tuple = *right_it;

    // 生成排序键，用于比较
    SortKey left_key = GenerateSortKey(left_tuple, plan_->GetOrderBy(), GetOutputSchema());
    SortKey right_key = GenerateSortKey(right_tuple, plan_->GetOrderBy(), GetOutputSchema());

    // 用比较器判断大小，选择较小的元组写入
    if (cmp_(SortEntry{left_key, left_tuple}, SortEntry{right_key, right_tuple})) {
      WriteTupleToPage(left_tuple, merged_pages);
      ++left_it;  // 左迭代器后移
    } else {
      WriteTupleToPage(right_tuple, merged_pages);
      ++right_it;  // 右迭代器后移
    }
  }

  // 4. 处理左Run的剩余元组
  while (left_it != left_end) {
    WriteTupleToPage(*left_it, merged_pages);
    ++left_it;
  }

  // 5. 处理右Run的剩余元组
  while (right_it != right_end) {
    WriteTupleToPage(*right_it, merged_pages);
    ++right_it;
  }

  // 6. 返回归并后的新Run
  return MergeSortRun(std::move(merged_pages), bpm_);
}

template <size_t K>
void ExternalMergeSortExecutor<K>::MergeRuns() {
  // 1. 基础情况：无Run或只有1个Run，直接赋值
  if (runs_.empty()) {
    merged_run_ = MergeSortRun();
    return;
  }
  if (runs_.size() == 1) {
    merged_run_ = runs_[0];
    std::cout << "外部归并排序执行器：仅1个有序Run，无需归并" << std::endl;
    return;
  }
  std::cout << "有序run个数 = " << runs_.size() << "\n";
  size_t count = 0;
  // 2. 多轮二路归并：用临时列表存储每轮归并后的Run
  std::vector<MergeSortRun> current_runs = runs_;
  while (current_runs.size() > 1) {
    std::cout << "第 " << ++count << "轮\n";
    std::vector<MergeSortRun> next_runs;

    for (size_t i = 0; i < current_runs.size(); i += 2) {
      MergeSortRun &left_run = current_runs[i];
      MergeSortRun *right_run = (i + 1 < current_runs.size()) ? &current_runs[i + 1] : nullptr;

      if (right_run != nullptr) {
        std::cout << "外部归并排序执行器：归并第" << i + 1 << "和" << i + 2 << "个Run" << std::endl;
        // 归并两个Run，生成新的有序Run
        MergeSortRun merged_temp = MergeTwoRuns(left_run, *right_run);
        next_runs.push_back(std::move(merged_temp));
      } else {
        // 剩余单个Run，直接加入下一轮（无需归并）
        next_runs.push_back(std::move(left_run));
      }
    }
    // 更新为下一轮的Run列表
    current_runs = std::move(next_runs);
  }

  // 3. 最终只剩一个Run，赋值给merged_run_
  merged_run_ = std::move(current_runs[0]);
}

template <size_t K>
void ExternalMergeSortExecutor<K>::Init() {
  // 1. 初始化子执行器（如SeqScanExecutor），准备读取待排序元组
  child_executor_->Init();

  // 2. 清空历史数据（避免重复初始化导致的脏数据）
  runs_.clear();
  merged_run_ = MergeSortRun();              // 重置最终归并的Run
  current_iter_ = MergeSortRun::Iterator();  // 重置结果迭代器

  // 3. 核心步骤1：生成分块并执行内部排序，生成多个有序的磁盘Run
  std::cout << "外部归并排序执行器：开始生成有序运行批次（Run）" << std::endl;
  GenerateSortedRuns();
  std::cout << "外部归并排序执行器：成功生成 " << runs_.size() << " 个有序运行批次（Run）" << std::endl;

  // 4. 核心步骤2：执行2路归并，将多个Run合并为一个最终的有序Run
  if (!runs_.empty()) {
    std::cout << "外部归并排序执行器：开始归并 " << runs_.size() << " 个有序运行批次（Run）" << std::endl;
    MergeRuns();
    std::cout << "外部归并排序执行器：完成所有运行批次（Run）的归并" << std::endl;

    // 5. 初始化结果迭代器，指向最终有序Run的起始位置
    current_iter_ = merged_run_.Begin();
  } else {
    current_iter_ = merged_run_.End();
  }
}

/**
 * Yield the next tuple batch from the external merge sort.
 * @param[out] tuple_batch The next tuple batch produced by the external merge sort.
 * @param[out] rid_batch The next tuple RID batch produced by the external merge sort.
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
template <size_t K>
auto ExternalMergeSortExecutor<K>::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                                        size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  // 遍历merged_run_，填充批次
  while (current_iter_ != merged_run_.End() && tuple_batch->size() < batch_size) {
    tuple_batch->push_back(*current_iter_);
    // RID可设为无效（或从元组中提取，根据需求调整）
    rid_batch->emplace_back(INVALID_PAGE_ID, 0);
    ++current_iter_;
  }

  // 返回是否有元组
  return !tuple_batch->empty();
}

template class ExternalMergeSortExecutor<2>;

}  // namespace bustub
