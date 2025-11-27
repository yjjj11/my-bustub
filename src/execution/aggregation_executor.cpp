//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2025，卡内基梅隆大学数据库组
//
//===----------------------------------------------------------------------===//

#include "execution/executors/aggregation_executor.h"
#include <memory>
#include "common/macros.h"

namespace bustub {

/**
 * 构造聚合执行器
 * @param exec_ctx 执行器上下文
 * @param plan 聚合计划节点
 * @param child_executor 子执行器（提供聚合数据源）
 */
AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.End()),
      is_aggregated_(false) {}

/** 初始化聚合执行器：遍历子执行器元组并完成聚合计算 */
void AggregationExecutor::Init() {
  // 初始化子执行器
  child_executor_->Init();

  // 若已完成聚合，直接返回（避免重复计算）
  if (is_aggregated_) {
    return;
  }

  // 遍历子执行器的所有元组，插入聚合哈希表
  std::vector<Tuple> tuple_batch;
  std::vector<RID> rid_batch;
  while (child_executor_->Next(&tuple_batch, &rid_batch, BUSTUB_BATCH_SIZE)) {
    for (const auto &tuple : tuple_batch) {
      // 转换为聚合键和聚合值
      AggregateKey agg_key = MakeAggregateKey(&tuple);
      AggregateValue agg_val = MakeAggregateValue(&tuple);
      // 插入并合并到哈希表
      aht_.InsertCombine(agg_key, agg_val);
    }
    // 清空批次，准备下一次读取
    tuple_batch.clear();
    rid_batch.clear();
  }

  // 标记聚合完成，初始化迭代器
  is_aggregated_ = true;
  aht_iterator_ = aht_.Begin();
}

/**
 * 输出聚合结果的下一个批次
 * @param tuple_batch 输出元组批次
 * @param rid_batch 输出RID批次（聚合操作无实际RID，置空）
 * @param batch_size 批次大小
 * @return 是否有更多结果
 */
auto AggregationExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                               size_t batch_size) -> bool {
  if (is_null_and_done_) return false;
  // 清空输出批次
  tuple_batch->clear();
  rid_batch->clear();

  // 第一步：若未完成聚合，先执行初始化（遍历子执行器元组并聚合）
  if (!is_aggregated_) {
    Init();
  }

  // 第二步：处理「全局聚合 + 哈希表为空」的特殊场景（空表）
  bool is_global_agg = plan_->GetGroupBys().empty();                                  // 无分组键 = 全局聚合
  bool is_aht_empty = (aht_iterator_ == aht_.End()) && (aht_.Begin() == aht_.End());  // 哈希表为空

  if (is_global_agg && is_aht_empty) {
    // 手动构造初始聚合值的元组，不插入哈希表，避免累加
    AggregateKey empty_key{};                                           // 空分组键（全局聚合）
    AggregateValue initial_val = aht_.GenerateInitialAggregateValue();  // 获取初始值（COUNT(*)为0，其他为NULL）
    Tuple output_tuple = MakeOutputTuple(empty_key, initial_val);       // 构造输出元组
    tuple_batch->push_back(output_tuple);                               // 加入结果批次
    rid_batch->push_back(RID{});
    is_null_and_done_ = true;
    return true;  // 返回true，表示有结果
  }

  // 第三步：正常遍历哈希表，生成结果批次（原有逻辑）
  size_t count = 0;
  while (aht_iterator_ != aht_.End() && count < batch_size) {
    Tuple output_tuple = MakeOutputTuple(aht_iterator_.Key(), aht_iterator_.Val());
    tuple_batch->push_back(output_tuple);
    rid_batch->push_back(RID{});
    ++aht_iterator_;
    ++count;
  }

  // 若批次中有数据，返回true；否则返回false
  return !tuple_batch->empty();
}

/** 获取子执行器（框架要求，不可删除） */
auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub