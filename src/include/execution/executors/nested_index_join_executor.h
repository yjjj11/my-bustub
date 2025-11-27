//===----------------------------------------------------------------------===//
//
//                         BusTub 数据库
//
// nested_index_join_executor.h
//
// 标识：src/include/execution/executors/nested_index_join_executor.h
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库研究组
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/nested_index_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * NestedIndexJoinExecutor 用于执行基于索引的嵌套连接操作。
 */
class NestedIndexJoinExecutor : public AbstractExecutor {
 public:
  /**
   * 构造基于索引的嵌套连接执行器实例
   * @param exec_ctx 执行器上下文
   * @param plan 要执行的基于索引的嵌套连接计划节点
   * @param child_executor 生成外表元组的子执行器
   */
  NestedIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                          std::unique_ptr<AbstractExecutor> &&child_executor);

  /** @return 基于索引的嵌套连接的输出模式 */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  /** 初始化基于索引的嵌套连接执行器 */
  void Init() override;

  /**
   * 获取基于索引的嵌套连接产生的下一批元组
   * @param tuple_batch 存储连接产生的下一批元组的容器
   * @param rid_batch 存储连接产生的下一批元组对应RID的容器
   * @param batch_size 批次中元组的数量
   * @return 若产生了元组则返回true，若已无更多元组则返回false
   */
  auto Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch, size_t batch_size)
      -> bool override;

  Tuple make_tuple_Null(const Tuple &scan_tuple);

  Tuple make_tuple(const Tuple &scan_tuple, const Tuple &inner_tuple);

 private:
  /** 待执行的基于索引的嵌套连接计划节点。 */
  const NestedIndexJoinPlanNode *plan_;
  /** 外表的执行器（子执行器） */
  std::unique_ptr<AbstractExecutor> child_executor_;
  std::shared_ptr<TableInfo> inner_table_info_;
  std::shared_ptr<IndexInfo> index_;

  std::vector<Tuple> outer_batch_;
  size_t offset_{0};
  bool is_exhausted_{false};
};
}  // namespace bustub
