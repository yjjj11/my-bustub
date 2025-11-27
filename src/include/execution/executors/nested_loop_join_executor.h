//===----------------------------------------------------------------------===//
//
//                         BusTub 数据库
//
// nested_loop_join_executor.h
//
// 标识：src/include/execution/executors/nested_loop_join_executor.h
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库研究组
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * NestedLoopJoinExecutor 用于在两张表上执行嵌套循环连接操作。
 */
class NestedLoopJoinExecutor : public AbstractExecutor {
 public:
  /**
   * 构造嵌套循环连接执行器实例
   * @param exec_ctx 执行器上下文
   * @param plan 要执行的嵌套循环连接计划节点
   * @param left_executor 为连接左侧生成元组的子执行器
   * @param right_executor 为连接右侧生成元组的子执行器
   */
  NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&left_executor,
                         std::unique_ptr<AbstractExecutor> &&right_executor);

  /** 初始化连接执行器 */
  void Init() override;

  /**
   * 获取连接操作产生的下一批元组
   * @param tuple_batch 存储连接产生的下一批元组的容器
   * @param rid_batch 存储连接产生的下一批元组对应RID的容器
   * @param batch_size 批次中元组的数量
   * @return 若产生了元组则返回true，若已无更多元组则返回false
   */
  auto Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch, size_t batch_size)
      -> bool override;

  /** @return 连接操作的输出模式 */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** 待执行的嵌套循环连接计划节点。 */
  const NestedLoopJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_executor_;
  std::unique_ptr<AbstractExecutor> right_executor_;
  std::vector<Tuple> left_tuple_;
  Tuple current_left_tuple_;
  size_t offset_;
  bool left_exhausted_;
  std::deque<Tuple> backup_tuple_;
  std::deque<RID> backup_rids_;

  std::vector<Tuple> all_inner_tuple_;
  std::vector<RID> all_inner_rid_;
};

}  // namespace bustub