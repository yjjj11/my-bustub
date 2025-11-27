//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.h
//
// 标识：src/include/execution/executors/limit_executor.h
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库研究组
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "execution/executors/abstract_executor.h"
#include "execution/plans/limit_plan.h"

namespace bustub {

/**
 * LimitExecutor 用于限制其子执行器输出的元组数量。
 */
class LimitExecutor : public AbstractExecutor {
 public:
  /**
   * 构造 LimitExecutor 实例。
   * @param exec_ctx 执行器上下文
   * @param plan 要执行的 Limit 计划
   * @param child_executor 用于获取元组的子执行器
   */
  LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                std::unique_ptr<AbstractExecutor> &&child_executor);

  /** 初始化 Limit 执行器 */
  void Init() override;

  /**
   * 从 Limit 执行器中获取下一个元组批次。
   * @param[out] tuple_batch Limit 执行器生成的下一个元组批次
   * @param[out] rid_batch Limit 执行器生成的下一个元组 RID 批次
   * @param batch_size 批次中包含的元组数量（默认：BUSTUB_BATCH_SIZE）
   * @return 若生成了元组则返回 `true`，若无更多元组则返回 `false`
   */
  auto Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch, size_t batch_size)
      -> bool override;

  /** @return Limit 执行器的输出模式 */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  const LimitPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> child_executor_;
  size_t output_count_ = 0;
};
}  // namespace bustub