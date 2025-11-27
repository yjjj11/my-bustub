//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// 标识：src/execution/limit_executor.cpp
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库研究组
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"
#include "common/macros.h"

namespace bustub {

/**
 * 构造 LimitExecutor 实例。
 * @param exec_ctx 执行器上下文
 * @param plan 要执行的 Limit 计划
 * @param child_executor 用于获取受限制元组的子执行器
 */
LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
  child_executor_->Init();
  output_count_ = 0;
}

/**
 * 从 Limit 执行器中获取下一个元组批次。
 * @param[out] tuple_batch Limit 执行器生成的下一个元组批次
 * @param[out] rid_batch Limit 执行器生成的下一个元组 RID 批次
 * @param batch_size 批次中包含的元组数量（默认：BUSTUB_BATCH_SIZE）
 * @return 若生成了元组则返回 `true`，若无更多元组则返回 `false`
 */
auto LimitExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                         size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  // 若已输出的元组数量达到 Limit 限制，直接返回 false
  if (output_count_ >= plan_->GetLimit()) {
    return false;
  }

  std::vector<Tuple> child_tuples;
  std::vector<RID> child_rids;
  if (!child_executor_->Next(&child_tuples, &child_rids, batch_size)) {
    // 子执行器无更多元组，返回当前批次（若有）
    return !tuple_batch->empty();
  }

  size_t remaining = plan_->GetLimit() - output_count_;
  size_t take = std::min(remaining, child_tuples.size());

  tuple_batch->insert(tuple_batch->end(), child_tuples.begin(), child_tuples.begin() + take);
  rid_batch->insert(rid_batch->end(), child_rids.begin(), child_rids.begin() + take);

  output_count_ += take;
  return true;
}

}  // namespace bustub