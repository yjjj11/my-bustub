//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// filter_executor.cpp
//
// 标识：src/execution/executors/filter_executor.cpp
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库组
//
//===----------------------------------------------------------------------===//

#include "execution/executors/filter_executor.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * 构造一个新的 FilterExecutor 实例。
 * @param exec_ctx 执行器上下文（包含数据库连接、事务等信息）
 * @param plan 要执行的过滤计划节点（存储过滤条件）
 * @param child_executor 子执行器（提供待过滤的输入数据）
 */
FilterExecutor::FilterExecutor(ExecutorContext *exec_ctx, const FilterPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/** 初始化过滤器执行器 */
void FilterExecutor::Init() {
  // 初始化子执行器（确保子执行器可以开始产生数据）
  child_executor_->Init();
}

/**
 * 从过滤器中生成下一批符合条件的元组。
 * @param[out] tuple_batch 本次输出的符合条件的元组批
 * @param[out] rid_batch 本次输出的元组对应的RID批
 * @param batch_size 批处理大小（一次最多输出的元组数量，默认值为BUSTUB_BATCH_SIZE）
 * @return 若生成了符合条件的元组则返回true，若所有数据已处理完毕则返回false
 */
auto FilterExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                          size_t batch_size) -> bool {
  tuple_batch->clear();  // 清空输出容器
  rid_batch->clear();

  auto filter_expr = plan_->GetPredicate();  // 获取过滤条件（谓词表达式）

  while (true) {
    // 如果子执行器的上一批数据未处理完（child_offset_不为0），继续处理剩余部分
    if (child_offset_ != 0) {
      for (size_t i = child_offset_; i < child_tuples_.size(); ++i) {
        auto &tuple = child_tuples_[i];  // 子执行器输出的元组
        auto &rid = child_rids_[i];      // 元组对应的RID
        // 评估过滤条件
        auto value = filter_expr->Evaluate(&tuple, child_executor_->GetOutputSchema());
        // 若没有过滤条件，或条件评估为true（且非空），则加入输出
        if (filter_expr == nullptr || (!value.IsNull() && value.GetAs<bool>())) {
          tuple_batch->push_back(tuple);
          rid_batch->push_back(rid);
        }
      }
    }

    child_offset_ = 0;  // 重置子执行器数据的处理偏移量

    // 从子执行器获取下一批数据
    const auto status = child_executor_->Next(&child_tuples_, &child_rids_, batch_size);

    // 若子执行器无更多数据，且输出批为空，则返回false（所有数据处理完毕）
    if (!status && tuple_batch->empty()) {
      return false;
    }

    // 若子执行器无更多数据，但输出批非空，则返回true（还有部分数据可输出）
    if (!status && !tuple_batch->empty()) {
      return true;
    }

    // 处理子执行器返回的新一批数据
    for (size_t i = 0; i < child_tuples_.size(); ++i) {
      auto &tuple = child_tuples_[i];
      auto &rid = child_rids_[i];
      // 评估过滤条件
      auto value = filter_expr->Evaluate(&tuple, child_executor_->GetOutputSchema());
      // 若符合条件，加入输出批
      if (filter_expr == nullptr || (!value.IsNull() && value.GetAs<bool>())) {
        tuple_batch->push_back(tuple);
        rid_batch->push_back(rid);
        // 若输出批达到batch_size，检查是否还有未处理的子数据
        if (tuple_batch->size() >= batch_size) {
          // 若子数据未处理完，记录偏移量（下次从这里继续）
          if (i + 1 < child_tuples_.size()) {
            child_offset_ = i + 1;
          } else {
            child_offset_ = 0;
          }
          return true;  // 返回当前批数据
        }
      }
    }
  }
}

}  // namespace bustub