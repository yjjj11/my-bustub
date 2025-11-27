//===----------------------------------------------------------------------===//
//
//                         BusTub 数据库
//
// nested_loop_join_executor.cpp
//
// 标识：src/execution/nested_loop_join_executor.cpp
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库研究组
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "common/macros.h"
#include "type/value_factory.h"
namespace bustub {

/**
 * 构造一个新的嵌套循环连接执行器实例。
 * @param exec_ctx 执行器上下文
 * @param plan 待执行的嵌套循环连接计划
 * @param left_executor 为连接左侧生成元组的子执行器
 * @param right_executor 为连接右侧生成元组的子执行器
 */
NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)),
      offset_(0),
      left_exhausted_(false) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }

  std::cout << "进入了NestedLoopJoinExecutor\n";
}

/** 初始化连接执行器 */
void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  left_tuple_.clear();
  offset_ = 0;
  left_exhausted_ = false;
  backup_rids_.clear();
  backup_tuple_.clear();
  std::vector<Tuple> tuple_batch;
  std::vector<RID> rid_batch;
  while (right_executor_->Next(&tuple_batch, &rid_batch, 20)) {
    for (size_t i = 0; i < tuple_batch.size(); i++) {
      all_inner_rid_.push_back(rid_batch[i]);
      all_inner_tuple_.push_back(tuple_batch[i]);
    }
  }
  // 存储全部右表元素
}

/**
 * 生成连接操作的下一批元组。
 * @param[out] tuple_batch 连接产生的下一批元组（输出参数）
 * @param[out] rid_batch 连接产生的下一批元组对应的RID（输出参数）
 * @param batch_size 批次中要包含的元组数量（默认值：BUSTUB_BATCH_SIZE）
 * @return 若产生了元组则返回true，若已无更多元组则返回false
 */
auto NestedLoopJoinExecutor::Next(std::vector<Tuple> *tuple_batch, std::vector<RID> *rid_batch, size_t batch_size)
    -> bool {
  tuple_batch->clear();
  rid_batch->clear();
  // 优先消费上一次的备份缓存（核心：保存上一次的进度）
  size_t consume_count = 0;
  while (consume_count < batch_size && !backup_tuple_.empty()) {
    tuple_batch->push_back(backup_tuple_.front());
    rid_batch->push_back(backup_rids_.front());
    backup_tuple_.pop_front();
    backup_rids_.pop_front();
    consume_count++;
  }
  if (consume_count == batch_size) {
    return true;
  }

  // 3. 备份缓存未填满批次，继续生成新的连接结果
  const size_t remain = batch_size - consume_count;
  while (backup_tuple_.size() < remain && !left_exhausted_) {
    // 3.1 左表批量元组遍历完毕，拉取新的左表批量
    if (offset_ >= left_tuple_.size()) {
      std::vector<RID> left_rid_batch;
      left_tuple_.clear();
      if (!left_executor_->Next(&left_tuple_, &left_rid_batch, batch_size)) {
        left_exhausted_ = true;
        break;
      }
      offset_ = 0;  // 重置左表批量索引
    }

    if (left_tuple_.empty()) {
      break;
    }

    // 3.3 处理当前左表元组
    current_left_tuple_ = left_tuple_[offset_];
    offset_++;
    right_executor_->Init();
    // std::cout<<"左表为"<<current_left_tuple_.ToString(&left_executor_->GetOutputSchema())<<"\n";
    // 遍历所有右表元组
    bool has_join = false;

    // 3.5 批量遍历右表元组
    for (size_t i = 0; i < all_inner_tuple_.size(); i++) {
      const auto &current_right_tuple = all_inner_tuple_[i];
      const auto &current_right_rid = all_inner_rid_[i];
      // std::cout<<"右表为"<<current_right_tuple.ToString(&right_executor_->GetOutputSchema())<<"\n";
      // 计算连接条件
      Value predicate_result =
          plan_->Predicate()->EvaluateJoin(&current_left_tuple_, left_executor_->GetOutputSchema(),
                                           &current_right_tuple, right_executor_->GetOutputSchema());

      // 连接条件满足，构造结果元组并加入备份缓存
      if (!predicate_result.IsNull() && predicate_result.GetAs<bool>()) {
        std::vector<Value> values;
        // 拼接左表列
        for (size_t i = 0; i < left_executor_->GetOutputSchema().GetColumns().size(); i++) {
          values.push_back(current_left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        // 拼接右表列
        for (size_t i = 0; i < right_executor_->GetOutputSchema().GetColumns().size(); i++) {
          values.push_back(current_right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
        }
        backup_tuple_.push_back(Tuple(values, &GetOutputSchema()));
        backup_rids_.push_back(current_right_rid);
        has_join = true;
      }
    }

    // 3.6 左连接：当前左表元组无任何右表匹配，填充NULL值到备份缓存
    if (!has_join && plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values;
      // 拼接左表列
      for (size_t i = 0; i < left_executor_->GetOutputSchema().GetColumns().size(); i++) {
        values.push_back(current_left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
      }
      // 右表列填充NULL
      for (size_t i = 0; i < right_executor_->GetOutputSchema().GetColumns().size(); i++) {
        values.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
      }
      backup_tuple_.push_back(Tuple(values, &GetOutputSchema()));
      backup_rids_.push_back(RID{});
    }
  }

  // 4. 消费新生成的备份缓存（填充剩余的批次空间）
  while (consume_count < batch_size && !backup_tuple_.empty()) {
    tuple_batch->push_back(backup_tuple_.front());
    rid_batch->push_back(backup_rids_.front());
    backup_tuple_.pop_front();
    backup_rids_.pop_front();
    consume_count++;
  }

  // 5. 若批次中有数据则返回true，否则返回false
  return !tuple_batch->empty();
}

}  // namespace bustub