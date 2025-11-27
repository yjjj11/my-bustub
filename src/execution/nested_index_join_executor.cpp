//===----------------------------------------------------------------------===//
//
//                         BusTub 数据库
//
// nested_index_join_executor.cpp
//
// 标识：src/execution/nested_index_join_executor.cpp
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库研究组
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "common/macros.h"
#include "type/value_factory.h"
namespace bustub {

/**
 * 创建一个新的基于索引的嵌套连接执行器。
 * @param exec_ctx 执行基于索引的嵌套连接的上下文
 * @param plan 待执行的基于索引的嵌套连接计划
 * @param child_executor 外表的执行器（子执行器）
 */
NestedIndexJoinExecutor::NestedIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                                 std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // 2025年春季学期备注：你只需要实现左连接和内连接。
    throw bustub::NotImplementedException(fmt::format("不支持的连接类型：{}", plan->GetJoinType()));
  }

  auto *catalog = exec_ctx->GetCatalog();
  inner_table_info_ = catalog->GetTable(plan->GetInnerTableOid());
  index_ = catalog->GetIndex(plan_->GetIndexName(), inner_table_info_->name_);
}

void NestedIndexJoinExecutor::Init() {
  child_executor_->Init();
  outer_batch_.clear();
  offset_ = 0;
  is_exhausted_ = false;
}

/**
 * 获取基于索引的嵌套连接产生的下一批元组
 * @param[out] tuple_batch 连接产生的下一批元组（输出参数）
 * @param[out] rid_batch 连接产生的下一批元组对应的RID（输出参数）
 * @param batch_size 批次中要包含的元组数量（默认值：BUSTUB_BATCH_SIZE）
 * @return 若产生了元组则返回true，若已无更多元组则返回false
 */
auto NestedIndexJoinExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                                   size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  if (is_exhausted_) return false;

  while (tuple_batch->size() < batch_size) {
    if (offset_ >= outer_batch_.size()) {
      std::vector<RID> rids;
      int ret = child_executor_->Next(&outer_batch_, &rids, batch_size);
      if (ret == 0) {
        is_exhausted_ = true;
        break;
      }
      offset_ = 0;
    }
    auto scan_tuple = outer_batch_[offset_];
    std::cout << "外部表元组为" << scan_tuple.ToString(&child_executor_->GetOutputSchema()) << "\n";
    // 1. 从计划节点获取键谓词，计算索引键的值
    const auto &key_predicate = plan_->KeyPredicate();
    Value key_value = key_predicate->Evaluate(&scan_tuple, child_executor_->GetOutputSchema());
    const Schema &index_key_schema = *index_->index_->GetKeySchema();
    std::vector<Value> key_values;
    key_values.push_back(key_value);
    Tuple scan_key(key_values, &index_key_schema);

    std::vector<RID> result;
    index_->index_->ScanKey(scan_key, &result, exec_ctx_->GetTransaction());
    if (result.size() == 0) {
      if (plan_->GetJoinType() == JoinType::LEFT) {
        // 如果是左连接，那么依旧要加入
        std::cout << "索引上无满足的\n";
        tuple_batch->push_back(make_tuple_Null(scan_tuple));
        rid_batch->push_back(RID{});
      }
    } else {
      for (const auto &rid : result) {
        auto [meta, inner_tuple] = inner_table_info_->table_->GetTuple(rid);
        std::cout << "内部表元组为" << inner_tuple.ToString(plan_->inner_table_schema_.get()) << "\n";
        if (meta.is_deleted_) continue;
        tuple_batch->push_back(make_tuple(scan_tuple, inner_tuple));
        rid_batch->push_back(RID{});
      }
    }
    offset_++;
  }
  // 若批次不为空，返回true；否则返回false
  return !tuple_batch->empty();
}

Tuple NestedIndexJoinExecutor::make_tuple_Null(const Tuple &scan_tuple) {
  std::vector<Value> values;
  // 添加外表列
  const auto &outer_schema = child_executor_->GetOutputSchema();
  for (size_t i = 0; i < outer_schema.GetColumns().size(); ++i) {
    values.push_back(scan_tuple.GetValue(&outer_schema, i));
  }
  // 内表列填充NULL
  const auto &inner_schema = plan_->InnerTableSchema();
  for (const auto &col : inner_schema.GetColumns()) {
    values.push_back(ValueFactory::GetNullValueByType(col.GetType()));
  }
  return Tuple(values, &GetOutputSchema());
}
Tuple NestedIndexJoinExecutor::make_tuple(const Tuple &scan_tuple, const Tuple &inner_tuple) {
  std::vector<Value> values;
  // 添加外表列
  const auto &outer_schema = child_executor_->GetOutputSchema();
  for (size_t i = 0; i < outer_schema.GetColumns().size(); ++i) {
    values.push_back(scan_tuple.GetValue(&outer_schema, i));
  }
  // 内表列填充NULL
  const auto &inner_schema = plan_->InnerTableSchema();
  for (size_t i = 0; i < inner_schema.GetColumns().size(); ++i) {
    values.push_back(inner_tuple.GetValue(&inner_schema, i));
  }
  return Tuple(values, &GetOutputSchema());
}

}  // namespace bustub