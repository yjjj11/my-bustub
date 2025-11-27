//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "common/macros.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"

#include "execution/executors/update_executor.h"

namespace bustub {

/**
 * Construct a new UpdateExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The update plan to be executed
 * @param child_executor The child executor that feeds the update
 */
UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan_->GetTableOid())),
      indexes_(exec_ctx->GetCatalog()->GetTableIndexes(table_info_->name_)),
      is_finished_(false) {
  std::cout << "使用了我的updateexcutor\n";
}

/** Initialize the update */
void UpdateExecutor::Init() {
  is_finished_ = false;
  child_executor_->Init();
}
/**
 * Yield the number of rows updated in the table.
 * @param[out] tuple_batch The tuple batch with one integer indicating the number of rows updated in the table
 * @param[out] rid_batch The next tuple RID batch produced by the update (ignore, not used)
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: UpdateExecutor::Next() does not use the `rid_batch` out-parameter.
 * NOTE: UpdateExecutor::Next() returns true with the number of updated rows produced only once.
 */
auto UpdateExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                          size_t batch_size) -> bool {
  if (is_finished_) return false;
  size_t insert_count = 0;
  std::vector<Tuple> child_tuples;
  std::vector<RID> child_rids;

  while (child_executor_->Next(&child_tuples, &child_rids, batch_size)) {
    for (size_t i = 0; i < child_tuples.size(); i++) {
      auto old_tuple = child_tuples[i];
      auto old_rid = child_rids[i];
      // 预备删除旧数据
      TupleMeta new_meta;
      new_meta.is_deleted_ = true;
      table_info_->table_->UpdateTupleMeta(new_meta, old_rid);

      std::vector<Value> new_value;
      for (const auto &expr : plan_->target_expressions_) {
        new_value.push_back(expr->Evaluate(&old_tuple, table_info_->schema_));  // 将表达式计算的值转化为该层模式的形式
      }
      Tuple new_tuple(new_value, &table_info_->schema_);

      new_meta.is_deleted_ = false;
      auto new_rid = table_info_->table_->InsertTuple(new_meta, new_tuple, exec_ctx_->GetLockManager(),
                                                      exec_ctx_->GetTransaction(), table_info_->oid_);
      size_t count = 0;
      if (new_rid.has_value()) {
        insert_count++;
        for (auto &index : indexes_) {
          count++;
          std::cout << "delete the" << count << " index\n";
          // 对该表的所有索引进行修改，先删除原元组
          Tuple old_key =
              old_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
          index->index_->DeleteEntry(old_key, old_rid, exec_ctx_->GetTransaction());  // 删除
                                                                                      //
          Tuple new_key =
              new_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
          index->index_->InsertEntry(new_key, new_rid.value(), exec_ctx_->GetTransaction());  // 删i除
        }
      }
    }
  }
  std::cout << "Successfully updated\n";
  Schema result_schema = plan_->OutputSchema();
  std::vector<Value> values;
  values.emplace_back(TypeId::INTEGER, static_cast<int32_t>(insert_count));
  Tuple result_tuple(values, &result_schema);
  tuple_batch->push_back(result_tuple);

  is_finished_ = true;
  return true;

}  // namespace bustub
}  // namespace bustub
