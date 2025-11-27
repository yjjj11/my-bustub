//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "common/macros.h"

#include "execution/executors/delete_executor.h"

namespace bustub {

/**
 * Construct a new DeleteExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The delete plan to be executed
 * @param child_executor The child executor that feeds the delete
 */
DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())),
      indexes_(exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)),
      deleted_count_(0),
      is_finished_(false) {}

/** Initialize the delete */
void DeleteExecutor::Init() { child_executor_->Init(); }
/**
 * Yield the number of rows deleted from the table.
 * @param[out] tuple_batch The tuple batch with one integer indicating the number of rows deleted from the table
 * @param[out] rid_batch The next tuple RID batch produced by the delete (ignore, not used)
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: DeleteExecutor::Next() does not use the `rid_batch` out-parameter.
 * NOTE: DeleteExecutor::Next() returns true with the number of deleted rows produced only once.
 */
auto DeleteExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                          size_t batch_size) -> bool {
  if (is_finished_) {
    return false;
  }

  std::vector<Tuple> child_tuples;
  std::vector<RID> child_rids;

  while (child_executor_->Next(&child_tuples, &child_rids, batch_size)) {
    for (size_t i = 0; i < child_tuples.size(); ++i) {
      const Tuple &tuple = child_tuples[i];
      RID rid = child_rids[i];

      // 标记元组为“已删除”
      TupleMeta meta;
      meta.is_deleted_ = true;
      table_info_->table_->UpdateTupleMeta(meta, rid);
      deleted_count_++;

      // 更新所有关联的索引
      for (auto index : indexes_) {
        Tuple key = tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
        index->index_->DeleteEntry(key, rid, exec_ctx_->GetTransaction());
      }
    }
  }

  // 构造返回结果：包含删除行数的元组
  Schema result_schema = plan_->OutputSchema();
  std::vector<Value> values;
  values.emplace_back(TypeId::INTEGER, static_cast<int32_t>(deleted_count_));
  Tuple result_tuple(values, &result_schema);
  tuple_batch->push_back(result_tuple);

  is_finished_ = true;
  return true;
}
}  // namespace bustub
