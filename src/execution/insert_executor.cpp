//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "common/macros.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"

#include "execution/executors/insert_executor.h"

namespace bustub {

/**
 * Construct a new InsertExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The insert plan to be executed
 * @param child_executor The child executor from which inserted tuples are pulled
 */
InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan_->GetTableOid())),
      indexes_(exec_ctx->GetCatalog()->GetTableIndexes(table_info_->name_)),
      insert_lines_(0),
      is_finished_(false) {
  // std::cout << "调用了新写的insertExcutor\n";
}

/** Initialize the insert */
void InsertExecutor::Init() {
  Catalog *catalog = exec_ctx_->GetCatalog();
  table_oid_t table_oid = plan_->GetTableOid();
  table_info_ = catalog->GetTable(table_oid);
  insert_lines_ = 0;
  is_finished_ = false;
  // 2. 获取表关联的所有索引（用于后续更新索引）

  // 3. 初始化子执行器
  child_executor_->Init();
}

/**
 * Yield the number of rows inserted into the table.
 * @param[out] tuple_batch The tuple batch with one integer indicating the number of rows inserted into the table
 * @param[out] rid_batch The next tuple RID batch produced by the insert (ignore, not used)
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: InsertExecutor::Next() does not use the `rid_batch` out-parameter.
 * NOTE: InsertExecutor::Next() returns true with the number of inserted rows produced only once.
 */
auto InsertExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                          size_t batch_size) -> bool {
  if (is_finished_) return false;
  std::vector<Tuple> child_tuples;
  std::vector<RID> child_rids;

  while (child_executor_->Next(&child_tuples, &child_rids, batch_size)) {
    for (const auto &tuple : child_tuples) {
      TupleMeta meta;
      meta.is_deleted_ = false;

      auto rid = table_info_->table_->InsertTuple(meta, tuple, exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(),
                                                  table_info_->oid_);
      if (rid.has_value()) {
        insert_lines_++;

        for (auto index : indexes_) {
          Tuple key = tuple.KeyFromTuple(table_info_->schema_, index->key_schema_,
                                         index->index_->GetKeyAttrs());  // 生成对应索引的键的形式的 元组
          index->index_->InsertEntry(key, rid.value(), exec_ctx_->GetTransaction());
        }
      }
    }
  }

  Schema result_schema = plan_->OutputSchema();
  std::vector<Value> values;
  values.emplace_back(TypeId::INTEGER, static_cast<int32_t>(insert_lines_));
  Tuple result_tuple(values, &result_schema);
  tuple_batch->push_back(result_tuple);
  is_finished_ = true;
  return true;
}

}  // namespace bustub
