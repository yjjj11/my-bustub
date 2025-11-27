//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "catalog/catalog.h"
#include "common/macros.h"

namespace bustub {

/**
 * Construct a new SeqScanExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The sequential scan plan to be executed
 */
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())),  // 提前获取表信息
      table_iter_(nullptr)                                                 // 直接在初始化列表中构造迭代器
{
  std::cout << "确认使用了我的seqscna\n";
}

void SeqScanExecutor::Init() {
  // 重置迭代器（若需要重新扫描，直接重新构造，而非赋值）
  table_iter_ = std::make_unique<TableIterator>(table_info_->table_->MakeIterator());
}

/**
 * Yield the next tuple batch from the seq scan.
 * @param[out] tuple_batch The next tuple batch produced by the scan
 * @param[out] rid_batch The next tuple RID batch produced by the scan
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto SeqScanExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                           size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();
  // if (!table_iter_) {
  //   Init();
  // }
  size_t count = 0;
  while (count < batch_size && !table_iter_->IsEnd()) {
    auto [meta, tuple] = table_iter_->GetTuple();
    RID rid = table_iter_->GetRID();
    ++(*table_iter_);  // 先移动迭代器，避免重复读取

    if (meta.is_deleted_) {
      continue;
    }

    // 应用过滤条件
    const AbstractExpressionRef &predicate = plan_->filter_predicate_;
    bool pass = true;
    if (predicate != nullptr) {
      Value eval = predicate->Evaluate(&tuple, table_info_->schema_);
      pass = !eval.IsNull() && eval.GetAs<bool>();
    }

    if (pass) {
      tuple_batch->push_back(tuple);
      rid_batch->push_back(rid);
      count++;
    }
  }
  // std::cout << "SeqScan Next: 返回" << tuple_batch->size() << "个元组\n";  // 新增日志
  return !tuple_batch->empty();
}
}  // namespace bustub
