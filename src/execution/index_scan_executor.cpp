//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/index_scan_executor.h"
#include <memory>
#include "common/macros.h"
#include "storage/index/b_plus_tree_index.h"

namespace bustub {

/**
 * Creates a new index scan executor.
 * @param exec_ctx the executor context
 * @param plan the index scan plan to be executed
 */
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan_->table_oid_)),
      tree_(dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(
          exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->index_.get())),
      is_point_lookup_(false),
      iter_(tree_->GetBeginIterator()),
      offset_(0) {
  std::cout << "采用了indexScan\n";
}

void IndexScanExecutor::Init() {
  std::cout << "indexscan init\n";
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());

  const Schema *index_key_schema = &index_info->key_schema_;
  std::cout << "到这里为止都正常\n";
  if (plan_->filter_predicate_ != nullptr) {
    is_point_lookup_ = true;
    auto &pred_keys = plan_->pred_keys_;  // 过滤条件中的键值表达式列表
    std::cout << "pred_keys.size()=" << pred_keys.size() << "\n";
    // 2.1 构造符合索引键结构的Tuple
    std::vector<Value> key_values;  // 存储索引键的各个字段值
    for (size_t i = 0; i < pred_keys.size(); ++i) {
      // 解析表达式得到具体值（val），这里以整数为例
      key_values.clear();
      Value val = pred_keys[i]->Evaluate(nullptr, index_info->key_schema_);  // 使用索引schema解析
      key_values.push_back(val);
      std::cout << "这是第" << i << "次提取\n";
      Tuple key_tuple(key_values, index_key_schema);  // 用值和schema构造Tuple
      std::cout << "成功构造tuple\n";
      scan_key_.push_back(std::move(key_tuple));
    }
    std::cout << "一共有" << scan_key_.size() << "个=\n";
    // 在 Init 方法中添加打印逻辑
    std::cout << "scan_key_ 中的内容：" << std::endl;
    for (const auto &key_tuple : scan_key_) {
      std::cout << key_tuple.ToString(&index_info->key_schema_) << std::endl;
    }
    // 2.2 调用KeyFromTuple生成索引内部使用的查询键
  } else {
    // 范围扫描：初始化迭代器
    is_point_lookup_ = false;
  }
}
auto IndexScanExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                             size_t batch_size) -> bool {
  std::cout << batch_size << "\n";
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  tuple_batch->clear();
  rid_batch->clear();
  size_t count = 0;
  std::cout << "调用了next\n";
  if (is_point_lookup_) {
    std::cout << "这是一个点查询\n";
    // 点查询，只返回一个元组
    std::vector<bustub::RID> tmp_batch;
    while (offset_ < scan_key_.size() && count < batch_size) {
      std::cout << "进入了循环\n";

      std::cout << scan_key_[offset_].ToString(&index_info->key_schema_) << std::endl;
      tree_->ScanKey(scan_key_[offset_], &tmp_batch, exec_ctx_->GetTransaction());
      std::cout << "tmp_batch的大小为" << tmp_batch.size() << "\n";
      if (tmp_batch.size() == 0) {
        offset_++;
        continue;
      }

      auto [meta, tuple] = table_info_->table_->GetTuple(tmp_batch[0]);
      tuple_batch->push_back(tuple);
      std::cout << "查询到的Tuple: " << tuple.ToString(&table_info_->schema_) << std::endl;
      rid_batch->push_back(tmp_batch[0]);
      count++;
      offset_++;
      std::cout << "offset_=" << offset_ << "\n";
      std::cout << "tuple_batch.size()=" << tuple_batch->size() << "\n";
      tmp_batch.clear();
    }
    // if (offset_ == scan_key_.size()) return false;
    if (tuple_batch->size() == 0) return false;
    return true;
  } else {
    std::cout << "这是一个order by查询\n";
    std::cout << "iter_.IsEnd()=" << iter_.IsEnd() << "\n";
    // 排序查询，只处理从小到大的，所以只要能够遍历索引就可以了
    while (!iter_.IsEnd() && tuple_batch->size() < batch_size) {
      auto rid = (*iter_).second;
      auto [meta, tuple] = table_info_->table_->GetTuple(rid);
      tuple.ToString(&exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_)->key_schema_);
      tuple_batch->push_back(tuple);
      rid_batch->push_back(rid);
      count++;
      ++iter_;
    }
    std::cout << "这次next一共有=" << count << "个\n";
    if (count == 0) return false;
    return true;
  }
}
const Schema &IndexScanExecutor::GetOutputSchema() const {
  // 根据实际情况返回对应的Schema
  return exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->key_schema_;
}
}  // namespace bustub
