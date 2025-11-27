//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// execution_common.cpp
//
// Identification: src/execution/execution_common.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/execution_common.h"

#include "catalog/catalog.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"

namespace bustub {

TupleComparator::TupleComparator(std::vector<OrderBy> order_bys) : order_bys_(std::move(order_bys)) {}

/** TODO(P3): Implement the comparison method */
auto TupleComparator::operator()(const SortEntry &entry_a, const SortEntry &entry_b) const -> bool {
  const auto &key_a = entry_a.first;  // 排序键 a（std::vector<Value>）
  const auto &key_b = entry_b.first;  // 排序键 b（std::vector<Value>）

  // 排序键数量必须与 ORDER BY 规则数量一致
  BUSTUB_ASSERT(key_a.size() == order_bys_.size() && key_b.size() == order_bys_.size(),
                "Sort key size mismatch with order by expressions");

  // 按 ORDER BY 规则依次比较
  for (size_t i = 0; i < order_bys_.size(); ++i) {
    const auto &order_by = order_bys_[i];
    const auto &[sort_type, null_type, expr] = order_by;  // 解析排序规则
    const Value &val_a = key_a[i];
    const Value &val_b = key_b[i];

    OrderByNullType effective_null_type = null_type;
    if (null_type == OrderByNullType::DEFAULT) {
      if ((sort_type == OrderByType::ASC) || (sort_type == OrderByType::DEFAULT))
        effective_null_type = OrderByNullType::NULLS_FIRST;
      else
        effective_null_type = OrderByNullType::NULLS_LAST;
    }

    bool a_is_null = val_a.IsNull();
    bool b_is_null = val_b.IsNull();
    if (a_is_null || b_is_null) {
      if (a_is_null && b_is_null) {
        continue;  // 两者都是 NULL，继续比较下一个排序键
      }
      if (effective_null_type == OrderByNullType::NULLS_FIRST) {
        return a_is_null;
      } else {  // NULLS_LAST
        return b_is_null;
      }
    }

    bool is_asc = (sort_type == OrderByType::ASC) || (sort_type == OrderByType::DEFAULT);

    // 比较两个非空值
    CmpBool cmp_less = val_a.CompareLessThan(val_b);        // a < b ?
    CmpBool cmp_greater = val_a.CompareGreaterThan(val_b);  // a > b ?

    if (cmp_less == CmpBool::CmpTrue) {
      // a < b 时：升序则 a 在前（true），降序则 a 在后（false）
      return is_asc;
    } else if (cmp_greater == CmpBool::CmpTrue) {
      // a > b 时：升序则 a 在后（false），降序则 a 在前（true）
      return !is_asc;
    }

    // 若值相等，继续比较下一个排序键
    continue;
  }

  // 所有排序键都相等，保持原顺序（排序稳定性）
  return false;
}

/**
 * Generate sort key for a tuple based on the order by expressions.
 *
 * TODO(P3): Implement this method.
 */
auto GenerateSortKey(const Tuple &tuple, const std::vector<OrderBy> &order_bys, const Schema &schema) -> SortKey {
  SortKey sort_key;
  sort_key.reserve(order_bys.size());

  for (const auto &order_by : order_bys) {
    const auto &[sort_type, null_type, expr] = order_by;
    Value val = expr->Evaluate(&tuple, schema);
    sort_key.push_back(std::move(val));
  }

  return sort_key;
}

/**
 * Above are all you need for P3.
 * You can ignore the remaining part of this file until P4.
 */

/**
 * @brief Reconstruct a tuple by applying the provided undo logs from the base tuple. All logs in the undo_logs are
 * applied regardless of the timestamp
 *
 * @param schema The schema of the base tuple and the returned tuple.
 * @param base_tuple The base tuple to start the reconstruction from.
 * @param base_meta The metadata of the base tuple.
 * @param undo_logs The list of undo logs to apply during the reconstruction, the front is applied first.
 * @return An optional tuple that represents the reconstructed tuple. If the tuple is deleted as the result, returns
 * std::nullopt.
 */
auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  UNIMPLEMENTED("not implemented");
}

/**
 * @brief Collects the undo logs sufficient to reconstruct the tuple w.r.t. the txn.
 *
 * @param rid The RID of the tuple.
 * @param base_meta The metadata of the base tuple.
 * @param base_tuple The base tuple.
 * @param undo_link The undo link to the latest undo log.
 * @param txn The transaction.
 * @param txn_mgr The transaction manager.
 * @return An optional vector of undo logs to pass to ReconstructTuple(). std::nullopt if the tuple did not exist at the
 * time.
 */
auto CollectUndoLogs(RID rid, const TupleMeta &base_meta, const Tuple &base_tuple, std::optional<UndoLink> undo_link,
                     Transaction *txn, TransactionManager *txn_mgr) -> std::optional<std::vector<UndoLog>> {
  UNIMPLEMENTED("not implemented");
}

/**
 * @brief Generates a new undo log as the transaction tries to modify this tuple at the first time.
 *
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from the table heap. nullptr if the tuple is
 * deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a deletion.
 * @param ts The timestamp of the base tuple.
 * @param prev_version The undo link to the latest undo log of this tuple.
 * @return The generated undo log.
 */
auto GenerateNewUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple, timestamp_t ts,
                        UndoLink prev_version) -> UndoLog {
  UNIMPLEMENTED("not implemented");
}

/**
 * @brief Generate the updated undo log to replace the old one, whereas the tuple is already modified by this txn once.
 *
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from the table heap. nullptr if the tuple is
 * deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a deletion.
 * @param log The original undo log.
 * @return The updated undo log.
 */
auto GenerateUpdatedUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple,
                            const UndoLog &log) -> UndoLog {
  UNIMPLEMENTED("not implemented");
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);

  fmt::println(
      stderr,
      "You see this line of text because you have not implemented `TxnMgrDbg`. You should do this once you have "
      "finished task 2. Implementing this helper function will save you a lot of time for debugging in later tasks.");

  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}

}  // namespace bustub
