//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// execution_common.h
//
// 标识：src/include/execution/execution_common.h
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库组
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <utility>
#include <vector>

#include "binder/bound_order_by.h"
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "concurrency/transaction.h"
#include "storage/table/tuple.h"

namespace bustub {

/** SortKey 定义排序所基于的一组值 */
using SortKey = std::vector<Value>;
/** SortEntry 定义用于排序元组及对应RID的键值对 */
using SortEntry = std::pair<SortKey, Tuple>;

/** TupleComparator 为SortEntry提供比较函数 */
class TupleComparator {
 public:
  explicit TupleComparator(std::vector<OrderBy> order_bys);

  /** 比较两个SortEntry的大小 */
  auto operator()(const SortEntry &entry_a, const SortEntry &entry_b) const -> bool;

 private:
  std::vector<OrderBy> order_bys_;  // 排序规则列表
};

/**
 * 为元组生成排序键
 * @param tuple 要生成排序键的元组
 * @param order_bys 排序规则
 * @param schema 元组对应的模式
 * @return 生成的排序键
 */
auto GenerateSortKey(const Tuple &tuple, const std::vector<OrderBy> &order_bys, const Schema &schema) -> SortKey;

/**
 * 以上是P3（第三阶段）所需的全部内容。
 * 在进行P4（第四阶段）之前，您可以忽略本文件的剩余部分。
 */

/**
 * 根据撤销日志重建元组
 * @param schema 元组模式
 * @param base_tuple 基础元组
 * @param base_meta 基础元组的元数据
 * @param undo_logs 撤销日志列表
 * @return 重建后的元组（若失败则返回空）
 */
auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple>;

/**
 * 收集指定RID的撤销日志
 * @param rid 记录ID
 * @param base_meta 基础元组的元数据
 * @param base_tuple 基础元组
 * @param undo_link 撤销链表
 * @param txn 事务指针
 * @param txn_mgr 事务管理器指针
 * @return 收集到的撤销日志列表（若失败则返回空）
 */
auto CollectUndoLogs(RID rid, const TupleMeta &base_meta, const Tuple &base_tuple, std::optional<UndoLink> undo_link,
                     Transaction *txn, TransactionManager *txn_mgr) -> std::optional<std::vector<UndoLog>>;

/**
 * 生成新的撤销日志
 * @param schema 元组模式
 * @param base_tuple 基础元组
 * @param target_tuple 目标元组
 * @param ts 时间戳
 * @param prev_version 上一版本的撤销链表
 * @return 生成的撤销日志
 */
auto GenerateNewUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple, timestamp_t ts,
                        UndoLink prev_version) -> UndoLog;

/**
 * 生成更新后的撤销日志
 * @param schema 元组模式
 * @param base_tuple 基础元组
 * @param target_tuple 目标元组
 * @param log 原撤销日志
 * @return 更新后的撤销日志
 */
auto GenerateUpdatedUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple,
                            const UndoLog &log) -> UndoLog;

/**
 * 事务管理器调试函数
 * @param info 调试信息
 * @param txn_mgr 事务管理器指针
 * @param table_info 表信息
 * @param table_heap 表堆
 */
void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap);

// TODO(P4)：根据需要添加新函数...您可能需要定义更多函数。
//
// 为了让您了解哪些功能可以在执行器/事务管理器之间共享，以下是我们在参考解决方案中定义的辅助函数名称。
// 您在实现过程中可以自行设计。
// * WalkUndoLogs（遍历撤销日志）
// * Modify（修改操作）
// * IsWriteWriteConflict（检查写写冲突）
// * GenerateNullTupleForSchema（为模式生成空元组）
// * GetUndoLogSchema（获取撤销日志模式）
//
// 我们不提供这些函数的签名，因为这取决于您系统其他部分的实现。
// 您不必在实现中定义完全相同的辅助函数集。请根据需要添加自己的函数，
// 以避免在各处编写重复代码。

}  // namespace bustub