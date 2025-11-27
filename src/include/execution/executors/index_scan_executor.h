#pragma once

#include <vector>
#include "catalog/catalog.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/index_scan_plan.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/index/index_iterator.h"
#include "storage/table/tuple.h"
namespace bustub {

/**
 * IndexScanExecutor 用于通过索引扫描获取表中的元组，支持点查询和范围扫描
 */
class IndexScanExecutor : public AbstractExecutor {
 public:
  /**
   * 构造函数：初始化索引扫描执行器
   * @param exec_ctx 执行器上下文
   * @param plan 索引扫描计划节点，包含索引信息、过滤条件等
   */
  IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan);

  /**
   * 初始化扫描：获取索引对象，根据过滤条件初始化点查询或范围扫描的迭代器
   */
  void Init() override;

  /**
   * 批量获取下一组符合条件的元组和对应的RID
   * @param tuple_batch 输出参数，存储获取的元组
   * @param rid_batch 输出参数，存储元组对应的RID
   * @param batch_size 最大批量大小
   * @return 若成功获取元组则返回true，否则返回false
   */
  auto Next(std::vector<Tuple> *tuple_batch, std::vector<RID> *rid_batch, size_t batch_size) -> bool override;

  const Schema &GetOutputSchema() const override;

 private:
  /** 索引扫描计划节点，存储扫描相关的元信息 */
  const IndexScanPlanNode *plan_;

  std::shared_ptr<TableInfo> table_info_;
  /** B+树索引对象（假设索引键为两个整数列，根据项目实际类型调整） */
  BPlusTreeIndexForTwoIntegerColumn *tree_;

  /** 标记当前是否为点查询（true为点查询，false为范围扫描） */
  bool is_point_lookup_;

  std::vector<Tuple> scan_key_;

  BPlusTreeIndexIteratorForTwoIntegerColumn iter_;

  size_t offset_;
};

}  // namespace bustub
