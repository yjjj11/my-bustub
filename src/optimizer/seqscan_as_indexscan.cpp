//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seqscan_as_indexscan.cpp
//
// Identification: src/optimizer/seqscan_as_indexscan.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

// 优化器类的私有辅助函数：递归提取同一列的等值常量
// 参数：表达式、目标列索引（首次调用为-1，后续校验是否一致）、存储常量的vector
// 返回值：是否解析成功（所有OR子节点都是同一列的等值条件）
bool ExtractEqConstantsFromOr(const AbstractExpressionRef &expr, int32_t &target_col_idx,
                              std::vector<AbstractExpressionRef> &pred_keys) {
  // 情况1：表达式是等值比较表达式 → 提取常量和列索引
  if (const auto *comp_expr = dynamic_cast<const ComparisonExpression *>(expr.get())) {
    if (comp_expr->comp_type_ != ComparisonType::Equal) {
      return false;  // 非等值条件，不满足规则
    }

    // std::cout << "是=\n";
    // 区分列=常量 或 常量=列
    const ColumnValueExpression *col_expr = nullptr;
    const ConstantValueExpression *const_expr = nullptr;
    if (auto *lhs_col = dynamic_cast<const ColumnValueExpression *>(comp_expr->children_[0].get())) {
      col_expr = lhs_col;
      const_expr = dynamic_cast<const ConstantValueExpression *>(comp_expr->children_[1].get());
    } else if (auto *rhs_col = dynamic_cast<const ColumnValueExpression *>(comp_expr->children_[1].get())) {
      col_expr = rhs_col;
      const_expr = dynamic_cast<const ConstantValueExpression *>(comp_expr->children_[0].get());
    }

    // 校验列和常量是否有效
    if (col_expr == nullptr || const_expr == nullptr) {
      return false;
    }
    // std::cout << "列号=" << col_expr->GetColIdx() << "\n";
    // 首次提取：记录目标列索引
    if (target_col_idx == -1) {
      target_col_idx = col_expr->GetColIdx();
    } else if (target_col_idx != (int)col_expr->GetColIdx()) {
      // std::cout << "键不同，不走索引\n";
      return false;  // 不是同一列的等值条件，不满足规则
    }
    // 构造常量表达式并加入列表
    pred_keys.emplace_back(std::make_shared<ConstantValueExpression>(const_expr->val_));
    return true;
  }

  // 情况2：表达式是逻辑或表达式 → 递归解析所有子节点
  if (const auto *logical_or = dynamic_cast<const LogicExpression *>(expr.get())) {
    if (logical_or->logic_type_ != LogicType::Or) {
      return false;  // 非OR逻辑，不满足规则
    }
    // std::cout << "是or\n";
    // 递归解析每个子表达式
    for (const auto &child : logical_or->children_) {
      if (!ExtractEqConstantsFromOr(child, target_col_idx, pred_keys)) {
        return false;
      }
    }
    return true;
  }

  // 情况3：其他表达式（如AND、>、<等）→ 不满足规则
  return false;
}
/**
 * @brief Optimizes seq scan as index scan if there's an index on a table
 */
auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(P3): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  // std::cout << "进入OptimizeSeqScanAsIndexScan\n";
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }
  // std::cout << "递归子节点\n";
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() != PlanType::Filter) {
    return optimized_plan;
  }
  std::cout << " 当前是过滤节点\n";
  const auto &filte_plan = dynamic_cast<const FilterPlanNode &>(*optimized_plan);
  BUSTUB_ASSERT(optimized_plan->children_.size() == 1, "must have exactly one children");
  const auto &child_plan = *optimized_plan->children_[0];
  if (child_plan.GetType() != PlanType::SeqScan) {
    return optimized_plan;
  }

  std::cout << "儿是扫描节点\n";
  const auto &seq_scan = dynamic_cast<const SeqScanPlanNode &>(child_plan);
  const auto &predicate = filte_plan.GetPredicate();

  int32_t target_col_idx = -1;  // 目标索引列（首次为-1，由递归函数初始化）
  std::vector<AbstractExpressionRef> pred_keys;
  bool is_valid_or_eq = ExtractEqConstantsFromOr(predicate, target_col_idx, pred_keys);

  // 5. 若提取失败（非OR等值条件），直接返回原计划
  if (!is_valid_or_eq || pred_keys.empty()) {
    return optimized_plan;
  }

  auto table_info = catalog_.GetTable(seq_scan.GetTableOid());
  auto index_list = catalog_.GetTableIndexes(table_info->name_);
  std::cout << "扫描所有已经存在的索引\n";
  for (const auto &index : index_list) {
    const auto &key_attrs = index->index_->GetKeyAttrs();
    // 仅处理单键索引（若支持多键可扩展逻辑）
    if (key_attrs.size() != 1) {
      continue;
    }
    // 匹配索引键列
    if ((int)key_attrs[0] == target_col_idx) {
      return std::make_shared<IndexScanPlanNode>(optimized_plan->output_schema_, seq_scan.table_oid_, index->index_oid_,
                                                 predicate, std::move(pred_keys));
    }
  }
  return optimized_plan;
}  // namespace bustub
}  // namespace bustub
