//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nlj_as_hash_join.cpp
//
// Identification: src/optimizer/nlj_as_hash_join.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

bool ExtractJoinKeys(const AbstractExpressionRef &expr, std::vector<AbstractExpressionRef> &left_keys,
                     std::vector<AbstractExpressionRef> &right_keys) {
  // 处理AND逻辑：递归解析每个子条件
  if (const auto *logic_expr = dynamic_cast<const LogicExpression *>(expr.get())) {
    if (logic_expr->logic_type_ != LogicType::And) {
      return false;  // 仅支持AND组合的等值条件
    }
    for (const auto &child : logic_expr->children_) {
      if (!ExtractJoinKeys(child, left_keys, right_keys)) {
        return false;
      }
    }
    return true;
  }

  if (const auto *comp_expr = dynamic_cast<const ComparisonExpression *>(expr.get())) {
    if (comp_expr->comp_type_ != ComparisonType::Equal) {
      return false;  // 仅支持等值连接
    }

    // 提取左右两边的列表达式
    const auto *lhs = dynamic_cast<const ColumnValueExpression *>(comp_expr->children_[0].get());
    const auto *rhs = dynamic_cast<const ColumnValueExpression *>(comp_expr->children_[1].get());
    if (lhs == nullptr || rhs == nullptr) {
      return false;  // 连接条件必须是列与列的比较
    }

    // 确定左表和右表的键（根据tuple_idx区分，0为左表，1为右表）
    if (lhs->GetTupleIdx() == 0 && rhs->GetTupleIdx() == 1) {
      // 左表列 = 右表列（正常顺序）
      left_keys.push_back(comp_expr->children_[0]);
      right_keys.push_back(comp_expr->children_[1]);
    } else if (lhs->GetTupleIdx() == 1 && rhs->GetTupleIdx() == 0) {
      // 右表列 = 左表列（反转顺序）
      left_keys.push_back(comp_expr->children_[1]);
      right_keys.push_back(comp_expr->children_[0]);
    } else {
      return false;
    }
    return true;
  }

  return false;
}

/**
 * @brief 将嵌套循环连接优化为哈希连接
 */
auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // std::cout<<"确实走了OptimizeNLJAsHashJoin\n";
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() != PlanType::NestedLoopJoin) {
    return optimized_plan;
  }

  const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
  BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ must have exactly 2 children");

  std::vector<AbstractExpressionRef> left_keys;
  std::vector<AbstractExpressionRef> right_keys;
  if (!ExtractJoinKeys(nlj_plan.Predicate(), left_keys, right_keys) || left_keys.empty()) {
    return optimized_plan;
  }

  // std::cout<<"确认执行OptimizeNLJAsHashJoin\n";
  return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(),
                                            left_keys, right_keys, nlj_plan.GetJoinType());
}

}  // namespace bustub
