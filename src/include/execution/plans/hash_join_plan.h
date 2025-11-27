//===----------------------------------------------------------------------===//
//
//                         BusTub 数据库
//
// hash_join_plan.h
//
// 标识：src/include/execution/plans/hash_join_plan.h
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库研究组
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <utility>
#include <vector>

#include "binder/table_ref/bound_join_ref.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/abstract_plan.h"

namespace bustub {

/**
 * HashJoin（哈希连接）通过哈希表执行连接操作。
 */
class HashJoinPlanNode : public AbstractPlanNode {
 public:
  /**
   * 构造 HashJoinPlanNode 实例。
   * @param output_schema 连接操作的输出模式
   * @param children 用于获取元组的子计划节点
   * @param left_key_expressions 左侧连接键的表达式
   * @param right_key_expressions 右侧连接键的表达式
   */
  HashJoinPlanNode(SchemaRef output_schema, AbstractPlanNodeRef left, AbstractPlanNodeRef right,
                   std::vector<AbstractExpressionRef> left_key_expressions,
                   std::vector<AbstractExpressionRef> right_key_expressions, JoinType join_type)
      : AbstractPlanNode(std::move(output_schema), {std::move(left), std::move(right)}),
        left_key_expressions_{std::move(left_key_expressions)},
        right_key_expressions_{std::move(right_key_expressions)},
        join_type_(join_type) {}

  /** @return 计划节点的类型 */
  auto GetType() const -> PlanType override { return PlanType::HashJoin; }

  /** @return 计算左侧连接键的表达式 */
  auto LeftJoinKeyExpressions() const -> const std::vector<AbstractExpressionRef> & { return left_key_expressions_; }

  /** @return 计算右侧连接键的表达式 */
  auto RightJoinKeyExpressions() const -> const std::vector<AbstractExpressionRef> & { return right_key_expressions_; }

  /** @return 哈希连接的左子计划节点 */
  auto GetLeftPlan() const -> AbstractPlanNodeRef {
    BUSTUB_ASSERT(GetChildren().size() == 2, "哈希连接应包含恰好两个子计划节点");
    return GetChildAt(0);
  }

  /** @return 哈希连接的右子计划节点 */
  auto GetRightPlan() const -> AbstractPlanNodeRef {
    BUSTUB_ASSERT(GetChildren().size() == 2, "哈希连接应包含恰好两个子计划节点");
    return GetChildAt(1);
  }

  /** @return 哈希连接使用的连接类型 */
  auto GetJoinType() const -> JoinType { return join_type_; };

  BUSTUB_PLAN_NODE_CLONE_WITH_CHILDREN(HashJoinPlanNode);

  /** 计算左侧连接键的表达式 */
  std::vector<AbstractExpressionRef> left_key_expressions_;
  /** 计算右侧连接键的表达式 */
  std::vector<AbstractExpressionRef> right_key_expressions_;

  /** 连接类型 */
  JoinType join_type_;

 protected:
  /** @return 计划节点的字符串描述 */
  auto PlanNodeToString() const -> std::string override;
};

}  // namespace bustub