//===----------------------------------------------------------------------===//
//
//                         BusTub 数据库
//
// nested_index_join_plan.h
//
// 标识：src/include/execution/plans/nested_index_join_plan.h
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库研究组
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <utility>
#include <vector>

#include "binder/table_ref/bound_join_ref.h"
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "concurrency/transaction.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/abstract_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * NestedIndexJoinPlanNode 用于表示两张表之间的基于索引的嵌套连接执行计划。
 * 外表元组通过子执行计划节点（child executor）生成，
 * 内表元组则通过外表元组结合元数据目录（catalog）中的索引获取。
 */
class NestedIndexJoinPlanNode : public AbstractPlanNode {
 public:
  /**
   * 构造基于索引的嵌套连接计划节点
   * @param output 连接操作的输出模式（Schema）
   * @param child 外表对应的子计划节点
   * @param key_predicate 用于从外表元组中提取连接键的谓词表达式
   * @param inner_table_oid 内表的表ID（OID）
   * @param index_oid 用于连接的索引ID（OID）
   * @param index_name 用于连接的索引名称
   * @param index_table_name 索引所属的表名称
   * @param inner_table_schema 内表的模式（Schema）
   * @param join_type 连接类型（如内连接、左连接）
   */
  NestedIndexJoinPlanNode(SchemaRef output, AbstractPlanNodeRef child, AbstractExpressionRef key_predicate,
                          table_oid_t inner_table_oid, index_oid_t index_oid, std::string index_name,
                          std::string index_table_name, SchemaRef inner_table_schema, JoinType join_type)
      : AbstractPlanNode(std::move(output), {std::move(child)}),
        key_predicate_(std::move(key_predicate)),
        inner_table_oid_(inner_table_oid),
        index_oid_(index_oid),
        index_name_(std::move(index_name)),
        index_table_name_(std::move(index_table_name)),
        inner_table_schema_(std::move(inner_table_schema)),
        join_type_(join_type) {}

  /** @return 计划节点的类型（基于索引的嵌套连接） */
  auto GetType() const -> PlanType override { return PlanType::NestedIndexJoin; }

  /** @return 用于从外表元组中提取连接键的谓词表达式 */
  auto KeyPredicate() const -> const AbstractExpressionRef & { return key_predicate_; }

  /** @return 基于索引的嵌套连接所使用的连接类型 */
  auto GetJoinType() const -> JoinType { return join_type_; };

  /** @return 基于索引的嵌套连接中外表对应的子计划节点 */
  auto GetChildPlan() const -> AbstractPlanNodeRef { return GetChildAt(0); }

  /** @return 基于索引的嵌套连接中内表的表ID（OID） */
  auto GetInnerTableOid() const -> table_oid_t { return inner_table_oid_; }

  /** @return 用于基于索引的嵌套连接的索引名称 */
  auto GetIndexName() const -> std::string { return index_name_; }

  /** @return 用于基于索引的嵌套连接的索引ID（OID） */
  auto GetIndexOid() const -> index_oid_t { return index_oid_; }

  /** @return 内表中参与连接所需列对应的模式（Schema） */
  auto InnerTableSchema() const -> const Schema & { return *inner_table_schema_; }

  /** 克隆计划节点（含子计划节点）的宏定义 */
  BUSTUB_PLAN_NODE_CLONE_WITH_CHILDREN(NestedIndexJoinPlanNode);

  /** 基于索引的嵌套连接的键谓词表达式（用于提取连接键） */
  AbstractExpressionRef key_predicate_;
  /** 内表的表ID（OID） */
  table_oid_t inner_table_oid_;
  /** 连接所用索引的ID（OID） */
  index_oid_t index_oid_;
  /** 连接所用索引的名称 */
  const std::string index_name_;
  /** 索引所属表的名称 */
  const std::string index_table_name_;
  /** 内表的模式（Schema） */
  SchemaRef inner_table_schema_;

  /** 连接类型 */
  JoinType join_type_;

 protected:
  /** @return 计划节点的字符串描述（用于调试/日志） */
  auto PlanNodeToString() const -> std::string override {
    return fmt::format("NestedIndexJoin {{ 连接类型={}, 键谓词={}, 索引={}, 索引所属表={} }}", join_type_,
                       key_predicate_, index_name_, index_table_name_);
  }
};
}  // namespace bustub