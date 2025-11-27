//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_plan.h
//
// 标识：src/include/execution/plans/seq_scan_plan.h
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库组
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <utility>

#include "binder/table_ref/bound_base_table_ref.h"
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/abstract_plan.h"

namespace bustub {

/**
 * SeqScanPlanNode 代表一个顺序表扫描操作。
 * （顺序扫描：从头到尾逐行读取表中所有数据的操作，是最基础的表扫描方式）
 */
class SeqScanPlanNode : public AbstractPlanNode {
 public:
  /**
   * 构造一个新的 SeqScanPlanNode 实例。
   * @param output 此顺序扫描计划节点的输出 schema（描述输出数据的结构，如列名、类型等）
   * @param table_oid 要扫描的表的唯一标识符（OID：对象标识符，用于在系统中唯一标识表）
   * @param table_name 表的名称（用于显示或调试）
   * @param filter_predicate 扫描时的过滤条件（可选，默认为空）
   */
  SeqScanPlanNode(SchemaRef output, table_oid_t table_oid, std::string table_name,
                  AbstractExpressionRef filter_predicate = nullptr)
      : AbstractPlanNode(std::move(output), {}),
        table_oid_{table_oid},
        table_name_(std::move(table_name)),
        filter_predicate_(std::move(filter_predicate)) {}

  /** @return 计划节点的类型（此处返回顺序扫描类型） */
  auto GetType() const -> PlanType override { return PlanType::SeqScan; }

  /** @return 要扫描的表的 OID（唯一标识符） */
  auto GetTableOid() const -> table_oid_t { return table_oid_; }

  /**
   * 根据绑定的基表引用推断扫描操作的输出 schema。
   * （作用：自动生成扫描后输出数据的结构，无需手动定义）
   * @param table_ref 绑定的基表引用（包含表的元数据）
   * @return 推断出的输出 schema
   */
  static auto InferScanSchema(const BoundBaseTableRef &table_ref) -> Schema;

  /** 复制当前计划节点并替换子节点（用于查询计划优化时修改节点结构） */
  BUSTUB_PLAN_NODE_CLONE_WITH_CHILDREN(SeqScanPlanNode);

  /** 要扫描其元组（行数据）的表的 OID */
  table_oid_t table_oid_;

  /** 表的名称（用于日志、调试等场景的可读性展示） */
  std::string table_name_;

  /**
   * 顺序扫描中的过滤谓词（条件表达式）。
   * 例如：筛选出 "age > 18" 的行，避免扫描全部数据后再过滤，提升效率。
   * 2023 年秋季学期新增：将启用 MergeFilterScan 规则，
   * 该规则可将过滤条件与扫描合并，进一步支持索引点查询（通过索引直接定位符合条件的数据，无需全表扫描）。
  UNIMPLEMENTED("TODO(P3): Add implementation.");
   */
  AbstractExpressionRef filter_predicate_;

 protected:
  /**
   * 将计划节点转换为字符串（用于调试或日志输出）。
   * 例如：若有过滤条件，输出 "SeqScan { table=students, filter=age > 18 }"；
   * 若无过滤条件，输出 "SeqScan { table=students }"。
   */
  auto PlanNodeToString() const -> std::string override {
    if (filter_predicate_) {
      return fmt::format("SeqScan {{ table={}, filter={} }}", table_name_, filter_predicate_);
    }
    return fmt::format("SeqScan {{ table={} }}", table_name_);
  }
};

}  // namespace bustub
