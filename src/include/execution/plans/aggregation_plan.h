//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_plan.h
//
// 标识：src/include/execution/plans/aggregation_plan.h
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库组
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/abstract_plan.h"
#include "fmt/format.h"
#include "storage/table/tuple.h"

namespace bustub {

/** 聚合类型枚举，定义了系统中支持的所有聚合函数类型 */
enum class AggregationType { CountStarAggregate, CountAggregate, SumAggregate, MinAggregate, MaxAggregate };

/**
 * 聚合计划节点，代表SQL中的各类聚合函数操作
 * 例如 COUNT()、SUM()、MIN() 和 MAX()。
 *
 * 注意：为简化本项目，聚合计划节点必须且只能有一个子节点。
 */
class AggregationPlanNode : public AbstractPlanNode {
 public:
  /**
   * 构造一个新的聚合计划节点
   * @param output_schema 该计划节点的输出模式（Schema）
   * @param child 提供聚合数据源的子计划节点
   * @param group_bys 聚合操作的 GROUP BY 子句表达式列表
   * @param aggregates 待执行的聚合表达式列表
   * @param agg_types 聚合表达式对应的聚合类型列表
   */
  AggregationPlanNode(SchemaRef output_schema, AbstractPlanNodeRef child, std::vector<AbstractExpressionRef> group_bys,
                      std::vector<AbstractExpressionRef> aggregates, std::vector<AggregationType> agg_types)
      : AbstractPlanNode(std::move(output_schema), {std::move(child)}),
        group_bys_(std::move(group_bys)),
        aggregates_(std::move(aggregates)),
        agg_types_(std::move(agg_types)) {}

  /** @return 计划节点的类型 */
  auto GetType() const -> PlanType override { return PlanType::Aggregation; }

  /** @return 聚合计划节点的子节点 */
  auto GetChildPlan() const -> AbstractPlanNodeRef {
    BUSTUB_ASSERT(GetChildren().size() == 1, "聚合计划节点应只有一个子节点。");
    return GetChildAt(0);
  }

  /** @return 第idx个 GROUP BY 表达式 */
  auto GetGroupByAt(uint32_t idx) const -> const AbstractExpressionRef & { return group_bys_[idx]; }

  /** @return 所有 GROUP BY 表达式的列表 */
  auto GetGroupBys() const -> const std::vector<AbstractExpressionRef> & { return group_bys_; }

  /** @return 第idx个聚合表达式 */
  auto GetAggregateAt(uint32_t idx) const -> const AbstractExpressionRef & { return aggregates_[idx]; }

  /** @return 所有聚合表达式的列表 */
  auto GetAggregates() const -> const std::vector<AbstractExpressionRef> & { return aggregates_; }

  /** @return 所有聚合类型的列表 */
  auto GetAggregateTypes() const -> const std::vector<AggregationType> & { return agg_types_; }

  /**
   * 根据 GROUP BY 表达式、聚合表达式和聚合类型，推导聚合操作的输出模式
   * @param group_bys GROUP BY 表达式列表
   * @param aggregates 聚合表达式列表
   * @param agg_types 聚合类型列表
   * @return 推导后的聚合输出模式
   */
  static auto InferAggSchema(const std::vector<AbstractExpressionRef> &group_bys,
                             const std::vector<AbstractExpressionRef> &aggregates,
                             const std::vector<AggregationType> &agg_types) -> Schema;

  /** 克隆聚合计划节点并替换子节点的宏（BusTub框架内置） */
  BUSTUB_PLAN_NODE_CLONE_WITH_CHILDREN(AggregationPlanNode);

  /** GROUP BY 子句对应的表达式列表 */
  std::vector<AbstractExpressionRef> group_bys_;
  /** 聚合操作对应的表达式列表 */
  std::vector<AbstractExpressionRef> aggregates_;
  /** 聚合表达式对应的聚合类型列表 */
  std::vector<AggregationType> agg_types_;

 protected:
  /** 将计划节点转换为字符串（用于调试和日志） */
  auto PlanNodeToString() const -> std::string override;
};

/** 聚合键，代表聚合操作中的分组键 */
struct AggregateKey {
  /** GROUP BY 子句对应的具体值列表 */
  std::vector<Value> group_bys_;

  /**
   * 比较两个聚合键是否相等。已实现NULL值处理逻辑
   * @param other 待比较的另一个聚合键
   * @return 若两个聚合键的 GROUP BY 值全部相等则返回true，否则返回false
   */
  auto operator==(const AggregateKey &other) const -> bool {
    // 第一步：先判断分组键的数量是否相等，数量不同直接返回false
    if (group_bys_.size() != other.group_bys_.size()) {
      return false;
    }

    // 第二步：遍历每个分组键值，逐一向比较（包含NULL处理）
    for (uint32_t i = 0; i < group_bys_.size(); i++) {
      const auto &lhs_val = group_bys_[i];
      const auto &rhs_val = other.group_bys_[i];

      // NULL值比较逻辑：SQL语义中两个NULL视为相等，NULL与非NULL视为不相等
      if (lhs_val.IsNull() && rhs_val.IsNull()) {
        // 两个都是NULL，继续比较下一个值
        continue;
      }
      if (lhs_val.IsNull() || rhs_val.IsNull()) {
        // 一个是NULL，一个不是NULL，直接返回false
        return false;
      }

      // 非NULL值：使用原有逻辑调用CompareEquals比较
      if (lhs_val.CompareEquals(rhs_val) != CmpBool::CmpTrue) {
        return false;
      }
    }

    // 所有值都匹配（包括NULL的情况）
    return true;
  }
};

/** 聚合值，代表聚合操作中每个分组的实时聚合结果 */
struct AggregateValue {
  /** 聚合表达式对应的具体值列表 */
  std::vector<Value> aggregates_;
};

}  // namespace bustub

namespace std {

/** 为 AggregateKey 实现 std::hash 哈希函数，使其可作为unordered_map的键 */
template <>
struct hash<bustub::AggregateKey> {
  auto operator()(const bustub::AggregateKey &agg_key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : agg_key.group_bys_) {
      if (!key.IsNull()) {
        // 组合每个 GROUP BY 值的哈希，生成聚合键的整体哈希值
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

}  // namespace std

/** 为 AggregationType 实现 fmt 格式化器，支持直接打印聚合类型的名称 */
template <>
struct fmt::formatter<bustub::AggregationType> : formatter<std::string> {
  template <typename FormatContext>
  auto format(bustub::AggregationType c, FormatContext &ctx) const {
    using bustub::AggregationType;
    std::string name = "unknown";
    switch (c) {
      case AggregationType::CountStarAggregate:
        name = "count_star";
        break;
      case AggregationType::CountAggregate:
        name = "count";
        break;
      case AggregationType::SumAggregate:
        name = "sum";
        break;
      case AggregationType::MinAggregate:
        name = "min";
        break;
      case AggregationType::MaxAggregate:
        name = "max";
        break;
    }
    return formatter<std::string>::format(name, ctx);
  }
};