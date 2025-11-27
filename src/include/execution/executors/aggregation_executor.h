//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.h
//
// 标识：src/include/execution/executors/aggregation_executor.h
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库组
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/macros.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/aggregation_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * 一个简化的哈希表，包含聚合操作所需的所有核心功能
 */
class SimpleAggregationHashTable {
 public:
  /**
   * 构造一个新的 SimpleAggregationHashTable 实例
   * @param agg_exprs 聚合表达式列表
   * @param agg_types 聚合操作的类型列表（如 COUNT、SUM、MIN 等）
   */
  SimpleAggregationHashTable(const std::vector<AbstractExpressionRef> &agg_exprs,
                             const std::vector<AggregationType> &agg_types)
      : agg_exprs_{agg_exprs}, agg_types_{agg_types} {}

  /** @return 为当前聚合执行器生成的初始聚合值 */
  auto GenerateInitialAggregateValue() -> AggregateValue {
    std::vector<Value> values{};
    for (const auto &agg_type : agg_types_) {
      switch (agg_type) {
        case AggregationType::CountStarAggregate:
          // COUNT(*) 的初始值为 0
          values.emplace_back(ValueFactory::GetIntegerValue(0));
          break;
        case AggregationType::CountAggregate:
        case AggregationType::SumAggregate:
        case AggregationType::MinAggregate:
        case AggregationType::MaxAggregate:
          // 其他聚合操作的初始值为 NULL
          values.emplace_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
          break;
      }
    }
    return {values};
  }

  /**
   * 将输入值合并到聚合结果中
   * @param[out] result 输出的聚合结果（会被修改）
   * @param input 待合并的输入聚合值
   */
  void CombineAggregateValues(AggregateValue *result, const AggregateValue &input) {
    for (uint32_t i = 0; i < agg_exprs_.size(); i++) {
      const auto &agg_type = agg_types_[i];
      Value &res_val = result->aggregates_[i];
      const Value &in_val = input.aggregates_[i];

      switch (agg_type) {
        case AggregationType::CountStarAggregate:
          // COUNT(*) 直接累加 1（无需输入值，统计行数）
          res_val = res_val.Add(ValueFactory::GetIntegerValue(1));
          break;
        case AggregationType::CountAggregate:
          // COUNT(col) 仅当输入值非 NULL 时累加 1
          if (!in_val.IsNull()) {
            res_val =
                res_val.IsNull() ? ValueFactory::GetIntegerValue(1) : res_val.Add(ValueFactory::GetIntegerValue(1));
          }
          break;
        case AggregationType::SumAggregate:
          // SUM(col) 仅当输入值非 NULL 时累加
          if (!in_val.IsNull()) {
            res_val = res_val.IsNull() ? in_val : res_val.Add(in_val);
          }
          break;
        case AggregationType::MinAggregate:
          // MIN(col) 仅当输入值非 NULL 时取最小值
          if (!in_val.IsNull()) {
            res_val =
                res_val.IsNull() ? in_val : (res_val.CompareLessThan(in_val) == CmpBool::CmpTrue ? res_val : in_val);
          }
          break;
        case AggregationType::MaxAggregate:
          // MAX(col) 仅当输入值非 NULL 时取最大值
          if (!in_val.IsNull()) {
            res_val =
                res_val.IsNull() ? in_val : (res_val.CompareGreaterThan(in_val) == CmpBool::CmpTrue ? res_val : in_val);
          }
          break;
      }
    }
  }

  /**
   * 将键值对插入哈希表，并将输入聚合值与当前哈希表中的聚合结果合并
   * @param agg_key 待插入的聚合键（分组键）
   * @param agg_val 待插入的聚合值
   */
  void InsertCombine(const AggregateKey &agg_key, const AggregateValue &agg_val) {
    if (ht_.count(agg_key) == 0) {
      // 若分组键不存在，初始化该分组的聚合值
      ht_.insert({agg_key, GenerateInitialAggregateValue()});
    }
    // 合并新的聚合值到已有结果中
    CombineAggregateValues(&ht_[agg_key], agg_val);
  }

  /**
   * 清空哈希表中的所有数据
   */
  void Clear() { ht_.clear(); }

  /** 聚合哈希表的迭代器类 */
  class Iterator {
   public:
    /** 为聚合哈希表的 map 构造一个迭代器 */
    explicit Iterator(std::unordered_map<AggregateKey, AggregateValue>::const_iterator iter) : iter_{iter} {}

    /** @return 迭代器当前指向的聚合键 */
    auto Key() -> const AggregateKey & { return iter_->first; }

    /** @return 迭代器当前指向的聚合值 */
    auto Val() -> const AggregateValue & { return iter_->second; }

    /** @return 自增后的迭代器（前置++） */
    auto operator++() -> Iterator & {
      ++iter_;
      return *this;
    }

    /** @return 两个迭代器是否指向同一位置 */
    auto operator==(const Iterator &other) -> bool { return this->iter_ == other.iter_; }

    /** @return 两个迭代器是否指向不同位置 */
    auto operator!=(const Iterator &other) -> bool { return this->iter_ != other.iter_; }

   private:
    /** 聚合哈希表的底层 map 迭代器 */
    std::unordered_map<AggregateKey, AggregateValue>::const_iterator iter_;
  };

  /** @return 指向哈希表起始位置的迭代器 */
  auto Begin() -> Iterator { return Iterator{ht_.cbegin()}; }

  /** @return 指向哈希表末尾位置的迭代器 */
  auto End() -> Iterator { return Iterator{ht_.cend()}; }

 private:
  /** 哈希表的底层存储：键为聚合分组键，值为对应分组的聚合结果 */
  std::unordered_map<AggregateKey, AggregateValue> ht_{};
  /** 当前聚合执行器的聚合表达式列表 */
  const std::vector<AbstractExpressionRef> &agg_exprs_;
  /** 当前聚合执行器的聚合类型列表 */
  const std::vector<AggregationType> &agg_types_;
};

/**
 * 聚合执行器，用于执行聚合操作（如 COUNT、SUM、MIN、MAX）
 * 聚合的数据源为子执行器产生的元组
 */
class AggregationExecutor : public AbstractExecutor {
 public:
  AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                      std::unique_ptr<AbstractExecutor> &&child_executor);

  void Init() override;

  auto Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch, size_t batch_size)
      -> bool override;

  /** @return 聚合操作的输出模式（Schema） */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

  auto GetChildExecutor() const -> const AbstractExecutor *;

 private:
  /**
   * 将元组转换为聚合键（分组键）
   * @param tuple 待转换的元组
   * @return 元组对应的聚合分组键
   */
  auto MakeAggregateKey(const Tuple *tuple) -> AggregateKey {
    std::vector<Value> keys;
    for (const auto &expr : plan_->GetGroupBys()) {
      // 计算分组表达式的值，作为聚合键的一部分
      keys.emplace_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    return {keys};
  }

  /**
   * 将元组转换为聚合值
   * @param tuple 待转换的元组
   * @return 元组对应的聚合值（聚合表达式的计算结果）
   */
  auto MakeAggregateValue(const Tuple *tuple) -> AggregateValue {
    std::vector<Value> vals;
    for (const auto &expr : plan_->GetAggregates()) {
      // 计算聚合表达式的值，作为聚合值的一部分
      vals.emplace_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    return {vals};
  }

  /**
   * 将聚合键和聚合值转换为输出元组
   * @param key 聚合键（分组值）
   * @param val 聚合值（聚合结果）
   * @return 符合输出Schema的元组
   */
  auto MakeOutputTuple(const AggregateKey &key, const AggregateValue &val) -> Tuple {
    std::vector<Value> output_values;
    // 先添加分组键的值
    for (const auto &v : key.group_bys_) {
      output_values.push_back(v);
    }
    // 再添加聚合结果的值
    for (const auto &v : val.aggregates_) {
      output_values.push_back(v);
    }
    return Tuple(output_values, &GetOutputSchema());
  }

 private:
  /** 聚合执行计划节点，包含聚合的元数据（分组表达式、聚合表达式、聚合类型等） */
  const AggregationPlanNode *plan_;

  /** 子执行器，为聚合操作提供原始元组数据 */
  std::unique_ptr<AbstractExecutor> child_executor_;

  /** 简化的聚合哈希表 */
  SimpleAggregationHashTable aht_;

  /** 聚合哈希表的迭代器，用于遍历聚合结果 */
  SimpleAggregationHashTable::Iterator aht_iterator_;

  /** 标记是否已经完成聚合计算（避免重复计算） */
  bool is_aggregated_;

  bool is_null_and_done_{false};
};

}  // namespace bustub
