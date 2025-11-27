//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// comparison_expression.h
//
// 标识：src/include/execution/expressions/comparison_expression.h
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库组
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <utility>
#include <vector>

#include "catalog/schema.h"
#include "execution/expressions/abstract_expression.h"
#include "fmt/format.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

/** ComparisonType 表示要执行的比较操作类型 */
enum class ComparisonType {
  Equal,              // 等于
  NotEqual,           // 不等于
  LessThan,           // 小于
  LessThanOrEqual,    // 小于等于
  GreaterThan,        // 大于
  GreaterThanOrEqual  // 大于等于
};

/**
 * ComparisonExpression 表示对两个表达式执行比较操作
 */
class ComparisonExpression : public AbstractExpression {
 public:
  /** 创建一个新的比较表达式，代表 (left 比较运算符 right) */
  ComparisonExpression(AbstractExpressionRef left, AbstractExpressionRef right, ComparisonType comp_type)
      : AbstractExpression({std::move(left), std::move(right)}, Column{"<val>", TypeId::BOOLEAN}),
        comp_type_{comp_type} {}

  /**
   * 对单个元组求值
   * @param tuple 要求值的元组
   * @param schema 元组对应的模式
   * @return 比较结果（布尔值）
   */
  auto Evaluate(const Tuple *tuple, const Schema &schema) const -> Value override {
    Value lhs = GetChildAt(0)->Evaluate(tuple, schema);
    Value rhs = GetChildAt(1)->Evaluate(tuple, schema);
    return ValueFactory::GetBooleanValue(PerformComparison(lhs, rhs));
  }

  /**
   * 对连接操作中的元组求值
   * @param left_tuple 左表元组
   * @param left_schema 左表元组的模式
   * @param right_tuple 右表元组
   * @param right_schema 右表元组的模式
   * @return 比较结果（布尔值）
   */
  auto EvaluateJoin(const Tuple *left_tuple, const Schema &left_schema, const Tuple *right_tuple,
                    const Schema &right_schema) const -> Value override {
    Value lhs = GetChildAt(0)->EvaluateJoin(left_tuple, left_schema, right_tuple, right_schema);
    Value rhs = GetChildAt(1)->EvaluateJoin(left_tuple, left_schema, right_tuple, right_schema);
    return ValueFactory::GetBooleanValue(PerformComparison(lhs, rhs));
  }

  /** @return 表达式节点及其子节点的字符串表示 */
  auto ToString() const -> std::string override {
    return fmt::format("({}{}{})", *GetChildAt(0), comp_type_, *GetChildAt(1));
  }

  BUSTUB_EXPR_CLONE_WITH_CHILDREN(ComparisonExpression);

  /** 比较操作类型 */
  ComparisonType comp_type_;

 private:
  /**
   * 执行比较操作
   * @param lhs 左操作数
   * @param rhs 右操作数
   * @return 比较结果（CmpTrue/CmpFalse/CmpNull）
   */
  auto PerformComparison(const Value &lhs, const Value &rhs) const -> CmpBool {
    switch (comp_type_) {
      case ComparisonType::Equal:
        return lhs.CompareEquals(rhs);
      case ComparisonType::NotEqual:
        return lhs.CompareNotEquals(rhs);
      case ComparisonType::LessThan:
        return lhs.CompareLessThan(rhs);
      case ComparisonType::LessThanOrEqual:
        return lhs.CompareLessThanEquals(rhs);
      case ComparisonType::GreaterThan:
        return lhs.CompareGreaterThan(rhs);
      case ComparisonType::GreaterThanOrEqual:
        return lhs.CompareGreaterThanEquals(rhs);
      default:
        BUSTUB_ASSERT(false, "不支持的比较类型");
    }
  }
};
}  // namespace bustub

/** 为ComparisonType枚举提供fmt格式化支持 */
template <>
struct fmt::formatter<bustub::ComparisonType> : formatter<string_view> {
  template <typename FormatContext>
  auto format(bustub::ComparisonType c, FormatContext &ctx) const {
    string_view name;
    switch (c) {
      case bustub::ComparisonType::Equal:
        name = "=";
        break;
      case bustub::ComparisonType::NotEqual:
        name = "!=";
        break;
      case bustub::ComparisonType::LessThan:
        name = "<";
        break;
      case bustub::ComparisonType::LessThanOrEqual:
        name = "<=";
        break;
      case bustub::ComparisonType::GreaterThan:
        name = ">";
        break;
      case bustub::ComparisonType::GreaterThanOrEqual:
        name = ">=";
        break;
      default:
        name = "Unknown";
        break;
    }
    return formatter<string_view>::format(name, ctx);
  }
};