//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// string_expression.h
//
// 标识：src/include/execution/expressions/string_expression.h
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库组
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <cctype>
#include <string>
#include <utility>
#include <vector>

#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/abstract_expression.h"
#include "fmt/format.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/type_id.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * 字符串表达式类型枚举，定义支持的字符串操作
 */
enum class StringExpressionType {
  Lower,  // 转换为小写
  Upper   // 转换为大写
};

/**
 * StringExpression 用于处理字符串转换操作的表达式（如大小写转换）
 */
class StringExpression : public AbstractExpression {
 public:
  /**
   * 构造字符串表达式实例
   * @param arg 子表达式，其计算结果将作为字符串转换的输入
   * @param expr_type 字符串转换的类型（大写或小写）
   */
  StringExpression(AbstractExpressionRef arg, StringExpressionType expr_type)
      : AbstractExpression({std::move(arg)}, Column{"<val>", TypeId::VARCHAR, 256 /* 硬编码最大长度 */}),
        expr_type_{expr_type} {
    // 验证子表达式的返回类型必须是字符串类型
    if (GetChildAt(0)->GetReturnType().GetType() != TypeId::VARCHAR) {
      BUSTUB_ENSURE(GetChildAt(0)->GetReturnType().GetType() == TypeId::VARCHAR, "输入参数类型不符合预期");
    }
  }

  /**
   * 执行字符串转换计算
   * @param val 待转换的字符串
   * @return 转换后的字符串
   */
  auto Compute(const std::string &val) const -> std::string {
    // TODO(student): 实现大小写转换逻辑
    return {};
  }

  /**
   * 对单个元组求值
   * @param tuple 要求值的元组
   * @param schema 元组对应的模式
   * @return 转换后的字符串值
   */
  auto Evaluate(const Tuple *tuple, const Schema &schema) const -> Value override {
    Value val = GetChildAt(0)->Evaluate(tuple, schema);
    auto str = val.GetAs<char *>();
    return ValueFactory::GetVarcharValue(Compute(str));
  }

  /**
   * 对连接操作中的元组求值
   * @param left_tuple 左表元组
   * @param left_schema 左表元组的模式
   * @param right_tuple 右表元组
   * @param right_schema 右表元组的模式
   * @return 转换后的字符串值
   */
  auto EvaluateJoin(const Tuple *left_tuple, const Schema &left_schema, const Tuple *right_tuple,
                    const Schema &right_schema) const -> Value override {
    Value val = GetChildAt(0)->EvaluateJoin(left_tuple, left_schema, right_tuple, right_schema);
    auto str = val.GetAs<char *>();
    return ValueFactory::GetVarcharValue(Compute(str));
  }

  /** @return 表达式节点及其子节点的字符串表示 */
  auto ToString() const -> std::string override { return fmt::format("{}({})", expr_type_, *GetChildAt(0)); }

  BUSTUB_EXPR_CLONE_WITH_CHILDREN(StringExpression);

  /** 字符串表达式的操作类型（大写或小写转换） */
  StringExpressionType expr_type_;

 private:
};
}  // namespace bustub

/** 为StringExpressionType枚举提供fmt格式化支持 */
template <>
struct fmt::formatter<bustub::StringExpressionType> : formatter<string_view> {
  template <typename FormatContext>
  auto format(bustub::StringExpressionType c, FormatContext &ctx) const {
    string_view name;
    switch (c) {
      case bustub::StringExpressionType::Upper:
        name = "upper";
        break;
      case bustub::StringExpressionType::Lower:
        name = "lower";
        break;
      default:
        name = "Unknown";
        break;
    }
    return formatter<string_view>::format(name, ctx);
  }
};