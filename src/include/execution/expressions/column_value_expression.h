//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// column_value_expression.h
//
// 标识：src/include/execution/expressions/column_value_expression.h
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库组
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/schema.h"
#include "execution/expressions/abstract_expression.h"
#include "storage/table/tuple.h"

namespace bustub {
/**
 * ColumnValueExpression 用于维护相对于特定模式或连接操作的元组索引和列索引。
 */
class ColumnValueExpression : public AbstractExpression {
 public:
  /**
   * ColumnValueExpression 是对"表.成员"的抽象，通过索引来表示。
   * @param tuple_idx {元组索引 0 = 连接操作的左侧，元组索引 1 = 连接操作的右侧}
   * @param col_idx 列在模式中的索引
   * @param ret_type 表达式的返回类型
   */
  ColumnValueExpression(uint32_t tuple_idx, uint32_t col_idx, Column ret_type)
      : AbstractExpression({}, std::move(ret_type)), tuple_idx_{tuple_idx}, col_idx_{col_idx} {}

  /**
   * 对单个元组求值
   * @param tuple 要求值的元组
   * @param schema 元组对应的模式
   * @return 元组中指定列的值
   */
  auto Evaluate(const Tuple *tuple, const Schema &schema) const -> Value override {
    return tuple->GetValue(&schema, col_idx_);
  }

  /**
   * 对连接操作中的元组求值
   * @param left_tuple 左表元组
   * @param left_schema 左表元组的模式
   * @param right_tuple 右表元组
   * @param right_schema 右表元组的模式
   * @return 根据元组索引返回左表或右表中指定列的值
   */
  auto EvaluateJoin(const Tuple *left_tuple, const Schema &left_schema, const Tuple *right_tuple,
                    const Schema &right_schema) const -> Value override {
    return tuple_idx_ == 0 ? left_tuple->GetValue(&left_schema, col_idx_)
                           : right_tuple->GetValue(&right_schema, col_idx_);
  }

  /** @return 元组索引 */
  auto GetTupleIdx() const -> uint32_t { return tuple_idx_; }

  /** @return 列索引 */
  auto GetColIdx() const -> uint32_t { return col_idx_; }

  /** @return 表达式节点及其子节点的字符串表示 */
  auto ToString() const -> std::string override { return fmt::format("#{}.{}", tuple_idx_, col_idx_); }

  BUSTUB_EXPR_CLONE_WITH_CHILDREN(ColumnValueExpression);

 private:
  /** 元组索引 0 = 连接操作的左侧，元组索引 1 = 连接操作的右侧 */
  uint32_t tuple_idx_;
  /** 列索引指的是列在元组模式中的位置，例如模式 {A,B,C} 的索引为 {0,1,2} */
  uint32_t col_idx_;
};
}  // namespace bustub