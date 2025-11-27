//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// schema.h
//
// 标识：src/include/catalog/schema.h
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库组
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "catalog/column.h"
#include "common/exception.h"
#include "type/type.h"

namespace bustub {

class Schema;
using SchemaRef = std::shared_ptr<const Schema>;

class Schema {
 public:
  explicit Schema(const std::vector<Column> &columns);

  /**
   * 复制一个模式，并只保留指定的属性列
   * @param from 源模式
   * @param attrs 需要保留的属性列索引
   * @return 包含指定属性列的新模式
   */
  static auto CopySchema(const Schema *from, const std::vector<uint32_t> &attrs) -> Schema {
    std::vector<Column> cols;
    cols.reserve(attrs.size());
    for (const auto i : attrs) {
      cols.emplace_back(from->columns_[i]);
    }
    return Schema{cols};
  }

  /** @return 模式中所有的列 */
  auto GetColumns() const -> const std::vector<Column> & { return columns_; }

  /**
   * 返回模式中指定索引的列
   * @param col_idx 目标列的索引
   * @return 目标列
   */
  auto GetColumn(const uint32_t col_idx) const -> const Column & { return columns_[col_idx]; }

  /**
   * 查找并返回模式中第一个具有指定名称的列的索引
   * 如果多个列具有相同的名称，返回第一个出现的索引
   * @param col_name 要查找的列名
   * @return 具有指定名称的列的索引，如果不存在则抛出异常
   */
  auto GetColIdx(const std::string &col_name) const -> uint32_t {
    if (auto col_idx = TryGetColIdx(col_name)) {
      return *col_idx;
    }
    UNREACHABLE("列不存在");
  }

  /**
   * 查找并返回模式中第一个具有指定名称的列的索引
   * 如果多个列具有相同的名称，返回第一个出现的索引
   * @param col_name 要查找的列名
   * @return 具有指定名称的列的索引，如果不存在则返回std::nullopt
   */
  auto TryGetColIdx(const std::string &col_name) const -> std::optional<uint32_t> {
    for (uint32_t i = 0; i < columns_.size(); ++i) {
      if (columns_[i].GetName() == col_name) {
        return std::optional{i};
      }
    }
    return std::nullopt;
  }

  /** @return 非内联列的索引 */
  auto GetUnlinedColumns() const -> const std::vector<uint32_t> & { return uninlined_columns_; }

  /** @return 模式中元组的列数 */
  auto GetColumnCount() const -> uint32_t { return static_cast<uint32_t>(columns_.size()); }

  /** @return 非内联列的数量 */
  auto GetUnlinedColumnCount() const -> uint32_t { return static_cast<uint32_t>(uninlined_columns_.size()); }

  /** @return 一个元组使用的字节数 */
  inline auto GetInlinedStorageSize() const -> uint32_t { return length_; }

  /** @return 如果所有列都是内联的则返回true，否则返回false */
  inline auto IsInlined() const -> bool { return tuple_is_inlined_; }

  /** 转换为字符串表示 */
  auto ToString(bool simplified = true) const -> std::string;

 private:
  /** 固定长度的列大小，即一个元组使用的字节数 */
  uint32_t length_;

  /** 模式中所有的列，包括内联和非内联的 */
  std::vector<Column> columns_;

  /** 如果所有列都是内联的则为true，否则为false */
  bool tuple_is_inlined_{true};

  /** 所有非内联列的索引 */
  std::vector<uint32_t> uninlined_columns_;
};

}  // namespace bustub

template <typename T>
struct fmt::formatter<T, std::enable_if_t<std::is_base_of<bustub::Schema, T>::value, char>>
    : fmt::formatter<std::string> {
  template <typename FormatCtx>
  auto format(const bustub::Schema &x, FormatCtx &ctx) const {
    return fmt::formatter<std::string>::format(x.ToString(), ctx);
  }
};

template <typename T>
struct fmt::formatter<std::shared_ptr<T>, std::enable_if_t<std::is_base_of<bustub::Schema, T>::value, char>>
    : fmt::formatter<std::string> {
  template <typename FormatCtx>
  auto format(const std::shared_ptr<T> &x, FormatCtx &ctx) const {
    if (x != nullptr) {
      return fmt::formatter<std::string>::format(x->ToString(), ctx);
    }
    return fmt::formatter<std::string>::format("", ctx);
  }
};

template <typename T>
struct fmt::formatter<std::unique_ptr<T>, std::enable_if_t<std::is_base_of<bustub::Schema, T>::value, char>>
    : fmt::formatter<std::string> {
  template <typename FormatCtx>
  auto format(const std::unique_ptr<T> &x, FormatCtx &ctx) const {
    if (x != nullptr) {
      return fmt::formatter<std::string>::format(x->ToString(), ctx);
    }
    return fmt::formatter<std::string>::format("", ctx);
  }
};