//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// tuple.cpp
//
// 标识：src/storage/table/tuple.cpp
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库组
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <sstream>
#include <string>
#include <vector>

#include "storage/table/tuple.h"

namespace bustub {

// TODO(Amadou): 看起来不支持空值。需要添加空值位图吗？
/**
 * 基于输入值创建新元组的构造函数
 */
Tuple::Tuple(std::vector<Value> values, const Schema *schema) {
  assert(values.size() == schema->GetColumnCount());

  // 1. 计算元组的大小
  uint32_t tuple_size = schema->GetInlinedStorageSize();
  for (auto &i : schema->GetUnlinedColumns()) {
    auto len = values[i].GetStorageSize();
    if (len == BUSTUB_VALUE_NULL) {
      len = 0;
    }
    tuple_size += sizeof(uint32_t) + len;
  }

  // 2. 分配内存
  data_.resize(tuple_size);
  std::fill(data_.begin(), data_.end(), 0);

  // 3. 根据输入值序列化每个属性
  uint32_t column_count = schema->GetColumnCount();
  uint32_t offset = schema->GetInlinedStorageSize();

  for (uint32_t i = 0; i < column_count; i++) {
    const auto &col = schema->GetColumn(i);
    if (!col.IsInlined()) {
      // 序列化相对偏移量，实际的字符串数据存储在这里
      *reinterpret_cast<uint32_t *>(data_.data() + col.GetOffset()) = offset;
      // 序列化字符串值，按原样存储（大小+数据）
      values[i].SerializeTo(data_.data() + offset);
      auto len = values[i].GetStorageSize();
      if (len == BUSTUB_VALUE_NULL) {
        len = 0;
      }
      offset += sizeof(uint32_t) + len;
    } else {
      values[i].SerializeTo(data_.data() + col.GetOffset());
    }
  }
}

/**
 * 通过复制现有字节创建新元组的构造函数
 */
Tuple::Tuple(RID rid, const char *data, uint32_t size) {
  rid_ = rid;
  data_.resize(size);
  memcpy(data_.data(), data, size);
}

/**
 * 获取指定列的值（常量版本）
 * 检查模式以确定如何返回值
 */
auto Tuple::GetValue(const Schema *schema, const uint32_t column_idx) const -> Value {
  assert(schema);
  const TypeId column_type = schema->GetColumn(column_idx).GetType();
  const char *data_ptr = GetDataPtr(schema, column_idx);
  // 第三个参数"is_inlined"未使用
  return Value::DeserializeFrom(data_ptr, column_type);
}

/**
 * 基于模式和属性生成键元组
 */
auto Tuple::KeyFromTuple(const Schema &schema, const Schema &key_schema, const std::vector<uint32_t> &key_attrs) const
    -> Tuple {
  std::vector<Value> values;
  values.reserve(key_attrs.size());
  for (auto idx : key_attrs) {
    values.emplace_back(this->GetValue(&schema, idx));
  }
  return {values, &key_schema};
}

/**
 * 获取特定列的起始存储地址
 */
auto Tuple::GetDataPtr(const Schema *schema, const uint32_t column_idx) const -> const char * {
  assert(schema);
  const auto &col = schema->GetColumn(column_idx);
  bool is_inlined = col.IsInlined();
  // 对于内联类型，数据直接存储在其位置
  if (is_inlined) {
    return (data_.data() + col.GetOffset());
  }
  // 从元组数据中读取相对偏移量
  int32_t offset = *reinterpret_cast<const int32_t *>(data_.data() + col.GetOffset());
  // 返回VARCHAR类型的实际数据的起始地址
  return (data_.data() + offset);
}

auto Tuple::ToString(const Schema *schema) const -> std::string {
  std::stringstream os;

  int column_count = schema->GetColumnCount();
  bool first = true;
  os << "(";
  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    if (first) {
      first = false;
    } else {
      os << ", ";
    }
    if (IsNull(schema, column_itr)) {
      os << "<NULL>";
    } else {
      Value val = (GetValue(schema, column_itr));
      os << val.ToString();
    }
  }
  os << ")";

  return os.str();
}

/**
 * 序列化元组数据
 */
void Tuple::SerializeTo(char *storage) const {
  int32_t sz = data_.size();
  memcpy(storage, &sz, sizeof(int32_t));
  memcpy(storage + sizeof(int32_t), data_.data(), sz);
}

/**
 * 反序列化元组数据（深拷贝）
 */
void Tuple::DeserializeFrom(const char *storage) {
  uint32_t size = *reinterpret_cast<const uint32_t *>(storage);
  this->data_.resize(size);
  memcpy(this->data_.data(), storage + sizeof(int32_t), size);
}

}  // namespace bustub