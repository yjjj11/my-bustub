#pragma once

#include <cstddef>
#include <cstring>
#include <optional>
#include <vector>

// BusTub核心依赖
#include "catalog/schema.h"
#include "common/config.h"  // 包含BUSTUB_PAGE_SIZE常量
#include "common/exception.h"
#include "storage/table/tuple.h"  // 引入Tuple的SerializeTo/DeserializeFrom

namespace bustub {

/**
 * 用于存储外部归并排序和哈希连接中间数据的页布局模板类。
 * 核心特性：
 * 1. 完全匹配Tuple的SerializeTo/DeserializeFrom序列化格式；
 * 2. 作为页数据的内存布局模板，通过页保护的As/AsMut重映射使用；
 * 3. 元数据存储在页起始位置，变长元组从元数据后开始写入；
 * 4. 线程安全由外部页保护（ReadPageGuard/WritePageGuard）保证。
 */
class IntermediateResultPage {
 public:
  std::size_t tuple_count_;  // 页内已存储的元组数量（8字节）
  std::size_t page_size_;    // 页的物理总大小（8字节，默认BUSTUB_PAGE_SIZE=4096）
  std::size_t next_offset_;  // 下一个元组的写入偏移（8字节，初始为元数据总大小）

  static constexpr std::size_t TUPLE_HEADER_SIZE = sizeof(int32_t);

  IntermediateResultPage() = delete;
  IntermediateResultPage(const IntermediateResultPage &other) = delete;
  IntermediateResultPage &operator=(const IntermediateResultPage &other) = delete;
  ~IntermediateResultPage() = delete;

  void InitMetadata(std::size_t page_size = BUSTUB_PAGE_SIZE) {
    tuple_count_ = 0;
    page_size_ = page_size;
    next_offset_ = 24;  // 元组从元数据区结束后开始写入
  }

  void Reset() {
    tuple_count_ = 0;
    next_offset_ = 24;  // 重置写入偏移为元数据大小
  }

  bool WriteTuple(const Tuple &tuple) {
    if (page_size_ == 0 || next_offset_ < 24) {
      return false;
    }

    std::size_t required_space = tuple.GetLength() + 4;

    // 3. 检查页内剩余空间是否充足
    if (next_offset_ + required_space > page_size_) {
      return false;
    }

    // 4. 获取页内的写入指针，调用Tuple的SerializeTo完成序列化写入
    char *tuple_storage = reinterpret_cast<char *>(this) + next_offset_;
    tuple.SerializeTo(tuple_storage);

    // 5. 更新元数据：偏移量后移，元组数量+1
    next_offset_ += required_space;
    tuple_count_++;
    return true;
  }

  bool ReadTuple(size_t index, Tuple &result_tuple) const {
    // 1. 校验索引有效性
    if (index >= tuple_count_ || page_size_ == 0) {
      return false;
    }

    // 2. 遍历页内元组，定位目标索引的元组起始地址
    const char *page_data = reinterpret_cast<const char *>(this);
    size_t current_offset = 24;  // 从元数据区结束后开始遍历

    for (size_t i = 0; i < index; ++i) {
      // 读取当前元组的长度（匹配Tuple::SerializeTo写入的int32_t）
      int32_t tuple_data_size = *reinterpret_cast<const int32_t *>(page_data + current_offset);
      current_offset += 4 + tuple_data_size;
      // 防止页数据损坏导致的越界访问
      if (current_offset >= next_offset_ || current_offset >= page_size_) {
        return false;
      }
    }

    // 3. 反序列化目标元组（调用Tuple::DeserializeFrom）
    result_tuple.DeserializeFrom(page_data + current_offset);
    return true;
  }

  // ========================== 原有接口兼容与辅助方法 ==========================
  /**
   * 获取页内已存储的元组数量
   */
  size_t get_tuple_count() const { return tuple_count_; }

  /**
   * 获取下一个元组的写入偏移
   */
  size_t get_next_offset() const { return next_offset_; }

  /**
   * 检查指定偏移是否超出页大小
   */
  bool is_over(size_t offset) const { return offset >= page_size_; }
};

}  // namespace bustub