//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// count_min_sketch.cpp
//
// Identification: src/primer/count_min_sketch.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/count_min_sketch.h"
#include <algorithm>
#include <limits>
#include <stdexcept>
#include <string>

namespace bustub {

template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(uint32_t width, uint32_t depth) : width_(width), depth_(depth) {
  if (width == 0 || depth == 0) {
    throw std::invalid_argument("Width and depth must be positive.");
    return;
  }

  table_.resize(depth_);
  for (size_t i = 0; i < depth_; ++i) {
    // 为第 i 行分配 width_ 个 atomic<uint32_t>
    table_[i] = std::make_unique<std::atomic<uint32_t>[]>(width_);
    // 初始化每行的每个元素为 0
    for (size_t j = 0; j < width_; ++j) {
      table_[i][j].store(0, std::memory_order_relaxed);
    }
  }

  hash_functions_.reserve(depth_);
  for (size_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(this->HashFunction(i));
  }
}

template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(CountMinSketch &&other) noexcept
    : width_(other.width_), depth_(other.depth_), table_(std::move(other.table_)) {
  // 将源对象置于有效状态
  hash_functions_.clear();
  hash_functions_.reserve(depth_);
  for (size_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(this->HashFunction(i));
  }
  other.width_ = 0;
  other.depth_ = 0;
  other.hash_functions_.clear();
}

template <typename KeyType>
auto CountMinSketch<KeyType>::operator=(CountMinSketch &&other) noexcept -> CountMinSketch & {
  if (this != &other) {
    // 释放当前对象的资源（table_ 是 vector，会自动释放）
    // 移动赋值
    width_ = other.width_;
    depth_ = other.depth_;
    table_.clear();
    hash_functions_.clear();
    hash_functions_.reserve(depth_);
    for (size_t i = 0; i < depth_; i++) {
      hash_functions_.push_back(this->HashFunction(i));
    }

    table_ = std::move(other.table_);
    // 重置源对象
    other.width_ = 0;
    other.depth_ = 0;
    other.hash_functions_.clear();
  }
  return *this;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Insert(const KeyType &item) {
  // 无锁实现，使用原子操作
  for (size_t i = 0; i < depth_; i++) {
    // 修正拼写错误
    size_t pos = hash_functions_[i](item) % width_;
    table_[i][pos].fetch_add(1, std::memory_order_relaxed);
  }
}

template <typename KeyType>
void CountMinSketch<KeyType>::Merge(const CountMinSketch<KeyType> &other) {
  if (width_ != other.width_ || depth_ != other.depth_) {
    throw std::invalid_argument("Incompatible CountMinSketch dimensions for merge.");
  }

  for (size_t i = 0; i < depth_; i++) {
    for (size_t j = 0; j < width_; j++) {
      table_[i][j].fetch_add(other.table_[i][j].load(std::memory_order_relaxed));
    }
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::Count(const KeyType &item) const -> uint32_t {
  uint32_t min_count = std::numeric_limits<uint32_t>::max();
  for (size_t i = 0; i < depth_; i++) {
    // 修正拼写错误
    size_t pos = hash_functions_[i](item) % width_;
    uint32_t count = table_[i][pos].load(std::memory_order_relaxed);
    if (count < min_count) {
      min_count = count;
    }
  }
  return min_count;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Clear() {
  for (size_t i = 0; i < depth_; i++) {
    for (size_t j = 0; j < width_; j++) {
      table_[i][j].store(0, std::memory_order_relaxed);
    }
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::TopK(uint16_t k, const std::vector<KeyType> &candidates)
    -> std::vector<std::pair<KeyType, uint32_t>> {
  std::vector<std::pair<KeyType, uint32_t>> candidate_counts;

  // 为每个候选元素计算计数
  for (const auto &candidate : candidates) {
    uint32_t count = Count(candidate);  // 直接使用 Count 方法
    candidate_counts.emplace_back(candidate, count);
  }

  // 按计数降序排序
  std::sort(candidate_counts.begin(), candidate_counts.end(),
            [](const std::pair<KeyType, uint32_t> &a, const std::pair<KeyType, uint32_t> &b) {
              return a.second > b.second;  // 降序排列
            });

  // 返回前 k 个元素
  if (k > candidate_counts.size()) {
    k = candidate_counts.size();
  }

  return std::vector<std::pair<KeyType, uint32_t>>(candidate_counts.begin(), candidate_counts.begin() + k);
}

// Explicit instantiations for all types used in tests
template class CountMinSketch<std::string>;
template class CountMinSketch<int64_t>;  // For int64_t tests
template class CountMinSketch<int>;      // This covers both int and int32_t
}  // namespace bustub