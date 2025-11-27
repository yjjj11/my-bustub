//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// check_options.h
//
// 标识：src/include/execution/check_options.h
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库组
//
//===----------------------------------------------------------------------===//

// Copyright 2022 RisingLight Project Authors
//
// 根据Apache许可证2.0版（以下简称"许可证"）授权；
// 除非遵守许可证的规定，否则您不得使用本文件。
// 您可以在以下位置获取许可证的副本：
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// 除非适用法律要求或书面同意，否则根据许可证分发的软件
// 是按"原样"分发的，不附带任何明示或暗示的担保或条件。
// 请参阅许可证以了解管理权限和限制的具体条款。

#include <memory>
#include <unordered_set>

#pragma once

namespace bustub {

/**
 * 检查选项枚举，用于指定需要启用的检查类型
 */
enum class CheckOption : uint8_t {
  ENABLE_NLJ_CHECK = 0,   // 启用嵌套循环连接检查
  ENABLE_TOPN_CHECK = 1,  // 启用TopN操作检查
};

/**
 * CheckOptions类包含一组用于测试执行器逻辑的检查选项
 */
class CheckOptions {
 public:
  std::unordered_set<CheckOption> check_options_set_;  // 存储已启用的检查选项集合
};

};  // namespace bustub