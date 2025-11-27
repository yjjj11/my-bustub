//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// arc_replacer_performance_test.cpp
//
// 标识：test/buffer/arc_replacer_performance_test.cpp
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库研究组
//
//===----------------------------------------------------------------------===//

/**
 * arc_replacer_performance_test.cpp
 */

#include <thread>  // NOLINT
#include "buffer/arc_replacer.h"

#include "gtest/gtest.h"

namespace bustub {

TEST(ArcReplacerPerformanceTest, RecordAccessPerformanceTest) {
  // 测试 RecordAccess 方法的性能
  std::cout << "开始" << std::endl;
  std::cout << "本测试将验证当列表很大时，你的 RecordAccess 方法性能如何。" << std::endl;
  std::cout << "如果平均耗时超过 3 秒，在后续的一些项目中你可能难以获得满分..." << std::endl;
  std::cout << "如果在意这个问题，你可能需要思考：当列表非常大时，哪些操作可能很慢，以及如何加快这些操作。"
            << std::endl;
  const size_t bpm_size = 256 << 10;  // 1GB
  ArcReplacer arc_replacer(bpm_size);
  // 用大量页填充 mfu 列表
  for (size_t i = 0; i < bpm_size; i++) {
    arc_replacer.RecordAccess(i, i);
    arc_replacer.SetEvictable(i, true);
  }
  // 持续访问列表中间的页
  std::vector<std::thread> threads;
  const size_t rounds = 10;
  size_t access_frame_id = 256 << 9;
  std::vector<size_t> access_times;
  for (size_t round = 0; round < rounds; round++) {
    auto start_time = std::chrono::system_clock::now();
    for (size_t i = 0; i < bpm_size; i++) {
      arc_replacer.RecordAccess(access_frame_id, access_frame_id);
      access_frame_id = (access_frame_id + 1) % bpm_size;
    }
    auto end_time = std::chrono::system_clock::now();
    access_times.push_back(std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count());
  }
  double total = 0;
  for (const auto &x : access_times) {
    total += x;
  }
  total /= 1000;
  double avg = total / access_times.size();
  std::cout << "结束" << std::endl;
  std::cout << "平均耗时：" << avg << "秒。" << std::endl;
  std::cout << "如果平均耗时超过 3 秒，在后续的一些项目中你可能难以获得满分..." << std::endl;
  std::cout << "如果在意这个问题，可以尝试优化一下 RecordAccess 方法。" << std::endl;
  ASSERT_LT(avg, 3);
}

}  // namespace bustub