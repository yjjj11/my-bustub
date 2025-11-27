//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard_test.cpp
//
// 标识：test/storage/page_guard_test.cpp
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库组
//
//===----------------------------------------------------------------------===//

#include <cstdio>

#include "buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/page/page_guard.h"

#include "gtest/gtest.h"

namespace bustub {

const size_t FRAMES = 10;  // 缓冲池的帧数量

// 测试 PageGuard 的 Drop 功能（释放资源）
TEST(PageGuardTest, DropTest) {
  // 创建内存模拟磁盘管理器（无需实际磁盘I/O）
  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  // 创建缓冲池管理器（10个帧，关联内存磁盘管理器）
  auto bpm = std::make_shared<BufferPoolManager>(FRAMES, disk_manager.get());
  std::cout << "创建缓冲池管理器成功\n";

  {
    // 创建一个新页，获取页ID
    const auto pid0 = bpm->NewPage();
    std::cout << "成功获取页ID : pid== " << pid0 << "\n";
    // 获取该页的写保护（WritePageGuard）
    auto page0 = bpm->WritePage(pid0);
    std::cout << "成功获取page0的写保护\n";

    // 验证页的pin计数应为1（被写保护持有）
    ASSERT_EQ(1, bpm->GetPinCount(pid0));
    std::cout << "page0的pin计数 = 1\n";

    // 调用Drop应解除页的pin
    page0.Drop();
    ASSERT_EQ(0, bpm->GetPinCount(pid0));
    std::cout << "page0的pin计数 = 0\n";

    // 再次调用Drop应无效果
    page0.Drop();
    ASSERT_EQ(0, bpm->GetPinCount(pid0));
    std::cout << "page0的pin计数 = 0\n";
  }  // 离开作用域，PageGuard析构函数会被调用（虽已Drop但不影响）

  // 再创建两个新页用于后续测试
  const auto pid1 = bpm->NewPage();
  const auto pid2 = bpm->NewPage();

  {
    // 获取pid1的读保护、pid2的写保护
    auto read_guarded_page = bpm->ReadPage(pid1);
    auto write_guarded_page = bpm->WritePage(pid2);

    // 验证两个页的pin计数均为1
    ASSERT_EQ(1, bpm->GetPinCount(pid1));
    ASSERT_EQ(1, bpm->GetPinCount(pid2));

    // Drop操作应解除两个页的pin
    read_guarded_page.Drop();
    write_guarded_page.Drop();
    ASSERT_EQ(0, bpm->GetPinCount(pid1));
    ASSERT_EQ(0, bpm->GetPinCount(pid2));

    // 再次Drop无效果
    read_guarded_page.Drop();
    write_guarded_page.Drop();
    ASSERT_EQ(0, bpm->GetPinCount(pid1));
    ASSERT_EQ(0, bpm->GetPinCount(pid2));
  }  // 离开作用域，析构函数被调用（无副作用）

  // 若析构函数未正确解锁，此处会阻塞（验证锁释放逻辑）
  {
    const auto write_test1 = bpm->WritePage(pid1);
    const auto write_test2 = bpm->WritePage(pid2);
  }
  std::cout << "析构函数正确解锁了\n";
  std::vector<page_id_t> page_ids;  // 存储创建的页ID，用于后续验证
  {
    // 填满缓冲池（创建10个页，刚好占满所有帧）
    std::vector<WritePageGuard> guards;
    for (size_t i = 0; i < FRAMES; i++) {
      const auto new_pid = bpm->NewPage();
      guards.push_back(bpm->WritePage(new_pid));
      // 验证每个新页的pin计数均为1（被写保护持有）
      ASSERT_EQ(1, bpm->GetPinCount(new_pid));
      page_ids.push_back(new_pid);
    }
  }  // 离开作用域，所有WritePageGuard被销毁（Drop被调用）
  std::cout << "内存被填满过了\n";
  // 验证所有页的pin计数均已变为0
  for (size_t i = 0; i < FRAMES; i++) {
    ASSERT_EQ(0, bpm->GetPinCount(page_ids[i]));
  }
  std::cout << "计数全部变成了0\n";

  // 创建一个可修改的页，写入数据后释放，后续验证数据持久性
  const auto mutable_page_id = bpm->NewPage();
  auto mutable_guard = bpm->WritePage(mutable_page_id);
  strcpy(mutable_guard.GetDataMut(), "data");  // NOLINT（写入测试数据）
  std::cout << mutable_guard.GetDataMut() << "\n";
  std::cout << mutable_guard.IsDirty() << "\n";
  mutable_guard.Drop();  // 释放写保护
  std::cout << "刚写入就释放了\n";
  auto immutable_guard = bpm->ReadPage(mutable_page_id);
  std::cout << "引入成功了\n";
  immutable_guard.Drop();

  {
    // 再次填满缓冲池（可能会驱逐之前的mutable_page_id对应的页）
    std::vector<WritePageGuard> guards;
    for (size_t i = 0; i < FRAMES; i++) {
      auto new_pid = bpm->NewPage();
      guards.push_back(bpm->WritePage(new_pid));
      ASSERT_EQ(1, bpm->GetPinCount(new_pid));
    }
  }
  std::cout << "重新引入十个页并且析构\n";

  // 重新获取之前写入数据的页，验证数据是否正确（确保刷盘和加载逻辑正常）
  immutable_guard = bpm->ReadPage(mutable_page_id);
  std::cout << "引入成功了\n";
  ASSERT_EQ(0, std::strcmp("data", immutable_guard.GetData()));

  // 关闭磁盘管理器（清理资源）
  disk_manager->ShutDown();
}

// 测试 PageGuard 的移动语义（移动构造/移动赋值）
TEST(PageGuardTest, MoveTest) {
  // 创建内存模拟磁盘管理器和缓冲池管理器
  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(FRAMES, disk_manager.get());

  // 创建6个测试用的新页
  const auto pid0 = bpm->NewPage();
  const auto pid1 = bpm->NewPage();
  const auto pid2 = bpm->NewPage();
  const auto pid3 = bpm->NewPage();
  const auto pid4 = bpm->NewPage();
  const auto pid5 = bpm->NewPage();

  // 测试ReadPageGuard的移动语义
  auto guard0 = bpm->ReadPage(pid0);
  auto guard1 = bpm->ReadPage(pid1);
  // 初始pin计数均为1
  ASSERT_EQ(1, bpm->GetPinCount(pid0));
  ASSERT_EQ(1, bpm->GetPinCount(pid1));

  // 自移动赋值（无意义，但应无副作用）
  auto &guard0_r = guard0;
  guard0 = std::move(guard0_r);
  ASSERT_EQ(1, bpm->GetPinCount(pid0));

  // 移动赋值：将guard1移动给guard0，原guard0失效
  guard0 = std::move(guard1);
  ASSERT_EQ(0, bpm->GetPinCount(pid0));  // 原guard0释放pid0的pin
  ASSERT_EQ(1, bpm->GetPinCount(pid1));  // guard0现在持有pid1的pin

  // 移动构造：将guard0移动给guard0a，原guard0失效
  auto guard0a(std::move(guard0));
  ASSERT_EQ(0, bpm->GetPinCount(pid0));
  ASSERT_EQ(1, bpm->GetPinCount(pid1));

  // 再次测试另一组ReadPageGuard的移动语义
  auto guard2 = bpm->ReadPage(pid2);
  auto guard3 = bpm->ReadPage(pid3);
  ASSERT_EQ(1, bpm->GetPinCount(pid2));
  ASSERT_EQ(1, bpm->GetPinCount(pid3));

  // 自移动赋值（无副作用）
  auto &guard2_r = guard2;
  guard2 = std::move(guard2_r);
  ASSERT_EQ(1, bpm->GetPinCount(pid2));

  // 移动赋值：guard2接收guard3的资源，原guard2失效
  guard2 = std::move(guard3);
  ASSERT_EQ(0, bpm->GetPinCount(pid2));
  ASSERT_EQ(1, bpm->GetPinCount(pid3));

  // 移动构造：guard2a接收guard2的资源，原guard2失效
  auto guard2a(std::move(guard2));
  ASSERT_EQ(0, bpm->GetPinCount(pid2));
  ASSERT_EQ(1, bpm->GetPinCount(pid3));

  // 验证pid2的锁已释放（若未释放，此处会阻塞）
  { const auto temp_guard2 = bpm->WritePage(pid2); }

  // 测试WritePageGuard的移动语义
  auto guard4 = bpm->WritePage(pid4);
  auto guard5 = bpm->WritePage(pid5);
  ASSERT_EQ(1, bpm->GetPinCount(pid4));
  ASSERT_EQ(1, bpm->GetPinCount(pid5));

  // 自移动赋值（无副作用）
  auto &guard4_r = guard4;
  guard4 = std::move(guard4_r);
  ASSERT_EQ(1, bpm->GetPinCount(pid4));

  // 移动赋值：guard4接收guard5的资源，原guard4失效
  guard4 = std::move(guard5);
  ASSERT_EQ(0, bpm->GetPinCount(pid4));
  ASSERT_EQ(1, bpm->GetPinCount(pid5));

  // 移动构造：guard4a接收guard4的资源，原guard4失效
  auto guard4a(std::move(guard4));
  ASSERT_EQ(0, bpm->GetPinCount(pid4));
  ASSERT_EQ(1, bpm->GetPinCount(pid5));

  // 验证pid4的锁已释放（若未释放，此处会阻塞）
  { const auto temp_guard4 = bpm->ReadPage(pid4); }

  // 测试无效PageGuard的移动构造（无资源可转移）
  {
    ReadPageGuard invalidread0;
    const auto invalidread1{std::move(invalidread0)};
    WritePageGuard invalidwrite0;
    const auto invalidwrite1{std::move(invalidwrite0)};
  }

  // 测试向有效PageGuard移动赋值无效PageGuard（释放原资源）
  {
    const auto pid = bpm->NewPage();
    auto read = bpm->ReadPage(pid);
    ReadPageGuard invalidread;
    read = std::move(invalidread);  // read释放pid的资源，变为无效
    auto write = bpm->WritePage(pid);
    WritePageGuard invalidwrite;
    write = std::move(invalidwrite);  // write释放pid的资源，变为无效
  }

  // 关闭磁盘管理器（清理资源）
  disk_manager->ShutDown();
}

}  // namespace bustub