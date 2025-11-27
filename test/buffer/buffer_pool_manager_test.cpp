//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_test.cpp
//
// Identification: test/buffer/buffer_pool_manager_test.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <filesystem>

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/page/page_guard.h"

namespace bustub {

static std::filesystem::path db_fname("test.bustub");

// The number of frames we give to the buffer pool.
const size_t FRAMES = 10;

void CopyString(char *dest, const std::string &src) {
  BUSTUB_ENSURE(src.length() + 1 <= BUSTUB_PAGE_SIZE, "CopyString src too long");
  snprintf(dest, BUSTUB_PAGE_SIZE, "%s", src.c_str());
}

TEST(BufferPoolManagerTest, VeryBasicTest) {
  // A very basic test.

  auto disk_manager = std::make_shared<DiskManager>(db_fname);
  auto bpm = std::make_shared<BufferPoolManager>(FRAMES, disk_manager.get());

  const page_id_t pid = bpm->NewPage();
  const std::string str = "Hello, world!";

  // Check `WritePageGuard` basic functionality.
  {
    auto guard = bpm->WritePage(pid);
    CopyString(guard.GetDataMut(), str);
    EXPECT_STREQ(guard.GetData(), str.c_str());
  }

  // Check `ReadPageGuard` basic functionality.
  {
    const auto guard = bpm->ReadPage(pid);
    EXPECT_STREQ(guard.GetData(), str.c_str());
  }

  // Check `ReadPageGuard` basic functionality (again).
  {
    const auto guard = bpm->ReadPage(pid);
    EXPECT_STREQ(guard.GetData(), str.c_str());
  }

  ASSERT_TRUE(bpm->DeletePage(pid));
}

TEST(BufferPoolManagerTest, PagePinEasyTest) {
  auto disk_manager = std::make_shared<DiskManager>(db_fname);
  auto bpm = std::make_shared<BufferPoolManager>(2, disk_manager.get());
  const page_id_t pageid0 = bpm->NewPage();
  const page_id_t pageid1 = bpm->NewPage();

  const std::string str0 = "page0";
  const std::string str1 = "page1";
  const std::string str0updated = "page0updated";
  const std::string str1updated = "page1updated";

  {
    auto page0_write_opt = bpm->CheckedWritePage(pageid0);
    std::cout << "获取page0 的写保护\n";
    ASSERT_TRUE(page0_write_opt.has_value());
    auto page0_write = std::move(page0_write_opt.value());  // NOLINT
    CopyString(page0_write.GetDataMut(), str0);

    std::cout << "获取写保护成功，并修改页面0\n";
    auto page1_write_opt = bpm->CheckedWritePage(pageid1);
    ASSERT_TRUE(page1_write_opt.has_value());
    auto page1_write = std::move(page1_write_opt.value());  // NOLINT
    CopyString(page1_write.GetDataMut(), str1);
    EXPECT_STREQ(page0_write.GetData(), str0.c_str());
    std::cout << "获取写保护成功，并修改页面1\n";

    ASSERT_EQ(1, bpm->GetPinCount(pageid0));
    ASSERT_EQ(1, bpm->GetPinCount(pageid1));
    std::cout << "pincount 通过 1\n";
    const auto temp_page_id1 = bpm->NewPage();
    const auto temp_page1_opt = bpm->CheckedReadPage(temp_page_id1);
    ASSERT_FALSE(temp_page1_opt.has_value());

    const auto temp_page_id2 = bpm->NewPage();
    const auto temp_page2_opt = bpm->CheckedWritePage(temp_page_id2);
    ASSERT_FALSE(temp_page2_opt.has_value());

    ASSERT_EQ(1, bpm->GetPinCount(pageid0));
    page0_write.Drop();
    ASSERT_EQ(0, bpm->GetPinCount(pageid0));
    std::cout << "page0合验通过\n";
    ASSERT_EQ(1, bpm->GetPinCount(pageid1));
    page1_write.Drop();
    ASSERT_EQ(0, bpm->GetPinCount(pageid1));
    std::cout << "page1合验通过\n";
  }

  {
    const auto temp_page_id1 = bpm->NewPage();
    const auto temp_page1_opt = bpm->CheckedReadPage(temp_page_id1);
    ASSERT_TRUE(temp_page1_opt.has_value());

    const auto temp_page_id2 = bpm->NewPage();
    const auto temp_page2_opt = bpm->CheckedWritePage(temp_page_id2);
    ASSERT_TRUE(temp_page2_opt.has_value());

    ASSERT_FALSE(bpm->GetPinCount(pageid0).has_value());
    ASSERT_FALSE(bpm->GetPinCount(pageid1).has_value());
    std::cout << "通过驱逐，原0,1pin为0\n";
  }

  {
    auto page0_write_opt = bpm->CheckedWritePage(pageid0);
    ASSERT_TRUE(page0_write_opt.has_value());
    auto page0_write = std::move(page0_write_opt.value());  // NOLINT
    EXPECT_STREQ(page0_write.GetData(), str0.c_str());
    CopyString(page0_write.GetDataMut(), str0updated);
    std::cout << "修改page0成功\n";

    auto page1_write_opt = bpm->CheckedWritePage(pageid1);
    ASSERT_TRUE(page1_write_opt.has_value());
    auto page1_write = std::move(page1_write_opt.value());  // NOLINT
    EXPECT_STREQ(page1_write.GetData(), str1.c_str());
    CopyString(page1_write.GetDataMut(), str1updated);
    std::cout << "修改page1成功\n";

    ASSERT_EQ(1, bpm->GetPinCount(pageid0));
    ASSERT_EQ(1, bpm->GetPinCount(pageid1));
    std::cout << "pincount 通过 1\n";
  }

  ASSERT_EQ(0, bpm->GetPinCount(pageid0));
  ASSERT_EQ(0, bpm->GetPinCount(pageid1));
  std::cout << "pincount 通过 0\n";

  {
    auto page0_read_opt = bpm->CheckedReadPage(pageid0);
    ASSERT_TRUE(page0_read_opt.has_value());
    const auto page0_read = std::move(page0_read_opt.value());  // NOLINT
    EXPECT_STREQ(page0_read.GetData(), str0updated.c_str());

    auto page1_read_opt = bpm->CheckedReadPage(pageid1);
    ASSERT_TRUE(page1_read_opt.has_value());
    const auto page1_read = std::move(page1_read_opt.value());  // NOLINT
    EXPECT_STREQ(page1_read.GetData(), str1updated.c_str());

    ASSERT_EQ(1, bpm->GetPinCount(pageid0));
    ASSERT_EQ(1, bpm->GetPinCount(pageid1));
  }

  ASSERT_EQ(0, bpm->GetPinCount(pageid0));
  ASSERT_EQ(0, bpm->GetPinCount(pageid1));

  remove(db_fname);
  remove(disk_manager->GetLogFileName());
}

TEST(BufferPoolManagerTest, PagePinMediumTest) {
  auto disk_manager = std::make_shared<DiskManager>(db_fname);
  auto bpm = std::make_shared<BufferPoolManager>(FRAMES, disk_manager.get());

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  const auto pid0 = bpm->NewPage();
  auto page0 = bpm->WritePage(pid0);

  // Scenario: Once we have a page, we should be able to read and write content.
  const std::string hello = "Hello";
  CopyString(page0.GetDataMut(), hello);
  EXPECT_STREQ(page0.GetData(), hello.c_str());

  page0.Drop();

  // Create a vector of unique pointers to page guards, which prevents the guards from getting destructed.
  std::vector<WritePageGuard> pages;

  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  for (size_t i = 0; i < FRAMES; i++) {
    const auto pid = bpm->NewPage();
    auto page = bpm->WritePage(pid);
    pages.push_back(std::move(page));
  }

  // Scenario: All of the pin counts should be 1.
  for (const auto &page : pages) {
    const auto pid = page.GetPageId();
    EXPECT_EQ(1, bpm->GetPinCount(pid));
  }

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = 0; i < FRAMES; i++) {
    const auto pid = bpm->NewPage();
    const auto fail = bpm->CheckedWritePage(pid);
    ASSERT_FALSE(fail.has_value());
  }

  // Scenario: Drop the first 5 pages to unpin them.
  for (size_t i = 0; i < FRAMES / 2; i++) {
    const auto pid = pages[0].GetPageId();
    EXPECT_EQ(1, bpm->GetPinCount(pid));
    pages.erase(pages.begin());
    EXPECT_EQ(0, bpm->GetPinCount(pid));
  }

  // Scenario: All of the pin counts of the pages we haven't dropped yet should still be 1.
  for (const auto &page : pages) {
    const auto pid = page.GetPageId();
    EXPECT_EQ(1, bpm->GetPinCount(pid));
  }

  // Scenario: After unpinning pages {1, 2, 3, 4, 5}, we should be able to create 4 new pages and bring them into
  // memory. Bringing those 4 pages into memory should evict the first 4 pages {1, 2, 3, 4} because of LRU.
  for (size_t i = 0; i < ((FRAMES / 2) - 1); i++) {
    const auto pid = bpm->NewPage();
    auto page = bpm->WritePage(pid);
    pages.push_back(std::move(page));
  }

  // Scenario: There should be one frame available, and we should be able to fetch the data we wrote a while ago.
  {
    const auto original_page = bpm->ReadPage(pid0);
    EXPECT_STREQ(original_page.GetData(), hello.c_str());
  }

  // Scenario: Once we unpin page 0 and then make a new page, all the buffer pages should now be pinned. Fetching page 0
  // again should fail.
  const auto last_pid = bpm->NewPage();
  const auto last_page = bpm->ReadPage(last_pid);

  const auto fail = bpm->CheckedReadPage(pid0);
  ASSERT_FALSE(fail.has_value());

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove(db_fname);
}

TEST(BufferPoolManagerTest, PageAccessTest) {
  const size_t rounds = 50;  // 测试轮次：控制读写操作的总次数

  // 初始化磁盘管理器（关联测试文件）和缓冲池管理器（仅1个帧，强制触发读写竞争）
  auto disk_manager = std::make_shared<DiskManager>(db_fname);
  auto bpm = std::make_shared<BufferPoolManager>(1, disk_manager.get());

  // 创建一个测试页并获取其ID
  const auto pid = bpm->NewPage();
  char buf[BUSTUB_PAGE_SIZE];  // 用于暂存读取到的页内容，验证读取期间数据是否被修改

  // 启动写线程：循环向页中写入数据（模拟并发修改）
  auto thread = std::thread([&]() {
    // 写线程持续向同一页写入数据
    for (size_t i = 0; i < rounds; i++) {
      // 休眠5毫秒，模拟写操作前的准备工作，给读线程抢占机会
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
      // 获取页的写保护（独占锁），确保写入时的线程安全
      auto guard = bpm->WritePage(pid);
      // 向页中写入当前轮次编号（如"0"、"1"、...、"49"）
      CopyString(guard.GetDataMut(), std::to_string(i));
    }
  });

  // 主线程作为读线程：循环读取页内容，验证读期间数据的一致性
  for (size_t i = 0; i < rounds; i++) {
    // 休眠10毫秒，让写线程有机会先写入数据，制造读写交替的场景
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // 获取页的读保护（共享锁），确保读取时数据不被修改
    // 若锁机制正确，读取期间写线程会被阻塞
    const auto guard = bpm->ReadPage(pid);

    // 保存当前读取到的内容到临时缓冲区
    memcpy(buf, guard.GetData(), BUSTUB_PAGE_SIZE);

    // 持有读锁期间休眠10毫秒，模拟读取后的处理操作
    // 若锁机制有效，这段时间内写线程无法修改数据
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // 验证读取期间数据未被修改（锁机制生效）
    EXPECT_STREQ(guard.GetData(), buf);
  }

  // 等待写线程执行完毕
  thread.join();
}

TEST(BufferPoolManagerTest, ContentionTest) {
  // 初始化磁盘管理器和缓冲池管理器（10个帧，模拟多线程竞争场景）
  auto disk_manager = std::make_shared<DiskManager>(db_fname);
  auto bpm = std::make_shared<BufferPoolManager>(FRAMES, disk_manager.get());

  const size_t rounds = 100000;  // 每个线程的写操作次数（高并发压力测试）

  // 创建一个测试页，供所有线程竞争写入
  const auto pid = bpm->NewPage();

  // 启动4个写线程，同时对同一页进行高频写入（测试锁的并发安全性）
  auto thread1 = std::thread([&]() {
    for (size_t i = 0; i < rounds; i++) {
      // 获取写保护（独占锁），写入当前轮次编号
      auto guard = bpm->WritePage(pid);
      CopyString(guard.GetDataMut(), std::to_string(i));
    }
  });

  auto thread2 = std::thread([&]() {
    for (size_t i = 0; i < rounds; i++) {
      auto guard = bpm->WritePage(pid);
      CopyString(guard.GetDataMut(), std::to_string(i));
    }
  });

  auto thread3 = std::thread([&]() {
    for (size_t i = 0; i < rounds; i++) {
      auto guard = bpm->WritePage(pid);
      CopyString(guard.GetDataMut(), std::to_string(i));
    }
  });

  auto thread4 = std::thread([&]() {
    for (size_t i = 0; i < rounds; i++) {
      auto guard = bpm->WritePage(pid);
      CopyString(guard.GetDataMut(), std::to_string(i));
    }
  });

  // 等待所有线程执行完毕（验证无死锁、无数据竞争）
  thread3.join();
  thread2.join();
  thread4.join();
  thread1.join();
}

TEST(BufferPoolManagerTest, DeadlockTest) {
  // 初始化磁盘管理器和缓冲池管理器（10个帧，测试死锁避免机制）
  auto disk_manager = std::make_shared<DiskManager>(db_fname);
  auto bpm = std::make_shared<BufferPoolManager>(FRAMES, disk_manager.get());

  // 创建两个测试页，用于模拟锁顺序竞争
  const auto pid0 = bpm->NewPage();
  const auto pid1 = bpm->NewPage();

  // 主线程先获取page0的写保护（独占锁）
  auto guard0 = bpm->WritePage(pid0);

  // 简单的线程同步机制：确保子线程已启动后再继续测试
  std::atomic<bool> start = false;

  // 启动子线程：尝试获取page0的写保护（模拟锁竞争）
  auto child = std::thread([&]() {
    // 通知主线程：子线程已启动
    start.store(true);

    // 尝试写入page0（此时主线程持有page0的锁，子线程会阻塞等待）
    const auto guard0 = bpm->WritePage(pid0);
  });

  // 等待子线程启动（确保子线程已进入阻塞状态）
  while (!start.load()) {
  }

  // 让子线程等待1秒（模拟主线程持有page0锁进行耗时操作）
  // 此时子线程正在等待page0的锁
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));

  // 关键测试点：若锁机制实现不当（如使用全局大锁），此行代码会导致死锁
  // 思考：若持有全局锁且不释放，获取新锁时会阻塞所有线程
  // 正确的实现应使用页级细粒度锁，避免全局阻塞

  // 主线程在持有page0锁的同时，尝试获取page1的写保护
  const auto guard1 = bpm->WritePage(pid1);

  // 释放page0的锁，让子线程可以继续执行
  guard0.Drop();

  // 等待子线程执行完毕（若未死锁，则测试通过）
  child.join();
}

TEST(BufferPoolManagerTest, EvictableTest) {
  // 测试帧的可驱逐状态是否始终正确（DISABLEd表示默认不执行，需手动启用）
  const size_t rounds = 1000;    // 测试轮次
  const size_t num_readers = 8;  // 并发读线程数量

  // 初始化磁盘管理器和缓冲池管理器（仅1个帧，强制验证驱逐逻辑）
  auto disk_manager = std::make_shared<DiskManager>(db_fname);
  auto bpm = std::make_shared<BufferPoolManager>(1, disk_manager.get());

  for (size_t i = 0; i < rounds; i++) {
    std::mutex mutex;            // 用于线程同步的互斥锁
    std::condition_variable cv;  // 用于线程等待/通知的条件变量

    // 同步信号：主线程获取读锁后，通知所有读线程开始执行
    bool signal = false;

    // 创建两个测试页：
    // winner_pid：会被加载到唯一的帧中
    // loser_pid：尝试加载到已被占用的帧中（预期失败）
    const auto winner_pid = bpm->NewPage();
    const auto loser_pid = bpm->NewPage();

    // 创建多个读线程，验证帧被Pin时无法驱逐
    std::vector<std::thread> readers;
    for (size_t j = 0; j < num_readers; j++) {
      readers.emplace_back([&]() {
        std::unique_lock<std::mutex> lock(mutex);

        // 等待主线程的信号（确保主线程已获取读锁）
        while (!signal) {
          cv.wait(lock);
        }

        // 以共享模式读取winner_pid（此时帧被Pin）
        const auto read_guard = bpm->ReadPage(winner_pid);

        // 验证：唯一的帧被Pin，无法驱逐，因此loser_pid无法加载
        ASSERT_FALSE(bpm->CheckedReadPage(loser_pid).has_value());
      });
    }

    std::unique_lock<std::mutex> lock(mutex);

    if (i % 2 == 0) {
      // 偶数轮：主线程获取读锁并Pin住页
      auto read_guard = bpm->ReadPage(winner_pid);

      // 通知所有读线程可以开始执行
      signal = true;
      cv.notify_all();
      lock.unlock();

      // 释放读锁，允许其他线程访问
      read_guard.Drop();
    } else {
      // 奇数轮：主线程获取写锁并Pin住页
      auto write_guard = bpm->WritePage(winner_pid);

      // 通知所有读线程可以开始执行
      signal = true;
      cv.notify_all();
      lock.unlock();

      // 释放写锁，允许其他线程访问
      write_guard.Drop();
    }

    // 等待所有读线程执行完毕
    for (size_t i = 0; i < num_readers; i++) {
      readers[i].join();
    }
  }
}

}  // namespace bustub
