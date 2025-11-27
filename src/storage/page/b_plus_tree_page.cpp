//===----------------------------------------------------------------------===//
//
//                         BusTub（巴士桶）
//
// b_plus_tree_page.cpp
//
// 标识：src/storage/page/b_plus_tree_page.cpp
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库组
//
//===----------------------------------------------------------------------===//

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*
 * 用于获取/设置页类型的辅助方法
 * 页类型枚举类定义在 b_plus_tree_page.h 中
 */
auto BPlusTreePage::IsLeafPage() const -> bool {
  if (page_type_ == IndexPageType::LEAF_PAGE)
    return true;
  else
    return false;
}
void BPlusTreePage::SetPageType(IndexPageType page_type) { page_type_ = page_type; }

/*
 * 用于获取/设置页大小（该页中存储的键值对数量）的辅助方法
 */
auto BPlusTreePage::GetSize() const -> int { return size_; }
void BPlusTreePage::ChangeSizeBy(int amount) { size_ += amount; }

/*
 * 用于获取/设置页最大容量的辅助方法
 */
auto BPlusTreePage::GetMaxSize() const -> int { return max_size_; }
void BPlusTreePage::SetMaxSize(int size) { max_size_ = size; }

/*
 * 获取页最小容量的辅助方法
 * 通常，最小容量 == 最大容量 / 2
 * 具体是向上取整还是向下取整取决于你的实现
 */
auto BPlusTreePage::GetMinSize() const -> int { return max_size_ / 2; }

}  // namespace bustub