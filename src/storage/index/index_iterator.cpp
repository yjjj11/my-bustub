#include "storage/index/index_iterator.h"
#include <cassert>

namespace bustub {

// 实现3参数构造函数（原有逻辑）
FULL_INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(TracedBufferPoolManager *bpm, page_id_t leaf_page_id, int index)
    : bpm_(bpm), leaf_page_id_(leaf_page_id), index_(index) {}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return leaf_page_id_ == INVALID_PAGE_ID; }

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> std::pair<const KeyType, const ValueType> {
  assert(!IsEnd() && "迭代器已指向尾后位置，无法解引用");
  ReadPageGuard guard = bpm_->ReadPage(leaf_page_id_);
  auto leaf_page = guard.As<LeafPage>();  // 现在 LeafPage 已定义
  return {leaf_page->KeyAt(index_), leaf_page->ValueAt(index_)};
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> IndexIterator & {
  assert(!IsEnd() && "迭代器已指向尾后位置，无法递增");
  ReadPageGuard guard = bpm_->ReadPage(leaf_page_id_);
  auto leaf_page = guard.As<LeafPage>();
  index_++;

  if (index_ >= leaf_page->GetSize()) {
    page_id_t next_page_id = leaf_page->GetNextPageId();

    if (next_page_id != INVALID_PAGE_ID) {
      ReadPageGuard next_guard = bpm_->ReadPage(next_page_id);  // 直接调用 TracedBufferPoolManager 的方法
      guard.Drop();
      guard = std::move(next_guard);
      leaf_page_id_ = next_page_id;
      index_ = 0;
    } else {
      leaf_page_id_ = INVALID_PAGE_ID;
      index_ = 0;
    }
  }

  return *this;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
  return leaf_page_id_ == itr.leaf_page_id_ && index_ == itr.index_;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool { return !(*this == itr); }

// 显式实例化（删除负数 NumTombs，避免逻辑错误）
template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;
template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;
template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, -1>;

}  // namespace bustub
