#include "index/index_iterator.h"

#include "index/basic_comparator.h"
#include "index/generic_key.h"

 IndexIterator::IndexIterator()
         : current_page_id(INVALID_PAGE_ID),
           item_index(0),
           buffer_pool_manager(nullptr),
           page(nullptr) {}

IndexIterator::IndexIterator(page_id_t page_id, BufferPoolManager *bpm, int index)
    : current_page_id(page_id), item_index(index), buffer_pool_manager(bpm) {
    if (page_id != INVALID_PAGE_ID) {
        auto *frame = buffer_pool_manager->FetchPage(page_id);
        page = reinterpret_cast<LeafPage *>(frame->GetData());
    }
  page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(current_page_id)->GetData());
}

IndexIterator::~IndexIterator() {
  if (current_page_id != INVALID_PAGE_ID)
    buffer_pool_manager->UnpinPage(current_page_id, false);
}

/**
 * TODO: Student Implement
 */
std::pair<GenericKey *, RowId> IndexIterator::operator*() {
    // 返回当前 leaf 页中 item_index 处的 <key, value>
    assert(page != nullptr);
    return page->GetItem(item_index);
}

/**
 * TODO: Student Implement
 */
IndexIterator &IndexIterator::operator++() {
    // 先尝试在当前页中下一个位置移动
    if (page != nullptr && item_index + 1 < page->GetSize()) {
        item_index++;
        return *this;
    }

    // 当前页已经遍历完，跳到下一叶子页
    page_id_t next_page_id = INVALID_PAGE_ID;
    if (page != nullptr) {
        next_page_id = page->GetNextPageId();
        // unpin 掉当前页
        buffer_pool_manager->UnpinPage(current_page_id, /*is_dirty=*/false);
    }

    if (next_page_id == INVALID_PAGE_ID) {
        // 到了末尾
        current_page_id = INVALID_PAGE_ID;
        page = nullptr;
        item_index = 0;
    } else {
        // fetch & pin 下一页
        current_page_id = next_page_id;
        Page *p = buffer_pool_manager->FetchPage(current_page_id);
        page = reinterpret_cast<LeafPage *>(p->GetData());
        item_index = 0;
    }

    return *this;
}

bool IndexIterator::operator==(const IndexIterator &itr) const {
  return current_page_id == itr.current_page_id && item_index == itr.item_index;
}

bool IndexIterator::operator!=(const IndexIterator &itr) const {
  return !(*this == itr);
}