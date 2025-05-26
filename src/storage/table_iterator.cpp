#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn)
        : table_heap_(table_heap), rid_(rid), txn_(txn) {
    // 如果是合法的开始迭代位置，就把这一行 load 进 cur_row_
    if (table_heap_ != nullptr && rid_.GetPageId() != INVALID_PAGE_ID) {
        cur_row_ = Row();
        // —— 先给 cur_row_ 设上正确的 RowId ——
        cur_row_.SetRowId(rid_);
        auto page = reinterpret_cast<TablePage *>(
                table_heap_->buffer_pool_manager_->FetchPage(rid_.GetPageId()));
        // // std::cout << "TableIterator::TableIterator: rid_ = " << rid_.GetPageId() << std::endl;
        if (page != nullptr) {
            page->RLatch();
            bool ok = page->GetTuple(&cur_row_, table_heap_->schema_, txn_,
                                     table_heap_->lock_manager_);
            page->RUnlatch();
            table_heap_->buffer_pool_manager_->UnpinPage(rid_.GetPageId(), false);
            if (!ok) {
                // 读不到就认为是 end()
                table_heap_ = nullptr;
            }
        } else {
            table_heap_ = nullptr;
        }
    }
}

TableIterator::TableIterator(const TableIterator &other) {
    // 复制构造函数
    table_heap_ = other.table_heap_;
    rid_ = other.rid_;
    txn_ = other.txn_;
    cur_row_ = other.cur_row_;
    // 这里不需要复制 table_heap_ 的数据，因为它是一个指针
}

// 析构函数：默认
TableIterator::~TableIterator() = default;

bool TableIterator::operator==(const TableIterator &itr) const {
    // 两个 end() 或者同表同位置 就相等
    if (table_heap_ == nullptr && itr.table_heap_ == nullptr) return true;
    return table_heap_ == itr.table_heap_ && rid_ == itr.rid_;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(*this == itr);
}

const Row &TableIterator::operator*() {
    // 如果是合法的开始迭代位置，就把这一行 load 进 cur_row_
    if (table_heap_ != nullptr && rid_.GetPageId() != INVALID_PAGE_ID) {
        return cur_row_;
    }
    // 否则返回空行
    return Row();
}

Row *TableIterator::operator->() {
    // 如果是合法的开始迭代位置，就把这一行 load 进 cur_row_
    if (table_heap_ != nullptr && rid_.GetPageId() != INVALID_PAGE_ID) {
        return &cur_row_;
    }
    // 否则返回空行
    return nullptr;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
    if (this != &itr) {
        table_heap_ = itr.table_heap_;
        rid_ = itr.rid_;
        txn_ = itr.txn_;
        cur_row_ = itr.cur_row_;
    }
    return *this;
}

// ++iter
// storage/table_iterator.cpp

TableIterator &TableIterator::operator++() {
    // 如果已经是 end()，直接返回
    if (table_heap_ == nullptr) {
        return *this;
    }

    // 1) 本页内尝试找下一个 slot
    RowId next_rid;
    bool found = false;
    auto page = reinterpret_cast<TablePage *>(
            table_heap_->buffer_pool_manager_->FetchPage(rid_.GetPageId()));
    if (page) {
        page->RLatch();
        found = page->GetNextTupleRid(rid_, &next_rid);
        page->RUnlatch();
        table_heap_->buffer_pool_manager_->UnpinPage(rid_.GetPageId(), false);
    }

    // 2) 如果本页没找到，沿 “next page” 链找第一个非删除 tuple
    if (!found) {
        page_id_t pid = page ? page->GetNextPageId() : INVALID_PAGE_ID;
        while (pid != INVALID_PAGE_ID) {
            auto p = reinterpret_cast<TablePage *>(
                    table_heap_->buffer_pool_manager_->FetchPage(pid));
            if (!p) {
                break;
            }
            RowId first_rid;
            p->RLatch();
            bool ok = p->GetFirstTupleRid(&first_rid);
            p->RUnlatch();
            table_heap_->buffer_pool_manager_->UnpinPage(pid, false);
            if (ok) {
                next_rid = first_rid;
                found    = true;
                break;
            }
            pid = p->GetNextPageId();
        }
    }

    // 3) 如果确实没找到，下沉到 end()
    if (!found) {
        table_heap_ = nullptr;
        rid_        = RowId();   // 默认就是 INVALID_PAGE_ID, 0
        txn_        = nullptr;
        return *this;
    }

    // 4) 找到了 next_rid，再去加载那一行
    rid_     = next_rid;
    cur_row_ = Row();
    cur_row_.SetRowId(rid_);
    auto np = reinterpret_cast<TablePage *>(
            table_heap_->buffer_pool_manager_->FetchPage(rid_.GetPageId()));
    if (np) {
        np->RLatch();
        np->GetTuple(&cur_row_, table_heap_->schema_, txn_, table_heap_->lock_manager_);
        np->RUnlatch();
        table_heap_->buffer_pool_manager_->UnpinPage(rid_.GetPageId(), false);
    }
    return *this;
}


// iter++
TableIterator TableIterator::operator++(int) {
    TableIterator old(*this);
    ++(*this);  // 调用前置 ++
    return old;
}

