#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) {
    // 1. 尝试在已有页面中插
    page_id_t cur_pid = first_page_id_;
    page_id_t prev_pid = INVALID_PAGE_ID;  // 记录上一次的非空页
    while (cur_pid != INVALID_PAGE_ID) {
        auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(cur_pid));
        if (!page) return false;
        page->WLatch();
        bool inserted = page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(cur_pid, inserted);
        if (inserted) return true;

        // 记录当前页号，然后跳到下一个
        prev_pid = cur_pid;
        cur_pid = page->GetNextPageId();
    }

    // 2. 所有旧页都满了，prev_pid 正好是最后一页的页号
    page_id_t new_pid;
    Page *raw = buffer_pool_manager_->NewPage(new_pid);
    if (!raw) return false;
    auto new_page = reinterpret_cast<TablePage *>(raw);
    // 用 prev_pid 初始化新页
    new_page->Init(new_pid, prev_pid, log_manager_, txn);

    // 3. 如果 prev_pid 有效，就把它的 next 指向 new_pid
    if (prev_pid != INVALID_PAGE_ID) {
        auto prev_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(prev_pid));
        prev_page->WLatch();
        prev_page->SetNextPageId(new_pid);
        prev_page->WUnlatch();
        buffer_pool_manager_->UnpinPage(prev_pid, /*is_dirty=*/true);
    }

    // 4. 向新页插入
    new_page->WLatch();
    bool ok = new_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
    new_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(new_pid, ok);
    return ok;
}



bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(Row &new_row, const RowId &rid, Txn *txn) {
    page_id_t pid = rid.GetPageId();
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(pid));
    if (page == nullptr) {
        return false;
    }

    // —— 第一步，用 old_row 读取旧值，确保这个行存在 ——
    Row old_row;
    old_row.SetRowId(rid);
    page->WLatch();
    bool ok = page->GetTuple(&old_row, schema_, txn, lock_manager_);
    page->WUnlatch();
    // 如果读失败，unpin 并返回
    if (!ok) {
        buffer_pool_manager_->UnpinPage(pid, false);
        return false;
    }

    // —— 关键：要给 page->UpdateTuple 一个空 fields_ 的 Row ——
    Row fresh_row_for_update;
    fresh_row_for_update.SetRowId(rid);
    page->WLatch();
    ok = page->UpdateTuple(new_row, &fresh_row_for_update, schema_, txn, lock_manager_, log_manager_);
    page->WUnlatch();
    // 完成这次访问后 unpin
    buffer_pool_manager_->UnpinPage(pid, ok);

    if (!ok) {
        // 空间不足时：逻辑删除 + 插入
        MarkDelete(rid, txn);
        bool inserted = InsertTuple(new_row, txn);
        if (!inserted) {
            RollbackDelete(rid, txn);
        }
        return inserted;
    }

    // 更新成功，恢复 new_row 的 RowId
    new_row.SetRowId(rid);
    return true;
}



/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  // Step1: Find the page which contains the tuple.
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    if (page == nullptr) {
        return;
    }
    // Step2: Delete the tuple from the page.
    page->WLatch();
    page->ApplyDelete(rid, txn, log_manager_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Txn *txn) {
    // 先找到row对应的页
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
    if (page == nullptr) {
        return false;
    }
    // 然后从页中获取行
    page->RLatch();
    bool ok = page->GetTuple(row, schema_, txn, lock_manager_);
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return ok;
}

void TableHeap::DeleteTable(page_id_t page_id) {
    auto next_page_id = first_page_id_;
    while (next_page_id != INVALID_PAGE_ID) {
        auto old_page_id = next_page_id;
        // // cout << "Deleting page: " << old_page_id << endl;
        auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(old_page_id));
        assert(page != nullptr);
        next_page_id = page->GetNextPageId();
        // cout << "Next page: " << next_page_id << endl;
        buffer_pool_manager_->UnpinPage(old_page_id, false);
        buffer_pool_manager_->DeletePage(old_page_id);
    }
    if (page_id != INVALID_PAGE_ID) {
        buffer_pool_manager_->DeletePage(page_id);
    }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Txn *txn) {
    page_id_t pid = first_page_id_;
    // cout << "TableHeap::Begin: first_page_id_ = " << first_page_id_ << endl;
    // 遍历链表中所有页面，找到第一个有 tuple 的位置
    while (pid != INVALID_PAGE_ID) {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(pid));
    // cout << "TableHeap::Begin: Fetching page with id = " << pid << endl;
    if (page == nullptr) {
        break;  // 无法取到页，直接退出
    }
    // 锁住页面，找第一个 tuple
    page->RLatch();
    RowId first_rid;
    bool ok = page->GetFirstTupleRid(&first_rid);
    // cout << "TableHeap::Begin: GetFirstTupleRid returned " << ok  << endl;
    page->RUnlatch();
    // 释放页面引用，不标记脏
    buffer_pool_manager_->UnpinPage(pid, /*is_dirty=*/false);

    if (ok) {
        // 找到合法的 tuple，返回指向它的迭代器
        return TableIterator(this, first_rid, txn);
    }
    // 否则跳到下一页继续
    pid = page->GetNextPageId();
    }
    // 整个表都没有 tuple，返回 end()
    return End();
}


/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() {
    // 返回一个空的迭代器
    return TableIterator(nullptr, RowId(), nullptr);
}
