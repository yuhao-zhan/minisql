#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
        : pool_size_(pool_size), disk_manager_(disk_manager) {
    pages_ = new Page[pool_size_];
    replacer_ = new LRUReplacer(pool_size_);
    for (size_t i = 0; i < pool_size_; i++) {
        free_list_.emplace_back(i);
    }
}

BufferPoolManager::~BufferPoolManager() {
    for (auto page : page_table_) {
        FlushPage(page.first);
    }
    delete[] pages_;
    delete replacer_;
}

/**
 * TODO: Student Implement
 */
// 1.     Search the page table for the requested page (P).
// 1.1    If P exists, pin it and return it immediately.
// 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
//        Note that pages are always found from the free list first.
// 2.     If R is dirty, write it back to the disk.
// 3.     Delete R from the page table and insert P.
// 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
    lock_guard<recursive_mutex> guard(latch_);

    if (page_table_.count(page_id) != 0) {
        frame_id_t frame_id = page_table_[page_id];
        Page &page = pages_[frame_id];
        page.pin_count_++;
        replacer_->Pin(frame_id);
        return &page;
    }

    frame_id_t frame_id = TryToFindFreePage();
    if (frame_id == -1)
        return nullptr;

    Page &victim = pages_[frame_id];
    if (victim.IsDirty()) {
        disk_manager_->WritePage(victim.page_id_, victim.data_);
    }

    page_table_.erase(victim.page_id_);
    victim.page_id_ = page_id;
    victim.pin_count_ = 1;
    victim.is_dirty_ = false;

    page_table_[page_id] = frame_id;
    disk_manager_->ReadPage(page_id, victim.data_);

    return &victim;
}

/**
 * TODO: Student Implement
 */
// 0.   Make sure you call AllocatePage!
// 1.   If all the pages in the buffer pool are pinned, return nullptr.
// 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
// 3.   Update P's metadata, zero out memory and add P to the page table.
// 4.   Set the page ID output parameter. Return a pointer to P.
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
    lock_guard<recursive_mutex> guard(latch_);

    frame_id_t frame_id = TryToFindFreePage();
    if (frame_id == -1)
        return nullptr;

    Page &page = pages_[frame_id];
    if (page.IsDirty()) {
        disk_manager_->WritePage(page.page_id_, page.data_);
    }

    page_id = AllocatePage();

    page_table_.erase(page.page_id_);
    page_table_[page_id] = frame_id;

    page.page_id_ = page_id;
    page.pin_count_ = 1;
    page.is_dirty_ = false;
    memset(page.data_, 0, PAGE_SIZE);

    return &page;
}

/**
 * TODO: Student Implement
 */
// 0.   Make sure you call DeallocatePage!
// 1.   Search the page table for the requested page (P).
// 1.   If P does not exist, return true.
// 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
// 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
bool BufferPoolManager::DeletePage(page_id_t page_id) {
    lock_guard<recursive_mutex> guard(latch_);

    auto it = page_table_.find(page_id);

    if (it == page_table_.end()) return true;

    frame_id_t frame_id = it->second;
    Page &page = pages_[frame_id];

    if (page.pin_count_ > 0) return false;

    DeallocatePage(page_id);
    page_table_.erase(it);
    replacer_->Pin(frame_id);
    page.page_id_ = INVALID_PAGE_ID;
    page.pin_count_ = 0;
    page.is_dirty_ = false;
    memset(page.data_, 0, PAGE_SIZE);

    free_list_.emplace_back(frame_id);
    return false;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
    lock_guard<recursive_mutex> guard(latch_);

    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) return false;

    frame_id_t frame_id = it->second;
    Page &page =pages_[frame_id];

    // ASSERT(page.pin_count_ > 0, "Page Not Pinned!");
    // 如果已经没有 pin，就直接返回 false（或者根据你想要的语义返回 true）
    if (page.pin_count_ == 0) {
        LOG(WARNING) << "UnpinPage called on already-unpinned page " << page_id;
        return false;
    }

    page.pin_count_--;

    if (is_dirty) page.is_dirty_ = true;

    if (page.pin_count_ == 0) {
        replacer_->Unpin(frame_id);
    }

    return true;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
    lock_guard<recursive_mutex> guard(latch_);

    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) return false;

    frame_id_t frame_id = it->second;
    Page &page = pages_[frame_id];

    disk_manager_->WritePage(page.page_id_, page.data_);
    page.is_dirty_ = false;
    return true;
}

frame_id_t BufferPoolManager::TryToFindFreePage() {
    if (!free_list_.empty()) {
        frame_id_t frame = free_list_.front();
        free_list_.pop_front();
        return frame;
    }

    frame_id_t victim;
    if (replacer_->Victim(&victim)) return victim;

    return -1;
}

page_id_t BufferPoolManager::AllocatePage() {
    int next_page_id = disk_manager_->AllocatePage();
    return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
    disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
    return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
    bool res = true;
    for (size_t i = 0; i < pool_size_; i++) {
        if (pages_[i].pin_count_ != 0) {
            res = false;
            LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
        }
    }
    return res;
}