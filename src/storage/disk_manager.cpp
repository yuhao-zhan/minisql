#include "storage/disk_manager.h"

#include <sys/stat.h>

#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if (p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::AllocatePage() {
  ReadPhysicalPage(0, meta_data_);
  DiskFileMetaPage * disk_meta = reinterpret_cast<DiskFileMetaPage *>(meta_data_);

  ASSERT (disk_meta->num_allocated_pages_ < MAX_VALID_PAGE_ID, "Database Is Full!");

  char * cur_bitmap_data = new char[PAGE_SIZE];
  BitmapPage<PAGE_SIZE> * cur_bitmap_pointer = nullptr;
  uint32_t avail_extent;

  if (disk_meta->num_allocated_pages_ == disk_meta->num_extents_ * BITMAP_SIZE) {
    avail_extent = disk_meta->num_extents_;
    disk_meta->extent_used_page_[disk_meta->num_extents_]++;
    disk_meta->num_extents_++;
  }
  else {
    avail_extent = [&]() -> uint32_t {
      for (uint32_t i = 0; i < disk_meta->num_extents_; i++){
        if (disk_meta->extent_used_page_[i] < BITMAP_SIZE)
          return i;
      }
      return disk_meta->num_extents_;
    }();
    disk_meta->extent_used_page_[avail_extent]++;
  }

  ReadPhysicalPage(avail_extent * (BITMAP_SIZE + 1) + 1, cur_bitmap_data);
  disk_meta->num_allocated_pages_++;
  cur_bitmap_pointer = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(cur_bitmap_data);

  uint32_t page_offset;
  ASSERT(cur_bitmap_pointer->AllocatePage(page_offset), "Page Allocating Failed in Bitmap!");
  WritePhysicalPage(0, meta_data_);
  WritePhysicalPage(avail_extent * (BITMAP_SIZE + 1) + 1, cur_bitmap_data);
  delete[] cur_bitmap_data;
  return (disk_meta->num_extents_ - 1) * BITMAP_SIZE + page_offset;
}

/**
 * TODO: Student Implement
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  ReadPhysicalPage(0, meta_data_);
  DiskFileMetaPage * disk_meta = reinterpret_cast<DiskFileMetaPage *>(meta_data_);

  ASSERT(logical_page_id >=0 && logical_page_id < disk_meta->num_allocated_pages_, "No Such Page!");

  char * cur_bitmap_data = new char[PAGE_SIZE];
  BitmapPage<PAGE_SIZE> * cur_bitmap_pointer = nullptr;

  uint32_t page_offset = logical_page_id % BITMAP_SIZE;
  uint32_t bitmap_physcial_id = logical_page_id / BITMAP_SIZE;
  ReadPhysicalPage(MapPageId(logical_page_id) - page_offset - 1, cur_bitmap_data); 
  cur_bitmap_pointer = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(cur_bitmap_data);
  ASSERT(cur_bitmap_pointer->DeAllocatePage(page_offset), "DeAllocate Failed!");
  disk_meta->num_allocated_pages_--;
  disk_meta->extent_used_page_[bitmap_physcial_id]--;
  WritePhysicalPage(0, meta_data_);
  WritePhysicalPage(MapPageId(logical_page_id) - page_offset - 1, cur_bitmap_data); 
  delete[] cur_bitmap_data;
}

/**
 * TODO: Student Implement
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");

  page_id_t physcial_page_id = MapPageId(logical_page_id);
  page_id_t bitmap_physcial_id = physcial_page_id - logical_page_id % BITMAP_SIZE - 1;
  page_id_t page_offset = physcial_page_id - bitmap_physcial_id - 1;  

  char * bitmap_data = nullptr;
  ReadPhysicalPage(bitmap_physcial_id, bitmap_data);
  BitmapPage<PAGE_SIZE> * bitmap_pointer = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_data);

  return bitmap_pointer->IsPageFree(page_offset);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  return logical_page_id + logical_page_id / BITMAP_SIZE + 2;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}