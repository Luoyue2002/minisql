#include <stdexcept>
#include <sys/stat.h>

#include "glog/logging.h"
#include "page/bitmap_page.h"
#include "storage/disk_manager.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
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

page_id_t DiskManager::AllocatePage() {
  // operate on the meta page
  DiskFileMetaPage* meta = reinterpret_cast<DiskFileMetaPage*>(GetMetaData());
  ///printf("%u %lu %lu\n", meta->num_allocated_pages_, BITMAP_SIZE, (PAGE_SIZE/(8*sizeof(uint32_t)) - 2));
  ASSERT(meta->num_allocated_pages_ < BITMAP_SIZE*(PAGE_SIZE/sizeof(uint32_t) - 2), "Space is full!");

  char * page_data = new char[PAGE_SIZE];
  BitmapPage<PAGE_SIZE>* bitmap;
  uint32_t i;
  // find a suitable bit map
  for(i = 0; i < meta->GetExtentNums(); i++) {
    if(meta->GetExtentUsedPage(i) < BITMAP_SIZE) {
      // allocate one page from the bitmap
      ReadPhysicalPage(1+i*BITMAP_SIZE, page_data);
      bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(page_data);
      break;
    }
  }

  // set a new bitmap
  if(i == meta->GetExtentNums()) {
    ReadPhysicalPage(1+i*BITMAP_SIZE, page_data);
    bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(page_data);
    meta->num_extents_ ++ ;
    meta->extent_used_page_[i] = 0;
  }

  uint32_t pageoffset;
  bitmap->AllocatePage(pageoffset);
  // renew the metapage and bitmap
  WritePhysicalPage(1+i*BITMAP_SIZE, page_data);
  meta->num_allocated_pages_ ++;
  meta->extent_used_page_[i] ++;
  WritePhysicalPage(0, reinterpret_cast<char*>(meta) );

  delete [] page_data;
  return i*BITMAP_SIZE+pageoffset;
}

void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");

  DiskFileMetaPage* meta = reinterpret_cast<DiskFileMetaPage*>(GetMetaData());
  /// if(logical_page_id >= BITMAP_SIZE*meta->GetExtentNums()) return;

  uint32_t x = logical_page_id / BITMAP_SIZE;
  uint32_t y = logical_page_id % BITMAP_SIZE;
  // get the bitmap
  char * page_data = new char[PAGE_SIZE];
  BitmapPage<PAGE_SIZE>* bitmap;
  ReadPhysicalPage(1+x*BITMAP_SIZE, page_data);
  bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(page_data);

  // deallocate the page
  bitmap->DeAllocatePage(y);
  //write back
  meta->num_allocated_pages_ --;
  meta->extent_used_page_[x] --;
  WritePhysicalPage(1+x*BITMAP_SIZE, page_data);

  delete[] page_data;
}

bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  /// DiskFileMetaPage* meta = reinterpret_cast<DiskFileMetaPage*>(GetMetaData());
  /// if(logical_page_id >= BITMAP_SIZE*meta->GetExtentNums()) return false;

  uint32_t x = logical_page_id / BITMAP_SIZE;
  uint32_t y = logical_page_id % BITMAP_SIZE;
  // get the bitmap
  char * page_data = new char[PAGE_SIZE];
  BitmapPage<PAGE_SIZE>* bitmap;
  ReadPhysicalPage(1+x*BITMAP_SIZE, page_data);
  bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(page_data);

  bool re = bitmap->IsPageFree(y);
  delete[] page_data;
  return re;
}

page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  return logical_page_id + logical_page_id/(BITMAP_SIZE) + 2;
  
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
