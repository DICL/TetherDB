//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "env/spdk_env.h"

namespace rocksdb {

SpdkEnv::SpdkEnv(const std::shared_ptr<FileSystem>& base_fs) : FileSystemWrapper(base_fs) {
  fprintf(stderr,
          "%s:%d: Please use NewSpdkEnv(Env* base_env, std::string pcie_addr, "
          "int open_mod)\n",
          __FILE__, __LINE__);
}

SpdkEnv::SpdkEnv(const std::shared_ptr<FileSystem>& base_fs, Env* base_env, std::string pcie_addr,// const Options& opt,
                 int open_mod, int queue_start, int cache_size, int MULTI_NUM, int MY_ID)
    : FileSystemWrapper(base_fs), env_(base_env) {//, options_(opt) {
  fprintf(stderr, "open mode!!!!!!!!!!!!!!! %d\n", open_mod);//lemma
  Init(pcie_addr, 0, 20, cache_size, queue_start, MULTI_NUM, MY_ID);
  //Init(pcie_addr, options_.ssdlogging_num, options_.l0_queue_num, options_.topfs_cache_size);
  free_list.Put(LO_FILE_START, SPDK_MAX_LPN);
  if(open_mod == 0) { // open an empty rocksdb
    //
  }else if(open_mod == 1){ // open an existing db that produced by rocksdb
    //
  }else if(open_mod == 2){ // open an existing spandb
    fprintf(stderr, "read topfs metadata\n");
    //ReadMeta(&file_meta_, "/lemma/rocksdb");//options_.lo_path);
    fprintf(stderr, "read meta end!!!!!!!!!!!!!!! %d\n", open_mod);//lemma
    for(auto &meta : file_meta_){
      free_list.Remove(meta.second->start_lpn_, meta.second->end_lpn_);
    }//lemma
  }else{
    fprintf(stderr, "wrong open_mod\n");
    exit(0);
  }
  // TopsFSTest(this);
}

void SpdkEnv::Init(std::string pcie_addr, int logging_queue_num,
              int topfs_queue_num, int cache_size, int queue_start, int MULTI_NUM, int MY_ID) {
  //write_delay_.reset();
  //read_delay_.reset();
  sp_info_ = ssdlogging::InitSPDK(pcie_addr, logging_queue_num);
  master_mtx_.Lock();
  my_ID_ = ids_++;
  if(my_ID_ == 0) {
    std::thread t1(send_keep_alive);
    t1.detach();
  }
  sp_infos_.push_back(sp_info_);
  master_mtx_.Unlock();
  queue_start_ = queue_start;
  //SPDK_MAX_LPN =
  //    ((uint64_t)(sp_info_->namespaces->capacity * 0.8)) / SPDK_PAGE_SIZE;
  uint64_t total_cap = (uint64_t)(sp_info_->namespaces->capacity * 0.8) / SPDK_PAGE_SIZE;
  LO_START_LPN = MY_ID * (total_cap / MULTI_NUM);
  SPDK_MAX_LPN = (MY_ID+1) * (total_cap / MULTI_NUM) - 1;
  LO_FILE_START = (LO_START_LPN + (META_SIZE) / (SPDK_PAGE_SIZE));
  fprintf(stderr, "start l0:%lu, start File: %lu, end: %lu\n", LO_START_LPN, LO_FILE_START, SPDK_MAX_LPN);
  //fprintf(stderr, "SPDK_MAX_LPN: %lu\n", SPDK_MAX_LPN);
  spdk_tsc_rate_ = spdk_get_ticks_hz();
  assert(sp_info_ != nullptr);
  total_queue_num_ = sp_info_->num_io_queues;
  topfs_queue_num_ = topfs_queue_num;
  /*if (total_queue_num_ < topfs_queue_num_ + logging_queue_num + 1) {
    fprintf(stderr,
            "total queue num (%d) < topfs queue num (%d) + logging queue num (%d) "
            "+ 1\n",
            total_queue_num_, topfs_queue_num_, logging_queue_num);
    fprintf(stderr, "one queue is dedicated for logging metadata\n");
    ExitError();
  }*/
  fprintf(stderr, "total queue: %d, topfs queue: %d\n", total_queue_num_, topfs_queue_num_);

  if (my_ID_ == 0) {
    // 1.Allocate mem pool
    if(DISRUPTOR_QUEUE_LENGTH * FILE_BUFFER_ENTRY_SIZE < cache_size * (1ull<<30)){
      fprintf(stderr, "spdk memory pool size is smaller than cache size\n");
      ExitError();
    }
    huge_pages_ = new HugePage(cache_size * (1ull << 30));
    SPDK_MEM_POOL_ENTRY_NUM =  (cache_size * (1ull << 30)) / FILE_BUFFER_ENTRY_SIZE;
    spdk_mem_pool_ = new MemPool();
    for (uint64_t i = 0; i < SPDK_MEM_POOL_ENTRY_NUM; i++) {
      char *buffer = huge_pages_->Get(FILE_BUFFER_ENTRY_SIZE);
      if (UNLIKELY(buffer == nullptr)) {
        fprintf(stderr, "%s:%d: SPDK allocate memory failed\n", __FILE__,
              __LINE__);
        ExitError();
      }
      spdk_mem_pool_->WriteInBuf(buffer);
    }
#if LEVEL_CACHE
    topfs_cache = new SegMRUCache<uint64_t, std::shared_ptr<LRUEntry>>(SPDK_MEM_POOL_ENTRY_NUM - 1000);
#else
    topfs_cache = new SegLRUCache<uint64_t, std::shared_ptr<LRUEntry>>(SPDK_MEM_POOL_ENTRY_NUM - 1000);
#endif
  }


  //topfs_cache = new SegMRUCache<uint64_t, std::shared_ptr<LRUEntry>>(SPDK_MEM_POOL_ENTRY_NUM - 1000);
//#endif
  //topfs_cache = new HHVMLRUCache<uint64_t, std::shared_ptr<LRUEntry>>(SPDK_MEM_POOL_ENTRY_NUM - 2000);
  // 3.Initialize queue mutex and  queue stat
  spdk_queue_mutexes_ = new port::Mutex *[total_queue_num_];
  for (int i = 0; i < total_queue_num_; i++) {
    spdk_queue_mutexes_[i] = new port::Mutex();
  }
#ifdef PRINT_STAT
  last_print_flush_time_.store(0);
  last_print_compaction_time_.store(0);
  spdk_start_time_.store(0);
  total_flush_written_.store(0);
  total_compaction_written_.store(0);
#endif

}





SpdkEnv::~SpdkEnv() {
  Exit();
}

void SpdkEnv::ResetStat(){
  TopFSResetStat();
}

/*void SpdkEnv::SpanDBMigration(std::vector<LiveFileMetaData> files_metadata){
  SpanDBMigrationImpl(files_metadata, options_.max_level);
}*///lemma

// Partial implementation of the Env interface.
IOStatus SpdkEnv::NewSequentialFile(const std::string& fname,
                                  const FileOptions& option,
                                  std::unique_ptr<FSSequentialFile>* result,
                                  IODebugContext* /**/) {
  //fprintf(stderr, "%s not implemented in SPDK env\n", __FUNCTION__);
  auto fn = NormalizePath(fname);
  {
    // 1. search in FileSystem
    MutexLock lock(&fs_mutex_);
    if (spdk_file_system_.find(fn) != spdk_file_system_.end()) {
      result->reset(new SpdkSequentialFile(spdk_file_system_[fn], option));
      return IOStatus::OK();
    }
  }
  SPDKFileMeta* metadata = nullptr;
  {
    // 2. search in metadata
    MutexLock lock(&meta_mutex_);
    auto meta = file_meta_.find(fn);
    if (meta == file_meta_.end()) {
      *result = nullptr;
      fprintf(stderr, "SPDK-env: file not found %s\n", fname.c_str());
      return IOStatus::IOError(fn, "SPDK-env: file not found");
    }
    metadata = meta->second;
  }
  assert(metadata != nullptr);
  {
    // 3. search again in FileSystem
    MutexLock lock(&fs_mutex_);
    if (spdk_file_system_.find(fn) != spdk_file_system_.end()) {
      result->reset(new SpdkSequentialFile(spdk_file_system_[fn], option));
      return IOStatus::OK();
    } else {
      SpdkFile* file = new SpdkFile(env_, fn, metadata, metadata->size_, this);
      result->reset(new SpdkSequentialFile(file, option));
      spdk_file_system_[fn] = file;
      return IOStatus::OK();
    }
  }
/*  SpdkFile* file = new SpdkFile(env_, fn, metadata, metadata->size_);
  result->reset(new SpdkSequentialFile(file));
  {
    MutexLock lock(&fs_mutex_);
    spdk_file_system_[fn] = file;
  }
  return IOStatus::OK();*/
}

IOStatus SpdkEnv::NewRandomAccessFile(const std::string& fname,
                                    const FileOptions& /**/,
                                    std::unique_ptr<FSRandomAccessFile>* result,
                                    IODebugContext* /**/) {
  auto fn = NormalizePath(fname);
  {
    // 1. search in FileSystem
    MutexLock lock(&fs_mutex_);
    if (spdk_file_system_.find(fn) != spdk_file_system_.end()) {
      result->reset(new SpdkRandomAccessFile(spdk_file_system_[fn]));
      return IOStatus::OK();
    }
  }
  SPDKFileMeta* metadata = nullptr;
  {
    // 2. search in metadata
    MutexLock lock(&meta_mutex_);
    auto meta = file_meta_.find(fn);
    if (meta == file_meta_.end()) {
      *result = nullptr;
      fprintf(stderr, "SPDK-env: file not found %s\n", fname.c_str());
      return IOStatus::IOError(fn, "SPDK-env: file not found");
    }
    metadata = meta->second;
  }
  assert(metadata != nullptr);
  {
    // 3. search again in FileSystem
    MutexLock lock(&fs_mutex_);
    if (spdk_file_system_.find(fn) != spdk_file_system_.end()) {
      result->reset(new SpdkRandomAccessFile(spdk_file_system_[fn]));
      return IOStatus::OK();
    } else {
      SpdkFile* file = new SpdkFile(env_, fn, metadata, metadata->size_, this);
      result->reset(new SpdkRandomAccessFile(file));
      spdk_file_system_[fn] = file;
      return IOStatus::OK();
    }
  }
/*  SpdkFile* file = new SpdkFile(env_, fn, metadata, metadata->size_);
  result->reset(new SpdkRandomAccessFile(file));
  {
    MutexLock lock(&fs_mutex_);
    spdk_file_system_[fn] = file;
  }
  return IOStatus::OK();*/
}

/*Status SpdkEnv::NewRandomRWFile(const std::string& fname,
                                std::unique_ptr<RandomRWFile>* result,
                                const EnvOptions& //soptions) {
  if(fname == "") printf("\n");
  if(result == nullptr) printf("\n");
  printf("%s not implemented in SPDK env\n", __FUNCTION__);
  return Status::OK();
}*/

IOStatus SpdkEnv::ReuseWritableFile(const std::string& fname,
                                  const std::string& old_fname,
                                  const FileOptions& options,
                                  std::unique_ptr<FSWritableFile>* result,
                                  IODebugContext* dbg) {
  auto s = RenameFile(old_fname, fname, IOOptions(), dbg);
  if (!s.ok()) {
    fprintf(stderr, "%s error file %s\n", __FUNCTION__, fname.c_str());
    return s;
  }
  result->reset();
  return NewWritableFile(NormalizePath(fname), options, result, dbg);
}

IOStatus SpdkEnv::NewWritableFile(const std::string& fname,
                                const FileOptions& file_opts,
                                std::unique_ptr<FSWritableFile>* result,
                                IODebugContext* /**/) {
  return NewWritableFile(fname, result, file_opts.pre_allocate_size);
}

IOStatus SpdkEnv::NewWritableFile(const std::string& fname,
                                std::unique_ptr<FSWritableFile>* result,
                                uint64_t pre_allocate_size) {
  auto fn = NormalizePath(fname);
  bool is_flush = false;
  if (pre_allocate_size == 0) {
    pre_allocate_size = 1<<30;//env_options.allocate_size; lemmalemmalemma
    assert(pre_allocate_size != 0);
    is_flush = true;
  }
  assert(pre_allocate_size > 0);
  pre_allocate_size = pre_allocate_size + pre_allocate_size / 10;
  pre_allocate_size =
      (pre_allocate_size / FILE_BUFFER_ENTRY_SIZE + 1) * FILE_BUFFER_ENTRY_SIZE;
  uint64_t start_lpn = 0, end_lpn = 0;
  {
    uint64_t page_num = pre_allocate_size / SPDK_PAGE_SIZE;
#ifdef SPANDB_STAT
    auto start = SPDK_TIME;
#endif
    start_lpn = free_list.Get(page_num);
#ifdef SPANDB_STAT
    free_list_latency_.add(
        SPDK_TIME_DURATION(start, SPDK_TIME, spdk_tsc_rate_));
#endif
    if (start_lpn == 0) {
      fprintf(stderr, "1111 %s error file %s\n", __FUNCTION__, fname.c_str());
      return IOStatus::IOError("Get free start_lpn failed");
    }
    end_lpn = start_lpn + page_num - 1;
    if (start_lpn > end_lpn) {
      fprintf(stderr, "fname: %s, allocate_size: %ld, start_lpn %ld < end_lpn %ld\n",
             fname.c_str(), pre_allocate_size, start_lpn, end_lpn);
    }
  }
  SPDKFileMeta* meta = new SPDKFileMeta(fn, start_lpn, end_lpn, false);
  SpdkFile* file = new SpdkFile(env_, fn, meta, pre_allocate_size, this, is_flush);
  // check overlap
  //lemma_check IOStatus s = check_overlap(start_lpn, end_lpn, fn);
  {
    MutexLock lock(&meta_mutex_);
    //lemma_check if (!s.ok()) {
    //lemma_check   fprintf(stderr, "2222 %s error file %s\n", __FUNCTION__, fname.c_str());
    //lemma_check   return s;
    //lemma_check }
    file_meta_[fn] = meta;
  }
  {
    MutexLock lock(&fs_mutex_);
    spdk_file_system_[fn] = file;
  }
  //fprintf(stderr, "NewWritableFile %s from device\n", fname.c_str());
  result->reset(new SpdkWritableFile(file, nullptr/*lemma !!!!env_options.rate_limiter*/));
  // WriteMeta(&file_meta_);
  return IOStatus::OK();
}

IOStatus SpdkEnv::NewDirectory(const std::string& /*name*/,
                             const IOOptions& /**/,
                             std::unique_ptr<FSDirectory>* result,
                             IODebugContext* /**/) {
  result->reset(new SpdkDirectory());
  //fprintf(stderr, "%s not implemented in SPDK env\n", __FUNCTION__);
  return IOStatus::OK();
}

IOStatus SpdkEnv::FileExists(const std::string& fname,
                           const IOOptions& /**/,
                           IODebugContext* /**/) {
  auto fn = NormalizePath(fname);
  MutexLock lock(&meta_mutex_);
  if (file_meta_.find(fn) != file_meta_.end()) {
    return IOStatus::OK();
  }
  fprintf(stderr, "%s error file %s\n", __FUNCTION__, fname.c_str());
  return IOStatus::NotFound();
}

IOStatus SpdkEnv::GetChildren(const std::string& dir,
                            const IOOptions& /**/,
                            std::vector<std::string>* result,
                            IODebugContext* /**/) {
  if(dir == "") printf("\n");
  if(result == nullptr) printf("\n");
  fprintf(stderr, "%s not implemented in SPDK env\n", __FUNCTION__);
  return IOStatus::OK();
}

IOStatus SpdkEnv::DeleteFileInternal(const std::string& fname) {
  assert(fname == NormalizePath(fname));
  {
    // 1. delete from file system 
    const auto& fs_pair_t = spdk_file_system_.find(fname);
    if (fs_pair_t != spdk_file_system_.end()) {
      MutexLock lock(&fs_mutex_);
      const auto& fs_pair = spdk_file_system_.find(fname);
      if (fs_pair != spdk_file_system_.end()) {
        //fprintf(stderr, "DeleteFileInternal something wrong!! %s \n", fname.c_str());
        // will require fs_mutex_
        //assert(fs_pair->second->GetRef() == 1);
        //fs_pair->second->Unref();  // will be removed from file_system by unref()
        //fs_pair->second->UnrefWithLock();
        fs_pair->second->reset_last_seq();
      }
    }
  }
  uint64_t start_lpn = 0;
  uint64_t end_lpn = 0;
  {
    // 2. delete from file meta
    MutexLock lock(&meta_mutex_);
    const auto& meta_pair = file_meta_.find(fname);
    if (meta_pair != file_meta_.end()) {
      start_lpn = meta_pair->second->start_lpn_;
      end_lpn = meta_pair->second->end_lpn_;
      delete meta_pair->second;
      file_meta_.erase(fname);
    } else {
      fprintf(stderr, "DeleteFIle bad %s\n", fname.c_str());
      return IOStatus::IOError(fname, "SPDK-env: file not found");
    }
  }
#ifdef SPANDB_STAT
  auto start = SPDK_TIME;
#endif
  //lemma. storage node does not handle free list.
  //free_list.Put(start_lpn, end_lpn);
#ifdef SPANDB_STAT
  free_list_latency_.add(SPDK_TIME_DURATION(start, SPDK_TIME, spdk_tsc_rate_));
#endif
  RemoveFromLRUCache(start_lpn, end_lpn);
  return IOStatus::OK();
}

IOStatus SpdkEnv::DeleteFile(const std::string& fname,
                           const IOOptions& /**/,
                           IODebugContext* /**/) {
  auto fn = NormalizePath(fname);
  return DeleteFileInternal(fn);
}

/*Status SpdkEnv::Truncate(const std::string& fname, size_t size) {
  if(fname == "") printf("\n");
  if(size) printf("\n");
  printf("%s not implemented in SPDK env\n", __FUNCTION__);
  return Status::OK();
}*/

IOStatus SpdkEnv::CreateDir(const std::string& dirname,
                          const IOOptions& /**/,
                          IODebugContext* /**/) {
  if(dirname == "") printf("\n");
  fprintf(stderr, "%s not implemented in SPDK env\n", __FUNCTION__);
  return IOStatus::OK();
}

IOStatus SpdkEnv::CreateDirIfMissing(const std::string& dirname,
                                   const IOOptions& options,
                                   IODebugContext* dbg) {
  fprintf(stderr, "%s not implemented in SPDK env\n", __FUNCTION__);
  CreateDir(dirname, options, dbg);
  return IOStatus::OK();
}

IOStatus SpdkEnv::DeleteDir(const std::string& dirname,
                          const IOOptions& options,
                          IODebugContext* dbg) {
  return DeleteFile(dirname, options, dbg);
}

IOStatus SpdkEnv::GetFileSize(const std::string& fname,
                            const IOOptions& /**/,
                            uint64_t* file_size,
                            IODebugContext* /**/) {
  auto fn = NormalizePath(fname);
  MutexLock lock(&meta_mutex_);
  auto iter = file_meta_.find(fn);
  if (iter == file_meta_.end()) {
    fprintf(stderr, "GetFileSize err\n");
    return IOStatus::IOError(fn, "SPDK-env: file not found");
  }
  *file_size = iter->second->Size();
  return IOStatus::OK();
}
////////////////////////////////////////////////////////////////////////////////////////////
IOStatus SpdkEnv::GetFileAddr(const std::string& fname,
                            uint64_t* start_lpn,
                            uint64_t* end_lpn) {
  auto fn = NormalizePath(fname);
  MutexLock lock(&meta_mutex_);
  auto iter = file_meta_.find(fn);
  if (iter == file_meta_.end()) {
    fprintf(stderr, "GetFileSize err\n");
    return IOStatus::IOError(fn, "SPDK-env: file not found");
  }
  *start_lpn = iter->second->start_lpn_;
  *end_lpn = iter->second->end_lpn_;
  return IOStatus::OK();
}

#if LEVEL_CACHE
IOStatus SpdkEnv::AddInMeta(const std::string& fname,
                            uint64_t start_lpn,
                            uint64_t end_lpn,
                            uint64_t size,
                            int level) {
#else
IOStatus SpdkEnv::AddInMeta(const std::string& fname,
                            uint64_t start_lpn,
                            uint64_t end_lpn,
                            uint64_t size) {
#endif
  auto fn = NormalizePath(fname);
  MutexLock lock(&meta_mutex_);
  auto iter = file_meta_.find(fn);
  if (iter != file_meta_.end()) {
    //fprintf(stderr, "file already exist %lu %lu %lu %lu %lu %lu\n", start_lpn, iter->second->start_lpn_, end_lpn, iter->second->end_lpn_, size, iter->second->size_);
    return IOStatus::OK();
  }
  SPDKFileMeta *meta = new SPDKFileMeta(fn, start_lpn, end_lpn, false, size);
#if LEVEL_CACHE
  meta->level = level;
#endif
  file_meta_[fn] = meta;
  return IOStatus::OK();
}

uint64_t SpdkEnv::GetFreeBlocks(uint64_t pre_allocate_size,
                       int file_num,
                       uint64_t* start_lpns) {
  pre_allocate_size = pre_allocate_size + pre_allocate_size / 10;
  pre_allocate_size =
      (pre_allocate_size / FILE_BUFFER_ENTRY_SIZE + 1) * FILE_BUFFER_ENTRY_SIZE;
  uint64_t page_num = pre_allocate_size / SPDK_PAGE_SIZE;
  for(int i=0; i<file_num; i++)
    start_lpns[i] = free_list.Get(page_num);

  return page_num;
}

#if LEVEL_CACHE
IOStatus SpdkEnv::NewWritableFileFromFree(const std::string& fname,
                                std::unique_ptr<FSWritableFile>* result,
                                uint64_t pre_allocate_size,
                                uint64_t start_lpn,
                                int level) {
#else
IOStatus SpdkEnv::NewWritableFileFromFree(const std::string& fname,
                                std::unique_ptr<FSWritableFile>* result,
                                uint64_t pre_allocate_size,
                                uint64_t start_lpn) {
#endif
  auto fn = NormalizePath(fname);
  bool is_flush = false;
  if (pre_allocate_size == 0) {
    pre_allocate_size = 1<<30;//env_options.allocate_size; lemmalemmalemma
    assert(pre_allocate_size != 0);
    is_flush = true;
  }
  if(start_lpn > SPDK_MAX_LPN)
    fprintf(stderr, "SPDK_MAX_LPN overflow %lu, %lu\n", start_lpn, SPDK_MAX_LPN);
  assert(pre_allocate_size > 0);
  pre_allocate_size = pre_allocate_size + pre_allocate_size / 10;
  pre_allocate_size =
      (pre_allocate_size / FILE_BUFFER_ENTRY_SIZE + 1) * FILE_BUFFER_ENTRY_SIZE;
  uint64_t end_lpn = 0;
  {
    uint64_t page_num = pre_allocate_size / SPDK_PAGE_SIZE;
#ifdef SPANDB_STAT
    auto start = SPDK_TIME;
#endif
#ifdef SPANDB_STAT
    free_list_latency_.add(
        SPDK_TIME_DURATION(start, SPDK_TIME, spdk_tsc_rate_));
#endif
    if (start_lpn == 0) {
      fprintf(stderr, "1111 %s error file %s\n", __FUNCTION__, fname.c_str());
      return IOStatus::IOError("Get free start_lpn failed");
    }
    end_lpn = start_lpn + page_num - 1;
    if (start_lpn > end_lpn) {
      fprintf(stderr, "fname: %s, allocate_size: %ld, start_lpn %ld < end_lpn %ld\n",
             fname.c_str(), pre_allocate_size, start_lpn, end_lpn);
    }
  }
  SPDKFileMeta* meta = new SPDKFileMeta(fn, start_lpn, end_lpn, false);
#if LEVEL_CACHE
  meta->level = level;
#endif
  SpdkFile* file = new SpdkFile(env_, fn, meta, pre_allocate_size, this, is_flush);
  // check overlap
  //lemma_check IOStatus s = check_overlap(start_lpn, end_lpn, fn);
  {
    MutexLock lock(&meta_mutex_);
    //lemma_check if (!s.ok()) {
    //lemma_check   fprintf(stderr, "2222 %s error file %s\n", __FUNCTION__, fname.c_str());
    //lemma_check   return s;
    //lemma_check }
    file_meta_[fn] = meta;
  }
  {
    MutexLock lock(&fs_mutex_);
    spdk_file_system_[fn] = file;
  }
  //fprintf(stderr, "NewWritableFile %s from device\n", fname.c_str());
  result->reset(new SpdkWritableFile(file, nullptr/*lemma !!!!env_options.rate_limiter*/));
  // WriteMeta(&file_meta_);
  return IOStatus::OK();
}

IOStatus SpdkEnv::ReturnFreeBlocks(uint64_t start_lpn,
                       uint64_t end_lpn) {
  /*pre_allocate_size = pre_allocate_size + pre_allocate_size / 10;
  pre_allocate_size =
      (pre_allocate_size / FILE_BUFFER_ENTRY_SIZE + 1) * FILE_BUFFER_ENTRY_SIZE;
  uint64_t page_num = pre_allocate_size / SPDK_PAGE_SIZE;
  fou(int i=0; i<file_num; i++)*/
  free_list.Put(start_lpn, end_lpn);

  return IOStatus::OK();
}

//////////////////////////////////////////////////////////////////////////////////////////
IOStatus SpdkEnv::GetFileModificationTime(const std::string& fname,
                                        const IOOptions& /**/,
                                        uint64_t* time,
                                        IODebugContext* /**/) {
  auto fn = NormalizePath(fname);
  MutexLock lock(&meta_mutex_);
  auto iter = file_meta_.find(fn);
  if (iter == file_meta_.end()) {
    fprintf(stderr, "%s error file %s\n", __FUNCTION__, fname.c_str());
    return IOStatus::IOError(fn, "SPDK-env: file not found");
  }
  *time = iter->second->ModifiedTime();
  return IOStatus::OK();
}

IOStatus SpdkEnv::RenameFile(const std::string& src,
                           const std::string& dest,
                           const IOOptions& /**/,
                           IODebugContext* /**/) {
  auto s = NormalizePath(src);
  auto t = NormalizePath(dest);
  {
    // 1. rename in file meta
    MutexLock lock(&meta_mutex_);
    if (file_meta_.find(s) == file_meta_.end()) {
      fprintf(stderr, "%s error file %s\n", __FUNCTION__, src.c_str());
      return IOStatus::IOError(s, "SPDK-env: file not found");
    }
    file_meta_[t] = file_meta_[s];
    file_meta_.erase(s);
  }
  {
    // 2. rename in file system
    MutexLock lock(&fs_mutex_);
    if (spdk_file_system_.find(s) == spdk_file_system_.end()) {
      return IOStatus::OK();
    }
    spdk_file_system_[t] = spdk_file_system_[s];
    spdk_file_system_.erase(s);
    return IOStatus::OK();
  }
}

/*Status SpdkEnv::LinkFile(const std::string& src, const std::string& dest) {
  if(src == "") printf("\n");
  if(dest == "") printf("\n");
  printf("%s not implemented in SPDK env\n", __FUNCTION__);
  return Status::OK();
}*/

IOStatus SpdkEnv::NewLogger(const std::string& fname,
                          const IOOptions& /**/,
                          std::shared_ptr<Logger>* result,
                          IODebugContext* /**/) {
  if(fname == "") printf("\n");
  if(result == nullptr) printf("\n");
  fprintf(stderr, "%s not implemented in SPDK env\n", __FUNCTION__);
  return IOStatus::OK();
}

IOStatus SpdkEnv::LockFile(const std::string& fname,
                         const IOOptions& /**/,
                         FileLock** flock,
                         IODebugContext* /**/) {
  if(fname == "") printf("\n");
  if(flock == nullptr) printf("\n");
  fprintf(stderr, "%s not implemented in SPDK env\n", __FUNCTION__);
  return IOStatus::OK();
}

IOStatus SpdkEnv::UnlockFile(FileLock* flock,
                           const IOOptions& /**/,
                           IODebugContext* /**/) {
  if(flock == nullptr) printf("\n");
  fprintf(stderr, "%s not implemented in SPDK env\n", __FUNCTION__);
  return IOStatus::OK();
}

IOStatus SpdkEnv::GetTestDirectory(const IOOptions& /**/,
                                 std::string* path,
                                 IODebugContext* /**/) {
  *path = "/test";
  return IOStatus::OK();
}

std::string SpdkEnv::NormalizePath(const std::string path) {
  std::string dst;
  for (auto c : path) {
    if (!dst.empty() && c == '/' && dst.back() == '/') {
      continue;
    }
    dst.push_back(c);
  }
  return dst;
}

void SpdkEnv::RemoveFromLRUCache(uint64_t start_lpn, uint64_t end_lpn) {
  uint64_t start = start_lpn;
  uint64_t interval = FILE_BUFFER_ENTRY_SIZE / SPDK_PAGE_SIZE;
  while (start <= end_lpn) {
    topfs_cache->DeleteKey(convert_lpn(start, my_ID_));
    start += interval;
  }
}


#ifndef ROCKSDB_LITE
// This is to maintain the behavior before swithcing from InSpdkEnv to SpdkEnv
FileSystem* NewSpdkEnv(const std::shared_ptr<FileSystem>& base_fs) { return new SpdkEnv(base_fs); }
FileSystem* NewSpdkEnv(const std::shared_ptr<FileSystem>& base_fs, Env* base_env, std::string pcie_addr,// const Options& opt,
                int open_mod, int queue_start, int cache_size, int MULTI_NUM, int MY_ID) {
  return new SpdkEnv(base_fs, base_env, pcie_addr, open_mod, queue_start, cache_size, MULTI_NUM, MY_ID);// opt, open_mod);
}

#else  // ROCKSDB_LITE

FileSystem* NewSpdkEnv(FileSystem* /*base_env*/) { return nullptr; }

#endif  // !ROCKSDB_LITE

}  // namespace rocksdb
