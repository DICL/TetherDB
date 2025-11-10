#pragma once

#include "lemma.h"
#include "table/internal_iterator.h"
#include "grpc/compaction_data.grpc.pb.h"

#if LEMMA_L0
namespace ROCKSDB_NAMESPACE {

class LogSstIterator;
class MemTable;
class FileMetaData;
#define LOG_SST_SIZE (2100)
#define LOG_SST_SIZE_M ((LOG_SST_SIZE) + 10)

class LogSst {
 public:

  LogSst(MemTable* mem, const ImmutableDBOptions& db_options);

  LogSst(const ImmutableDBOptions& db_options, const compaction_data::CacheRequest* request);

  ~LogSst();

  FileMetaData* MakeLogSstMeta();

  MemTable* mem() { return mem_; }

  void LogSstRef() { refs_++; }
  LogSst* Unref() {
    --refs_;
    if(refs_ <= 0) {
      return this;
    }
    return nullptr;
  }

  void add_key_index(std::string key, uint64_t index) {
#if DEBUG_PRINT
   fprintf(stderr, "add_key_index %s %lu !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n", key.c_str(), index);
#endif
    keys_[key] = index;
  }

 private:
  friend class LogSstIterator;
  const ImmutableDBOptions& db_options_;
  MemTable* mem_;
  uint64_t log_number_;
  uint64_t num_entries_;
  uint64_t* offsets_;
  uint64_t* footers_;
  std::atomic<int> refs_;
  std::map<std::string, uint64_t> keys_;
};

class LogSstIterator : public InternalIteratorBase<Slice> {
 public:
  LogSstIterator(LogSst* log_sst, FileOptions file_options);

  LogSstIterator(const LogSstIterator &) = delete;
  void operator=(const LogSstIterator &) = delete;

  ~LogSstIterator() {
    //free(scratch);
    //delete key_key;
  }
  
  void Seek(const Slice& key) override {
    std::string tmp = ExtractUserKey(key).ToString();
    if (log_sst_->keys_.find(tmp) != log_sst_->keys_.end()) {
      cur_ = log_sst_->keys_[tmp];
#if DEBUG_PRINT
      fprintf(stderr, "LogSstIterator::Seek() %lu %lu %s!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n", log_sst_->log_number_, cur_, tmp.c_str());
#endif
      ReadFromFile();
    } else {
      valid_ = false;
      fprintf(stderr, "LogSstIterator::Seek() not implemented %s\n", tmp.c_str());
    }
  }
  void SeekForPrev(const Slice& /*target*/) override {
    fprintf(stderr, "LogSstIterator::SeekForPrev() not implemented\n");
  }
  void SeekToFirst() override {
    cur_ = 0;
    ReadFromFile();
  }
  void SeekToLast() override {
    fprintf(stderr, "LogSstIterator::SeekToLast() not implemented\n");
  }
  void Next() final override {
    cur_++;
    ReadFromFile();
  }
  bool NextAndGetResult(IterateResult* result) override {//////////////////////
    cur_++;
    ReadFromFile();
    if (valid_) {
      result->key = key_;
      result->bound_check_result = UpperBoundCheckResult();
      result->value_prepared = true;
    }
    return valid_;
  }
  void Prev() override {
    fprintf(stderr, "LogSstIterator::Prev() not implemented\n");
  }
  bool Valid() const override {
    return valid_;
  }
  Slice key() const override {
    return key_;
  }
  Slice user_key() const override {
    fprintf(stderr, "LogSstIterator::user_key() not implemented\n");
    return key_;
  }
  bool PrepareValue() override {
    fprintf(stderr, "LogSstIterator::PrepareValue() not implemented\n");
    return true;
  }
  Slice value() const override {
    return value_;
  }
  Status status() const override {
    return status_;
  }
  inline IterBoundCheck UpperBoundCheckResult() override {////////////////////////////
    return IterBoundCheck::kInbound;
  }
  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
  }
  bool IsKeyPinned() const override {
    fprintf(stderr, "LogSstIterator::IsKeyPinned() not implemented\n");
    return true;
  }
  bool IsValuePinned() const override {
    fprintf(stderr, "LogSstIterator::IsValuePinned() not implemented\n");
    return true;
  }
  

 private:
  uint64_t ReadRawData(Slice& result, uint64_t offset, uint64_t size);
  void ReadFromFile();

  LogSst* log_sst_;
  std::unique_ptr<FSRandomAccessFile> file_;
  PinnedIteratorsManager* pinned_iters_mgr_;
  Slice key_;
  Slice value_;
  bool valid_;
  Status status_;
  uint64_t cur_;
  const int header_size_;
  //char scratch[LOG_SST_SIZE_M];
  char* scratch;
  uint64_t scratch_size;
  uint64_t scratch_offset;
  std::string key_key;
  //char* big_pool;
  //bool* pool_check;
};
}//namespace ROCKSDB_NAMESPACE

#endif
