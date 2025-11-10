#pragma once

#include "lemma.h"
#include "table/internal_iterator.h"

#if LEMMA_L0
namespace ROCKSDB_NAMESPACE {

class LogSstIterator;
class MemTable;
class FileMetaData;
#define LOG_SST_SIZE (1100)
#define LOG_SST_SIZE_M ((LOG_SST_SIZE) + 10)

class LogSst {
 public:

  LogSst(MemTable* mem, const ImmutableDBOptions& db_options);

  explicit LogSst(const LogSst& copy)
    : db_options_(copy.db_options_) {
    fprintf(stderr, "LogSst copy called!! \n");
  }

  ~LogSst();

  FileMetaData* MakeLogSstMeta();

  MemTable* mem() { return mem_; }

  uint64_t* Offsets() { return offsets_; }
  uint64_t* Footers() { return footers_; }
  uint64_t GetIndexOf(const Slice& key);
  uint64_t ApproximateOffsetOf(uint64_t size, const Slice& key);

  void LogSstRef() { refs_++; }
  LogSst* Unref() {
    --refs_;
    if(refs_ <= 0) {
      return this;
    }
    return nullptr;
  }
  uint64_t LogNumber() { return log_number_; }

 private:
  friend class LogSstIterator;
  const ImmutableDBOptions& db_options_;
  MemTable* mem_;
  uint64_t log_number_;
  uint64_t num_entries_;
  uint64_t* offsets_;
  uint64_t* footers_;
  std::atomic<int> refs_;
};

class LogSstIterator : public InternalIteratorBase<Slice> {
 public:
  LogSstIterator(LogSst* log_sst, FileOptions file_options);

  /*explicit LogSstIterator(const LogSstIterator& copy)
    : header_size_(copy.header_size_) {
    fprintf(stderr, "LogSstIterator copy called \n");
  }*/
  ~LogSstIterator() {
    //delete key_key;
  }
  
  void Seek(const Slice& /*target*/) override {
    fprintf(stderr, "LogSstIterator::Seek() not implemented\n");
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
  char scratch[LOG_SST_SIZE_M];
  std::string key_key;
  //char* big_pool;
  //bool* pool_check;
};
}//namespace ROCKSDB_NAMESPACE

#endif
