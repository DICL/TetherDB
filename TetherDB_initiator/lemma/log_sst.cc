#include "log_sst.h"
#include "db/memtable.h"
#include "file/filename.h"
#include "db/log_format.h"
#include "db/version_edit.h"

#if LEMMA_L0
namespace ROCKSDB_NAMESPACE {


LogSst::LogSst(MemTable* mem, const ImmutableDBOptions& db_options)
  : db_options_(db_options), mem_(mem), log_number_(mem->GetLogNumber()), num_entries_(mem->num_entries()) {
  refs_.store(1);
  offsets_ = (uint64_t*)calloc(num_entries_, sizeof(uint64_t));
  footers_ = (uint64_t*)calloc(num_entries_, sizeof(uint64_t));
}

LogSst::~LogSst() {
  free(offsets_);
  free(footers_);
}

uint64_t LogSst::GetIndexOf(const Slice& key) {
  ReadOptions ro;
  ro.total_order_seek = true;
  Arena arena;
  InternalIterator* iter_ = mem_->NewIterator(ro, &arena);
  iter_->Seek(key);
  if(iter_->Valid()) {
    return iter_->get_index();
  } else {
    return num_entries_;
  }
}

uint64_t LogSst::ApproximateOffsetOf(uint64_t size, const Slice& key) {
  return size * GetIndexOf(key) / num_entries_;
}

FileMetaData* LogSst::MakeLogSstMeta() {
  ReadOptions ro;
  ro.total_order_seek = true;
  Arena arena;
  InternalIterator* iter_ = mem_->NewIterator(ro, &arena);
  FileMetaData* meta = new FileMetaData();
  uint64_t i=0;
  iter_->SeekToFirst();
  meta->smallest.DecodeFrom(iter_->key());
  uint64_t num = ExtractInternalKeyFooter(iter_->key());
  uint64_t key_size = 0;
  SequenceNumber min = num >> 8;
  SequenceNumber max = min;
  while(iter_->Valid()) {
    if(i >= num_entries_)
      fprintf(stderr, "WTF!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! why i >= num_entries_\n");
    key_size += iter_->key().size();
    footers_[i] = ExtractInternalKeyFooter(iter_->key());
    uint64_t seq = footers_[i] >> 8;
    if(seq < min)
      min = seq;
    if(seq > max)
      max = seq;
    offsets_[i] = iter_->log_offset();
    iter_->set_index(i);
    i++;
    iter_->Next();
  }
  if(i != num_entries_)
    fprintf(stderr, "WTF!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! why i != num_entries_ %lu %lu\n", i, num_entries_);
  iter_->SeekToLast();
  meta->largest.DecodeFrom(iter_->key());
  meta->fd = FileDescriptor(log_number_, /*file_path_id*/ 0, mem_->get_data_size(), min, max);

  int64_t _current_time = 0;
  auto status = db_options_.env->GetSystemClock()->GetCurrentTime(&_current_time);
  // Safe to proceed even if GetCurrentTime fails. So, log and proceed.
  if (!status.ok()) {
    ROCKS_LOG_WARN(
        db_options_.info_log,
        "Failed to get current time to populate creation_time property. "
        "Status: %s",
        status.ToString().c_str());
  }
  const uint64_t current_time = static_cast<uint64_t>(_current_time);
  uint64_t oldest_key_time = mem_->ApproximateOldestKeyTime();
  uint64_t oldest_ancester_time = std::min(current_time, oldest_key_time);

  meta->oldest_ancester_time = oldest_ancester_time;
  meta->file_creation_time = current_time;
  meta->num_entries = num_entries_;
  meta->num_deletions = mem_->num_deletes();
  meta->raw_key_size = key_size;
  meta->raw_value_size = meta->fd.file_size - key_size;

  meta->log_sst = this;
  meta->is_wal = true;
  //fprintf(stderr, "%s, %s, %lu, %lu\n", meta->smallest.Encode().ToString().c_str(), meta->largest.Encode().ToString().c_str(), meta->fd.smallest_seqno, meta->fd.largest_seqno);
  return meta;
}

LogSstIterator::LogSstIterator(LogSst* log_sst, FileOptions file_options)
    : log_sst_(log_sst),
      pinned_iters_mgr_(nullptr),
      valid_(false),
      status_(Status::OK()),
      header_size_(log_sst_->db_options_.recycle_log_file_num > 0 ? log::kRecyclableHeaderSize : log::kHeaderSize) {
  std::string fname = LogFileName(log_sst_->db_options_.wal_dir, log_sst_->log_number_);
#if ROCKSDB_SPDK
  status_ = log_sst_->db_options_.spdk_fs->NewRandomAccessFile(fname, file_options, &file_, nullptr);
#else
  status_ = log_sst_->db_options_.fs->NewRandomAccessFile(fname, file_options, &file_, nullptr);
#endif
  if(!status_.ok())
    fprintf(stderr, "??????????????????????? %s\n", fname.c_str());
  //key_key = new std::string();
  //big_pool = (char*)calloc((log::kBlockSize - header_size_) * 4096, sizeof(char));//////////////////////////////////
  //pool_check = (bool*)calloc(4096, sizeof(bool));/////////////////////////////////////////////////////////////////////
}
  
void LogSstIterator::ReadFromFile() {
  if(log_sst_ == nullptr)
    fprintf(stderr, "why??????? log_offset_ nullptr \n");
  if(log_sst_->offsets_ == nullptr || log_sst_->footers_ == nullptr)
    fprintf(stderr, "why??????? log_offset_->offsets_ nullptr %lu\n", log_sst_->log_number_);
  if(file_ == nullptr)
    fprintf(stderr, "no file!!!!\n");
  if(cur_ >= log_sst_->num_entries_) {
    valid_ = false;
    return;
  }
  valid_ = true;
  Slice result;
  uint64_t start_off = log_sst_->offsets_[cur_];

  uint64_t left = log::kBlockSize - start_off%log::kBlockSize;
  if(left > LOG_SST_SIZE) {//should change!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    file_->Read(log_sst_->offsets_[cur_], LOG_SST_SIZE, IOOptions(), &result, scratch, nullptr);
  } else {
    file_->Read(log_sst_->offsets_[cur_], left, IOOptions(), &result, scratch, nullptr);
    file_->Read(log_sst_->offsets_[cur_] + left + header_size_, LOG_SST_SIZE - left, IOOptions(), &result, scratch+left, nullptr);
    result = Slice(scratch, LOG_SST_SIZE);
  }

  if(result[0] == kTypeColumnFamilyValue || result[0] == kTypeValue) {
    if(result[0] == kTypeColumnFamilyValue)
      result.remove_prefix(1);
    uint32_t cid = 0;
    GetVarint32(&result, &cid);
    uint32_t len = 0;
    GetVarint32(&result, &len);      
    if(result.size() < len) {
      fprintf(stderr, "what!!!!!!!!!!!!!!!!!!!!!\n");
      //
    }
    key_key.clear();
    key_key.assign(result.data(), len);
    PutFixed64(&key_key, log_sst_->footers_[cur_]);
    key_ = key_key;
    //delete key_key;
    //key_key = new std::string();
    //key_key->append(result.data(), len);
    //PutFixed64(key_key, log_sst_->footers_[cur_]);
    //key_ = *key_key;
    result.remove_prefix(len);


    if(result.size() < 4) {
      fprintf(stderr, "what!!!!!!!!!!!!!!!!!!!!!\n");
      //
    }
    len = 0;
    GetVarint32(&result, &len);      
    if(result.size() < len) {
      fprintf(stderr, "what!!!!!!!!!!!!!!!!!!!!!\n");
      //
    }
    value_ = Slice(result.data(), len);
    result.remove_prefix(len);  
  } else {
    fprintf(stderr, "%lu !!!!!!!!!!!!!!!!!!! %lu\n", log_sst_->log_number_, start_off);
    fprintf(stderr, "!!!!!!!!!!!!!!!!!!! %lu %lu\n", cur_, log_sst_->num_entries_);
    fprintf(stderr, "result:");
    for(int i=0; i<20; i++){
      char tmp = result.data()[i];
      if(('a'<=tmp && tmp<='z') || ('A'<=tmp && tmp<='Z') || ('0'<=tmp && tmp<='9'))
        fprintf(stderr, "%c ", tmp);
      else
        fprintf(stderr, "|%d| ", tmp);
    }
    fprintf(stderr, "\n");
    valid_ = false;
    return;
  }
}

}//namespace ROCKSDB_NAMESPACE

#endif
