#pragma once

#include <algorithm>
#include <chrono>
// #include "lru_cache.h"
#include "port/likely.h"
#include "port/port.h"
#include "port/sys_time.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"//lemma
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "env/spdk_free_list.h"
#include "lemma/spdk_device.h"
#include "lemma/util.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/murmurhash.h"
#include "util/random.h"
#include "util/rate_limiter.h"
#include "utilities/lock_free_queue/disruptor_queue.h"

#if LEVEL_CACHE
#include "seg_level_mru_cache.h"
#else
#include "seg_lru_cache.h"
#include "hhvm_lru_cache.h"
#include "seg_mru_cache.h"
#endif
//#endif
//#define SPANDB_STAT

#define HUGE_PAGE_SIZE (1 << 26)
//#define HUGE_PAGE_SIZE (1 << 16)
#define SPDK_PAGE_SIZE (4096)
//#define FILE_BUFFER_ENTRY_SIZE (1ull << 12)
#define FILE_BUFFER_ENTRY_SIZE (1ull << 16)
#define META_SIZE (1 << 20)
//#define LO_START_LPN ((10ull << 30) / (SPDK_PAGE_SIZE))
//#define LO_FILE_START (LO_START_LPN + (META_SIZE) / (SPDK_PAGE_SIZE))
#define DISRUPTOR_QUEUE_LENGTH (64ull<<20)
#define READAHEAD (1)
#define SLEEP_LEMMA (1)

namespace rocksdb {

struct SPDKFileMeta;
struct BufferPool;
struct HugePage;
struct LRUEntry;
class SpdkFile;


static port::Mutex master_mtx_;
static std::vector<ssdlogging::SPDKInfo*> sp_infos_;
static uint64_t ids_ = 0;

static HugePage *huge_pages_ = nullptr;
static uint64_t SPDK_MEM_POOL_ENTRY_NUM;

static TopFSCache<uint64_t, std::shared_ptr<LRUEntry>> *topfs_cache = nullptr;//lemma
typedef DisruptorQueue<char *, DISRUPTOR_QUEUE_LENGTH> MemPool;
static MemPool *spdk_mem_pool_ = nullptr;//lemma

typedef std::map<std::string, SPDKFileMeta *> FileMeta;
typedef std::map<std::string, SpdkFile *> SPDKFileSystem;

static uint64_t spdk_tsc_rate_;

static void Exit();
static void ExitError();

#ifdef SPANDB_STAT
static uint64_t read_hit_;
static uint64_t read_miss_;
static ssdlogging::statistics::AvgStat free_list_latency_;
static ssdlogging::statistics::AvgStat write_latency_;
static ssdlogging::statistics::AvgStat read_latency_;
static ssdlogging::statistics::AvgStat read_miss_latency_;
static ssdlogging::statistics::AvgStat read_hit_latency_;
static ssdlogging::statistics::AvgStat read_disk_latency_;
static ssdlogging::statistics::AvgStat memcpy_latency_;
static ssdlogging::statistics::AvgStat test_latency_;
#endif
//static ssdlogging::statistics::AvgStat write_delay_;
//static ssdlogging::statistics::AvgStat read_delay_;

#ifdef PRINT_STAT
static std::mutex print_mutex_;
static std::atomic<uint64_t> total_flush_written_;
static std::atomic<uint64_t> total_compaction_written_;
static std::atomic<uint64_t> last_print_flush_time_;
static std::atomic<uint64_t> last_print_compaction_time_;
static std::atomic<uint64_t> spdk_start_time_;
#endif


class SpdkEnv : public FileSystemWrapper { //EnvWrapper {
 public:
  explicit SpdkEnv(const std::shared_ptr<FileSystem>& base_fs);

  explicit SpdkEnv(const std::shared_ptr<FileSystem>& base_fs, Env* base_env, std::string pcie_addr, int open_mod, int queue_start, int cache_size, int MULTI_NUM, int MY_ID);//const Options &opt, int open_mod);

  virtual ~SpdkEnv();


  //virtual void SpanDBMigration(std::vector<LiveFileMetaData> files_metadata) override;

  // Partial implementation of the Env interface.
  virtual IOStatus NewSequentialFile(const std::string& fname,
                                   const FileOptions& file_opts,
                                   std::unique_ptr<FSSequentialFile>* result,
                                   IODebugContext* dbg) override;

  virtual IOStatus NewRandomAccessFile(const std::string& fname,
                                     const FileOptions& file_opts,
                                     std::unique_ptr<FSRandomAccessFile>* result,
                                     IODebugContext* dbg) override;

/*  virtual Status NewRandomRWFile(const std::string& fname,
                                 std::unique_ptr<RandomRWFile>* result,
                                 const EnvOptions& options) override;
*/
  virtual IOStatus ReuseWritableFile(const std::string& fname,
                                   const std::string& old_fname,
                                   const FileOptions& file_opts,
                                   std::unique_ptr<FSWritableFile>* result,
                                   IODebugContext* dbg) override;

  virtual IOStatus NewWritableFile(const std::string& fname,
                                 const FileOptions& file_opts,
                                 std::unique_ptr<FSWritableFile>* result,
                                 IODebugContext* dbg) override;

  virtual IOStatus NewWritableFile(const std::string& fname,
                                 std::unique_ptr<FSWritableFile>* result,
                                 uint64_t pre_allocate_size);// override;

  virtual IOStatus NewDirectory(const std::string& name,
                              const IOOptions& io_opts,
                              std::unique_ptr<FSDirectory>* result,
                              IODebugContext* dbg) override;

  virtual IOStatus FileExists(const std::string& fname,
                            const IOOptions& options,
                            IODebugContext* dbg) override;

  virtual IOStatus GetChildren(const std::string& dir,
                             const IOOptions& options,
                             std::vector<std::string>* result,
                             IODebugContext* dbg) override;

  IOStatus DeleteFileInternal(const std::string& fname);

  virtual IOStatus DeleteFile(const std::string& fname,
                            const IOOptions& options,
                            IODebugContext* dbg) override;

//  virtual IOStatus Truncate(const std::string& fname, size_t size) override;

  virtual IOStatus CreateDir(const std::string& dirname,
                           const IOOptions& options,
                           IODebugContext* dbg) override;

  virtual IOStatus CreateDirIfMissing(const std::string& dirname,
                                    const IOOptions& options,
                                    IODebugContext* dbg) override;

  virtual IOStatus DeleteDir(const std::string& dirname,
                           const IOOptions& options,
                           IODebugContext* dbg) override;

  virtual IOStatus GetFileSize(const std::string& fname,
                             const IOOptions& options,
                             uint64_t* file_size,
                             IODebugContext* dbg) override;
///////////////////////////////////////////////////////////////////////////
  virtual IOStatus GetFileAddr(const std::string& fname,
                             uint64_t* start_lpn,
                             uint64_t* end_lpn) override;

  virtual IOStatus AddInMeta(const std::string& fname,
                             uint64_t start_lpn,
                             uint64_t end_lpn,
#if LEVEL_CACHE
                             uint64_t size, int level) override;
#else
                             uint64_t size) override;
#endif

  virtual uint64_t GetFreeBlocks(uint64_t pre_allocate_size,
                             int file_num,
                             uint64_t* start_lpns) override;

  virtual IOStatus NewWritableFileFromFree(const std::string& fname,
                                std::unique_ptr<FSWritableFile>* result,
                                uint64_t pre_allocate_size,
#if LEVEL_CACHE
                                uint64_t start_lpn, int level) override;
#else
                                uint64_t start_lpn) override;
#endif

  virtual IOStatus ReturnFreeBlocks(uint64_t start_lpn,
                             uint64_t end_lpn) override;
///////////////////////////////////////////////////////////////////////////

  virtual IOStatus GetFileModificationTime(const std::string& fname,
                                         const IOOptions& options,
                                         uint64_t* time,
                                         IODebugContext* dbg) override;

  virtual IOStatus RenameFile(const std::string& src,
                            const std::string& target,
                            const IOOptions& options,
                            IODebugContext* dbg) override;

//  virtual IOStatus LinkFile(const std::string& src,
//                          const std::string& target) override;

  virtual IOStatus NewLogger(const std::string& fname,
                           const IOOptions& io_opts,
                           std::shared_ptr<Logger>* result,
                           IODebugContext* dbg) override;

  virtual IOStatus LockFile(const std::string& fname,
                          const IOOptions& options,
                          FileLock** flock,
                          IODebugContext* dbg) override;

  virtual IOStatus UnlockFile(FileLock* flock,
                            const IOOptions& options,
                            IODebugContext* dbg) override;

  virtual IOStatus GetTestDirectory(const IOOptions& options,
                                  std::string* path,
                                  IODebugContext* dbg) override;

  void ResetStat();// override;

  // Doesn't really sleep, just affects output of GetCurrentTime(), NowMicros()
  // and NowNanos()
  void Init(std::string pcie_addr, int logging_queue_num,
                 int topfs_queue_num, int cache_size, int queue_start, int MULTI_NUM, int MY_ID);

  ssdlogging::SPDKInfo *spdk() { return sp_info_; }

  uint64_t GetSPDKQueueID() {
    //return __sync_fetch_and_add(&spdk_queue_id_, 1) % (topfs_queue_num_ - (topfs_queue_num_ / 3)) + queue_start_;
    return __sync_fetch_and_add(&spdk_queue_id_, 1) % topfs_queue_num_;
  }

  void RemoveFromLRUCache(uint64_t start_lpn, uint64_t end_lpn);
  uint64_t id() { return my_ID_; }

  port::Mutex **spdk_queue_mutexes_ = nullptr;
  FileMeta file_meta_;//lemma
  SPDKFileSystem spdk_file_system_;//lemma
  port::Mutex fs_mutex_;//lemma
  port::Mutex meta_mutex_;//lemma

 private:
  Env* env_;
  std::string NormalizePath(const std::string path);
  unsigned char my_ID_;

  uint64_t lo_current_lpn_;
  port::Mutex mutex_;
  //const Options options_;
  ssdlogging::SPDKInfo *sp_info_;
  uint64_t SPDK_MAX_LPN;
  uint64_t LO_START_LPN;
  uint64_t LO_FILE_START;
  SPDKFreeList free_list;
  int topfs_queue_num_ = 0;
  int total_queue_num_ = 0;
  uint64_t spdk_queue_id_ = 0;
  int queue_start_ = 0;
//static std::atomic<uint64_t> L0_hit;
//static std::atomic<uint64_t> L0_count;
};
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct HugePage {
 public:
  HugePage(uint64_t size)
      : size_(size), hugepages_(nullptr), index_(0), offset_(0) {
    page_num_ = size_ / HUGE_PAGE_SIZE;
    if (size % HUGE_PAGE_SIZE != 0) page_num_++;
    hugepages_ = new char *[page_num_];
    fprintf(stderr, "SPDK memory allocation starts (%.2lf GB)\n",
           size_ / 1024.0 / 1024.0 / 1024.0);
    for (uint64_t i = 0; i < page_num_; i++) {
      if (i % (uint64_t)(page_num_ * 0.2) == 0) {
        fprintf(stderr, "...%.1f%%", i * 1.0 / page_num_ * 100);
        fflush(stdout);
      }
      hugepages_[i] =
          (char *)spdk_zmalloc(HUGE_PAGE_SIZE, SPDK_PAGE_SIZE, NULL,
                               SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
      if (UNLIKELY(hugepages_[i] == nullptr)) {
        printf("\n");
        fprintf(stderr, "already allocated %.2lf GB\n",
                HUGE_PAGE_SIZE * i / 1024.0 / 1024.0 / 1024.0);
        fprintf(stderr, "%s:%d: SPDK allocate memory failed\n", __FILE__,
                __LINE__);
        ExitError();
      }
    }
    fprintf(stderr, "...100%%\nSPDK memory allocation finished.\n");
  };
  char *Get(uint64_t size) {
    assert(size <= HUGE_PAGE_SIZE);
    if (HUGE_PAGE_SIZE - offset_ < size) {
      index_++;
      offset_ = 0;
    }
    if (index_ >= page_num_){
      fprintf(stderr, "%s error \n", __FUNCTION__);
      return nullptr;
    }
    char *data = hugepages_[index_] + offset_;
    offset_ += size;
    return data;
  }
  ~HugePage() {
    for (uint64_t i = 0; i < page_num_; i++) {
      if (hugepages_[i] != nullptr) spdk_free(hugepages_[i]);
    }
  }

 private:
  uint64_t size_;
  uint64_t page_num_;
  char **hugepages_;
  uint64_t index_;
  uint64_t offset_;
};

struct LRUEntry {
  uint64_t key;
  char *data;
  LRUEntry(uint64_t k, char *d) : key(k), data(d) {}
  ~LRUEntry() {
    spdk_mem_pool_->WriteInBuf(data);
  }
};

// file buffer entry
struct FileBuffer {
  int queue_id_;
  uint64_t start_lpn_;
  bool synced_;
  bool readable_;
  char *data_;
  uint64_t start_time_;
  std::mutex mutex_;
  const uint64_t max_size_ = FILE_BUFFER_ENTRY_SIZE;
  uint64_t written_offset_;
#if SMART_CACHE
  int status_;
  std::shared_ptr<LRUEntry> buffer_cache_;
#endif

  explicit FileBuffer(uint64_t start_lpn)
      : queue_id_(-1),
        start_lpn_(start_lpn),
        synced_(false),
        readable_(false),
        data_(nullptr),
        written_offset_(0)
#if SMART_CACHE
        , status_(0)
        , buffer_cache_(nullptr)
#endif
        {}

  IOStatus AllocateSPDKBuffer() {
    if(spdk_mem_pool_->Length() < 30)
      topfs_cache->Evict(30 - spdk_mem_pool_->Length());
#ifdef SPANDB_STAT
    auto start = SPDK_TIME;
#endif
    assert(data_ == nullptr);
    queue_id_ = -1;
    readable_ = false;
    synced_ = false;
    assert(written_offset_ == 0);
    if(spdk_mem_pool_->Length() < 10)
      fprintf(stderr, "cannot AllocateSPDKBuffer %lu\n", spdk_mem_pool_->Length());
    data_ = spdk_mem_pool_->ReadValue();
#ifdef SPANDB_STAT
    test_latency_.add(
            SPDK_TIME_DURATION(start, SPDK_TIME, spdk_tsc_rate_));
#endif
    assert(data_ != nullptr);
    return IOStatus::OK();
  }

  ~FileBuffer() {}

  bool Full() {
    assert(data_ != nullptr);
    return written_offset_ == max_size_;
  }
  bool Empty() {
    if (data_ == nullptr) return true;
    return written_offset_ == 0;
  }
  size_t Append(const char *data, size_t size) {
    size_t s = 0;
    if (written_offset_ == max_size_) return s;
    if (written_offset_ + size > max_size_)
      s = max_size_ - written_offset_;
    else
      s = size;
    memcpy(data_ + written_offset_, data, s);
    written_offset_ += s;
    return s;
  }
};

struct SPDKFileMeta {
  std::string fname_;
  uint64_t start_lpn_;
  uint64_t end_lpn_;
  bool lock_file_;
  uint64_t size_;
  uint64_t modified_time_;
  uint64_t last_access_time_;
  bool readable_file;
#if LEVEL_CACHE
  int level;
#endif
  SPDKFileMeta(){
  }
  SPDKFileMeta(std::string fname, uint64_t start, uint64_t end, bool lock_file)
      : fname_(fname),
        start_lpn_(start),
        end_lpn_(end),
        lock_file_(lock_file),
        size_(0),
        modified_time_(0),
        last_access_time_(0),
        readable_file(false) {
#if LEVEL_CACHE
    level = 0;
#endif
  }
  SPDKFileMeta(std::string fname, uint64_t start, uint64_t end, bool lock_file,
               uint64_t size)
      : fname_(fname),
        start_lpn_(start),
        end_lpn_(end),
        lock_file_(lock_file),
        size_(size),
        modified_time_(0),
        last_access_time_(0) {
#if LEVEL_CACHE
    level = 0;
#endif
  }

  uint64_t Size() { return size_; }

  uint64_t ModifiedTime() { return modified_time_; }

  void SetReadable(bool readable) { readable_file = readable; }

  uint32_t Serialization(char *des) {
    uint32_t offset = 0;
    uint64_t size = fname_.size();
    EncodeFixed64(des + offset, size);
    offset += 8;
    memcpy(des + offset, fname_.c_str(), size);
    offset += size;
    EncodeFixed64(des + offset, start_lpn_);
    offset += 8;
    EncodeFixed64(des + offset, end_lpn_);
    offset += 8;
    char lock = lock_file_ ? '1' : '0';
    memcpy(des + offset, &lock, 1);
    offset += 1;
    EncodeFixed64(des + offset, size_);
    offset += 8;
    EncodeFixed64(des + offset, modified_time_);
    offset += 8;
    return offset;
  }

  uint32_t Deserialization(char *src) {
    uint32_t offset = 0;
    uint64_t size = 0;
    size = DecodeFixed64(src + offset);
    offset += 8;
    fname_ = std::string(src + offset, size);
    offset += fname_.size();
    start_lpn_ = DecodeFixed64(src + offset);
    offset += 8;
    end_lpn_ = DecodeFixed64(src + offset);
    offset += 8;
    char lock;
    memcpy(&lock, src + offset, 1);
    lock_file_ = (lock == '1');
    offset += 1;
    size_ = DecodeFixed64(src + offset);
    offset += 8;
    modified_time_ = DecodeFixed64(src + offset);
    offset += 8;
    return offset;
  }

  ~SPDKFileMeta(){
  }

  std::string ToString() {
    char out[200];
    sprintf(out,
            "FileName: %s, StartLPN: %ld, EndLPN: %ld, IsLock: %d, Size: %ld, "
            "ModifiedTime: %ld\n",
            fname_.c_str(), start_lpn_, end_lpn_, lock_file_, size_,
            modified_time_);
    return std::string(out);
  }
};

void send_keep_alive(){
  int count = 0;
  FILE * fp = fopen("cache_hist.txt", "w");
  while(1) {
#ifdef SPANDB_STAT
    size_t write_latency_size = write_latency_.size();
    size_t read_latency_size = read_latency_.size();
    fprintf(stderr, "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! write latency: %ld %.2lf us, read latency: %ld %.2lf us,\n",
         write_latency_size, write_latency_.avg(), read_latency_size, read_latency_.avg());
    fprintf(stderr, "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! read hit latency: %.2lf us, read miss latency: %.2lf us,\n",
         read_hit_latency_.avg(), read_miss_latency_.avg());
    write_latency_.reset();
    read_latency_.reset();
    read_hit_latency_.reset();
    read_miss_latency_.reset();
    if(read_hit_+read_miss_ > 0) {
      fprintf(fp,"read hit: %ld, read miss: %ld\n", read_hit_, read_miss_);
      fprintf(fp,"read hit ratio: %lf\n",
           (read_hit_ * 1.0) / (read_hit_ + read_miss_));
    }
#endif
    //fprintf(stderr, "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! write delay: %ld %.2lf us\n",
    //  write_delay_.size(), write_delay_.avg());
    //fprintf(stderr, "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! read delay: %ld %.2lf us\n",
    //  read_delay_.size(), read_delay_.avg());
    if (topfs_cache != nullptr){
      if (count == 0)
        topfs_cache->PrintHit(true);
      else
        topfs_cache->PrintHit(false);
    }
    if (count == 0) {
      MutexLock lock(&master_mtx_);
      for(auto sp : sp_infos_)
        spdk_nvme_ctrlr_process_admin_completions(sp->controller);
      fflush(fp);
    }
    count++;
    count = count%5;
    sleep(1);
  }
}

static void ExitError() {
  Exit();
  exit(-1);
}

static void WriteComplete(void *arg, const struct spdk_nvme_cpl *completion) {
  FileBuffer *buf = (FileBuffer *)arg;
  assert(buf->synced_ == false);
  if (spdk_nvme_cpl_is_error(completion)) {
    fprintf(stderr, "I/O error status: %s\n",
            spdk_nvme_cpl_get_status_string(&completion->status));
    fprintf(stderr, "start_lpn_: %lu \n", buf->start_lpn_);
    fprintf(stderr, "%s:%d: Write I/O failed, aborting run\n", __FILE__,
            __LINE__);
    buf->synced_ = false;
    ExitError();
  }
#ifdef SPANDB_STAT
  write_latency_.add(
      SPDK_TIME_DURATION(buf->start_time_, SPDK_TIME, spdk_tsc_rate_));
#endif
  buf->synced_ = true;
/*#if SMART_CACHE
  if (buf->data_ != nullptr) {
    std::shared_ptr<LRUEntry> value(
      new LRUEntry(buf->start_lpn_, buf->data_));
    topfs_cache->Insert(buf->start_lpn_, value);
    buf->data_ = nullptr;
  }
#endif*/
  //lemma spandb_controller_.IncreaseLDWritten(FILE_BUFFER_ENTRY_SIZE);
  //lemma spandb_controller_.IncreaseLDBgIO(FILE_BUFFER_ENTRY_SIZE);
}

static uint64_t convert_lpn(uint64_t lpn, unsigned char id) {
  uint64_t shifted_value = (uint64_t)id << 56;
  return lpn | shifted_value;
}

static void ReadComplete(void *arg, const struct spdk_nvme_cpl *completion) {
  FileBuffer *buf = (FileBuffer *)arg;
  assert(buf->readable_ == false);
  if (spdk_nvme_cpl_is_error(completion)) {
    fprintf(stderr, "I/O error status: %s\n",
            spdk_nvme_cpl_get_status_string(&completion->status));
    fprintf(stderr, "%s:%d: Write I/O failed, aborting run\n", __FILE__,
            __LINE__);
    buf->readable_ = false;
    ExitError();
  }
  buf->readable_ = true;
}

static void CheckComplete(SpdkEnv *sp_env, int queue_id) {
  assert(queue_id != -1);
  MutexLock lock(sp_env->spdk_queue_mutexes_[queue_id]);
  spdk_nvme_qpair_process_completions(sp_env->spdk()->namespaces->qpair[queue_id], 0);
}

static bool SPDKWrite(SpdkEnv *sp_env, FileBuffer *buf) {
  ssdlogging::SPDKInfo *spdk = sp_env->spdk();
  assert(buf != NULL);
  assert(buf->synced_ == false);
  assert(buf->data_ != NULL);
  uint64_t sec_per_page = SPDK_PAGE_SIZE / spdk->namespaces->sector_size;
  uint64_t lba = buf->start_lpn_ * sec_per_page;
  uint64_t size = buf->max_size_;
  if (size % SPDK_PAGE_SIZE != 0)
    size = (size / SPDK_PAGE_SIZE + 1) * SPDK_PAGE_SIZE;
  uint64_t sec_num = size / SPDK_PAGE_SIZE * sec_per_page;
  int queue_id = sp_env->GetSPDKQueueID();
  buf->queue_id_ = queue_id;
#ifdef SPANDB_STAT
  buf->start_time_ = SPDK_TIME;
#endif
  // printf("write lba: %ld, size: %ld sectors, queue: %d\n", lba, sec_num, queue_id);
  int rc = 0;
  {
    MutexLock lock(sp_env->spdk_queue_mutexes_[queue_id]);
    rc = spdk_nvme_ns_cmd_write(spdk->namespaces->ns,
                                spdk->namespaces->qpair[queue_id], buf->data_,
                                lba,     /* LBA start */
                                sec_num, /* number of LBAs */
                                WriteComplete, buf, 0);
  }
  if (rc != 0) {
    fprintf(stderr, "%s:%d: SPDK IO failed: %s\n", __FILE__, __LINE__,
            strerror(rc * -1));
    ExitError();
  }
  return true;
}

static bool SPDKRead(SpdkEnv *sp_env, FileBuffer *buf) {
  ssdlogging::SPDKInfo *spdk = sp_env->spdk();
  // ssdlogging::ns_entry* namespace = spdk->namespaces;
  assert(buf != NULL);
  assert(buf->readable_ == false);
  assert(buf->data_ != NULL);
  uint64_t sec_per_page = SPDK_PAGE_SIZE / spdk->namespaces->sector_size;
  uint64_t lba = buf->start_lpn_ * sec_per_page;
  uint64_t sec_num = buf->max_size_ / SPDK_PAGE_SIZE * sec_per_page;
  
  int queue_id = sp_env->GetSPDKQueueID();
  // printf("read lba: %ld, size: %ld sectors, queue: %d\n", lba, sec_num, queue_id);
  buf->queue_id_ = queue_id;
  int rc = 0;
  {
    MutexLock lock(sp_env->spdk_queue_mutexes_[queue_id]);
    rc = spdk_nvme_ns_cmd_read(spdk->namespaces->ns,
                               spdk->namespaces->qpair[queue_id], buf->data_,
                               lba,     /* LBA start */
                               sec_num, /* number of LBAs */
                               ReadComplete, buf, 0);
  }
  if (rc != 0) {
    fprintf(stderr, "%s:%d: SPDK IO failed: %s\n", __FILE__, __LINE__,
            strerror(rc * -1));
    ExitError();
  }
  return true;
}

class SpdkFile {
 public:
  explicit SpdkFile(Env *env, const std::string &fname, SPDKFileMeta *metadata,
                    uint64_t allocate_size, SpdkEnv *_sp_env, bool is_flush = false)
      : env_(env),
        fname_(fname),
        rnd_(static_cast<uint32_t>(
            MurmurHash(fname.data(), static_cast<int>(fname.size()), 0))),
        current_buf_index_(0),
        //last_sync_index_(-1),
        last_sync_index_(0),
        refs_(0),
#if PIN_L0
        refs_hist_(0),
#endif
        metadata_(metadata),
        sp_env_(_sp_env),
        sp_info_(sp_env_->spdk()),
        is_flush_(is_flush),
        allocated_buffer(0) {
    // printf("create spdk file fname: %s, ref: %ld\n", fname_.c_str(), refs_);
    uint64_t current_page = metadata->start_lpn_;
    start_lpn_ = metadata->start_lpn_;
    uint64_t pages_per_buffer = FILE_BUFFER_ENTRY_SIZE / SPDK_PAGE_SIZE;
    file_buffer_num_ = allocate_size / FILE_BUFFER_ENTRY_SIZE;
    if (allocate_size % FILE_BUFFER_ENTRY_SIZE != 0) file_buffer_num_++;
    // printf("open spdkfile: %s\n", metadata_->ToString().c_str());
    // printf("file_buffer_num_: %ld\n", file_buffer_num_);

    //file_buffer_ = (FileBuffer **)malloc(sizeof(FileBuffer) * file_buffer_num_);
    file_buffer_ = new FileBuffer*[file_buffer_num_];
    for (uint64_t i = 0; i < file_buffer_num_; i++) {
      file_buffer_[i] = new FileBuffer(current_page);
      current_page += pages_per_buffer;
    }
#if SMART_CACHE
    if (fname[fname.size()-1] == 't' || fname[fname.size()-1] == 'p')
      is_sst_ = true;
    else
      is_sst_ = false;
    first_read_unsync_index_ = 0;
    last_prefetch_index_ = -1;
#endif
#if LEVEL_CACHE
    level_ = metadata->level;
#endif
    // Ref();
  }

  int BufferSize() { return allocated_buffer.load(); }

  SpdkFile(const SpdkFile &) = delete;  // No copying allowed.
  void operator=(const SpdkFile &) = delete;

  ~SpdkFile() {
    assert(refs_ == 0);
    for (uint64_t i = 0; i < file_buffer_num_; i++) {
      if (file_buffer_[i]->data_ != nullptr) {
        file_buffer_[i]->buffer_cache_.reset(new LRUEntry(file_buffer_[i]->start_lpn_, file_buffer_[i]->data_));
/*#if LEVEL_CACHE
        topfs_cache->Insert(convert_lpn(file_buffer_[i]->start_lpn_, sp_env_->id()), file_buffer_[i]->buffer_cache_, level_);
#else
        topfs_cache->Insert(convert_lpn(file_buffer_[i]->start_lpn_, sp_env_->id()), file_buffer_[i]->buffer_cache_);
#endif*/
/*        std::shared_ptr<LRUEntry> value(
            new LRUEntry(file_buffer_[i]->start_lpn_, file_buffer_[i]->data_));
#if PRI_CACHE
        topfs_cache->Insert(file_buffer_[i]->start_lpn_, value, is_sst_);
#else
        topfs_cache->Insert(file_buffer_[i]->start_lpn_, value);
#endif*/
      }
      delete file_buffer_[i];
      file_buffer_[i] = nullptr;
    }
    //free(file_buffer_);
    delete[] file_buffer_;
    //assert(file_meta_.find(fname_) != file_meta_.end());
  }

  bool is_lock_file() const { return metadata_->lock_file_; }

  bool Lock() {
    assert(metadata_->lock_file_);
    MutexLock lock(&mutex_);
    if (locked_) {
      return false;
    } else {
      locked_ = true;
      return true;
    }
  }

  void Unlock() {
    assert(metadata_->lock_file_);
    MutexLock lock(&mutex_);
    locked_ = false;
  }

  uint64_t Size() const { return metadata_->size_; }

  void Truncate(size_t size) {
    MutexLock lock(&mutex_);
    if(size) printf("\n");
    fprintf(stderr, "%s:%d: SPDK File Truncate() not implemented\n", __FILE__,
            __LINE__);
  }

  void CorruptBuffer() {
    fprintf(stderr, "%s:%d: SPDK File CorruptBuffer() not implemented\n",
            __FILE__, __LINE__);
  }

#if SMART_CACHE
  IOStatus Prefetch(uint64_t offset, size_t n) {
    size_t size = 0;
    while (n > 0) {
#ifdef SPANDB_STAT
      bool hit = false;
#endif
      uint64_t buffer_index = offset / FILE_BUFFER_ENTRY_SIZE;
      uint64_t buffer_offset = offset % FILE_BUFFER_ENTRY_SIZE;
      assert(buffer_index < file_buffer_num_);
      if(buffer_index >= file_buffer_num_) {
        fprintf(stderr, "Prefetch overflow !! buffer_index: %lu, file_buffer_num_: %lu\n", buffer_index, file_buffer_num_);
        return IOStatus::OK();
      }
      if(file_buffer_[buffer_index] == nullptr) {
        fprintf(stderr, "file_buffer_[buffer_index] == nullptr 11\n");
        fprintf(stderr, "nullptr %s %s\n", fname_.c_str(), metadata_->fname_.c_str());
      }
      FileBuffer *buffer = file_buffer_[buffer_index];

      if(buffer->status_ == 0) {
        buffer->mutex_.lock();
        if(buffer->status_ == 0) {
          if (topfs_cache->Find(convert_lpn(buffer->start_lpn_, sp_env_->id()), buffer->buffer_cache_)) {
#ifdef SPANDB_STAT
            hit = true;
#endif
            buffer->status_ = 2;
          } else {
            assert(buffer->data_ == nullptr);
            buffer->AllocateSPDKBuffer();
            SPDKRead(sp_env_, buffer);
            buffer->status_ = 1;
          }
        }
        buffer->mutex_.unlock();
      }
      size_t s = 0;
      if (buffer_offset + n <= FILE_BUFFER_ENTRY_SIZE) {
        s = n;
      } else {
        s = FILE_BUFFER_ENTRY_SIZE - buffer_offset;
      }
      size += s;
      offset += s;
      n -= s;
#ifdef SPANDB_STAT
      if (hit) {
        __sync_fetch_and_add(&read_hit_, 1);
      } else {
        __sync_fetch_and_add(&read_miss_, 1);
      }
#endif
    }
    return IOStatus::OK();
  }

  void FinishSeqPrefetch() {
    for(int64_t i = first_read_unsync_index_; i <= last_prefetch_index_; i++) {
      FileBuffer *buffer = file_buffer_[i];
      if (buffer == nullptr) return;
      uint64_t lpn = buffer->start_lpn_;

      buffer->mutex_.lock();
      if (buffer->status_ == 1) {
#if SLEEP_LEMMA
        uint64_t check_count = 0;
#endif
        while (!buffer->readable_) {
          CheckComplete(sp_env_, buffer->queue_id_);
#if SLEEP_LEMMA
          if(check_count++ > 1000000) {
            check_count = 0;
            usleep(100);
          }
#endif
        }
        buffer->buffer_cache_.reset(new LRUEntry(lpn, buffer->data_));
#if PIN_L0 == 0
        topfs_cache->Insert(convert_lpn(lpn, sp_env_->id()), buffer->buffer_cache_);
#endif
        buffer->data_ = nullptr;
        buffer->status_ = 2;
      }
      buffer->mutex_.unlock();
    }
  }

  IOStatus SeqPrefetch(uint64_t offset, size_t n, Slice *result, char *scratch) {
#ifdef SPANDB_STAT
    auto start = SPDK_TIME;
#endif
    assert(scratch != nullptr);
    size_t size = 0;
    while (n > 0) {
      uint64_t buffer_index = offset / FILE_BUFFER_ENTRY_SIZE;
      uint64_t buffer_offset = offset % FILE_BUFFER_ENTRY_SIZE;
      assert(buffer_index < file_buffer_num_);
      if(buffer_index >= file_buffer_num_) {
        fprintf(stderr, "buffer_index: %lu, file_buffer_num_: %lu\n", buffer_index, file_buffer_num_);
      }
      if(file_buffer_[buffer_index] == nullptr) {
        fprintf(stderr, "file_buffer_[buffer_index] == nullptr 22\n");
        fprintf(stderr, "nullptr %s %s\n", fname_.c_str(), metadata_->fname_.c_str());
      }
      FileBuffer *buffer = file_buffer_[buffer_index];

      //buffer->mutex_.lock();
      if(buffer->mutex_.try_lock()) {
        if(buffer->status_ == 0) {
          assert(buffer->data_ == nullptr);
          buffer->AllocateSPDKBuffer();
          SPDKRead(sp_env_, buffer);
          buffer->status_ = 1;
#ifdef SPANDB_STAT
          __sync_fetch_and_add(&read_miss_, 1);
#endif
        }
        buffer->mutex_.unlock();
      }
      last_prefetch_index_++;
      

      if(buffer_index > 15) {
        FileBuffer *old_buf = file_buffer_[buffer_index-16];
        old_buf->mutex_.lock();
        if(old_buf->status_ == 1) {
#if SLEEP_LEMMA
          uint64_t check_count = 0;
#endif
          while (!old_buf->readable_) {
            CheckComplete(sp_env_, old_buf->queue_id_);
#if SLEEP_LEMMA
            if(check_count++ > 1000000) {
              check_count = 0;
              usleep(100);
            }
#endif
          }
          old_buf->buffer_cache_.reset(new LRUEntry(old_buf->start_lpn_, old_buf->data_));
#if PIN_L0 == 0
          topfs_cache->Insert(convert_lpn(old_buf->start_lpn_, sp_env_->id()), old_buf->buffer_cache_);
#endif
          old_buf->data_ = nullptr;
          old_buf->status_ = 2;
        }
        old_buf->mutex_.unlock();
        first_read_unsync_index_++;
      }
      size_t s = 0;
      if (buffer_offset + n <= FILE_BUFFER_ENTRY_SIZE) {
        s = n;
      } else {
        s = FILE_BUFFER_ENTRY_SIZE - buffer_offset;
      }
      size += s;
      offset += s;
      n -= s;
    }
    assert(n == 0);
    *result = Slice(scratch, size);
#ifdef SPANDB_STAT
    read_latency_.add(SPDK_TIME_DURATION(start, SPDK_TIME, spdk_tsc_rate_));
#endif
    return IOStatus::OK();
  }

  IOStatus DIO_Read(uint64_t offset, size_t n, Slice *result, char *scratch) {
    size_t size = 0;
    char *ptr = scratch;
    while (n > 0) {
      uint64_t buffer_index = offset / FILE_BUFFER_ENTRY_SIZE;
      uint64_t buffer_offset = offset % FILE_BUFFER_ENTRY_SIZE;
      uint64_t buffer_entry_size;
      buffer_entry_size = FILE_BUFFER_ENTRY_SIZE;
      FileBuffer* buf = new FileBuffer(start_lpn_+buffer_index);
      buf->AllocateSPDKBuffer();
      SPDKRead(sp_env_, buf);
      //uint64_t count = 0;
      while (!buf->readable_) {
        CheckComplete(sp_env_, buf->queue_id_);
        /*count++;
        if (count > 1000000) {
          count = 0;
          usleep(100);
        }*/
      }
      size_t s = 0;
      if (buffer_offset + n <= buffer_entry_size) {
        s = n;
      } else {
        s = buffer_entry_size - buffer_offset;
      }
      memcpy(ptr + size, buf->data_ + buffer_offset, s);
      size += s;
      offset += s;
      n -= s;
      spdk_mem_pool_->WriteInBuf(buf->data_);
      delete buf;
    }
    *result = Slice(scratch, size);
    return IOStatus::OK();
  }

  IOStatus Read(uint64_t offset, size_t n, Slice *result, char *scratch) {
    //auto start = SPDK_TIME;
    size_t og_n = n;
#ifdef SPANDB_STAT
    auto start = SPDK_TIME;
#endif
    assert(scratch != nullptr);
    size_t size = 0;
    char *ptr = scratch;
    while (n > 0) {
#ifdef SPANDB_STAT
      auto s_time = SPDK_TIME;
      bool hit = false;
#endif
      uint64_t buffer_index = offset / FILE_BUFFER_ENTRY_SIZE;
      uint64_t buffer_offset = offset % FILE_BUFFER_ENTRY_SIZE;
      assert(buffer_index < file_buffer_num_);
      if(buffer_index >= file_buffer_num_) {
        fprintf(stderr, "buffer_index: %lu, file_buffer_num_: %lu\n", buffer_index, file_buffer_num_);
      }
      if(file_buffer_[buffer_index] == nullptr) {
        fprintf(stderr, "file_buffer_[buffer_index] == nullptr 33\n");
        fprintf(stderr, "nullptr %s %s\n", fname_.c_str(), metadata_->fname_.c_str());
      }
      FileBuffer *buffer = file_buffer_[buffer_index];
      uint64_t lpn = buffer->start_lpn_;
      char *data = nullptr;


      buffer->mutex_.lock();
      //if (!is_sst_)
      //  L0_count++;
      if (buffer->status_ == 2) {
        if (buffer->buffer_cache_ == nullptr) {
          fprintf(stderr, "read buffer state 3 something worng!!!!!\n");
        } else {
          //if (!is_sst_)
          //  L0_hit++;
          data = buffer->buffer_cache_->data;
#ifdef SPANDB_STAT
          hit = true;
#endif
        }
      } else if (buffer->status_ == 0 && topfs_cache->Find(convert_lpn(lpn, sp_env_->id()), buffer->buffer_cache_)) {
        data = buffer->buffer_cache_->data;
#ifdef SPANDB_STAT
        hit = true;
#endif
        buffer->status_ = 2;
      } else {
        if (buffer->status_ == 0) {
          assert(buffer->data_ == nullptr);
          buffer->AllocateSPDKBuffer();
          SPDKRead(sp_env_, buffer);
        }
#if SLEEP_LEMMA
        uint64_t check_count = 0;
#endif
        while (!buffer->readable_) {
          CheckComplete(sp_env_, buffer->queue_id_);
#if SLEEP_LEMMA
          if(check_count++ > 1000000) {
            check_count = 0;
            usleep(100);
          }
#endif
        }
        buffer->buffer_cache_.reset(new LRUEntry(lpn, buffer->data_));
        data = buffer->data_;
        buffer->data_ = nullptr;
        buffer->status_ = 2;
      }
      buffer->mutex_.unlock();

//////////////////////////////////// read ahead /////////////////////////////////
      if(is_sst_) {
        /*if(buffer_index == 0) {
          for(uint64_t i = 0; i < READAHEAD - 1; i++) {
            if(buffer_index + i < file_buffer_num_) {
              FileBuffer *future_buf = file_buffer_[buffer_index + i];
              if (future_buf->status_ == 0) {
                //future_buf->mutex_.lock();
                if(future_buf->mutex_.try_lock()) {
                  if (future_buf->status_ == 0) {
                    if (topfs_cache->Find(convert_lpn(future_buf->start_lpn_, sp_env_->id()), future_buf->buffer_cache_)) {
                      future_buf->status_ = 2;
                    } else {
                      future_buf->AllocateSPDKBuffer();
                      SPDKRead(sp_env_, future_buf);
                      future_buf->status_ = 1;
                    }
                  }
                  future_buf->mutex_.unlock();
                }
              }
            }
          }
        }
        if(buffer_index + READAHEAD < file_buffer_num_) {
          FileBuffer *future_buf = file_buffer_[buffer_index + READAHEAD];
          if (future_buf->status_ == 0) {
            //future_buf->mutex_.lock();
            if(future_buf->mutex_.try_lock()) {
              if (future_buf->status_ == 0) {
                if (topfs_cache->Find(convert_lpn(future_buf->start_lpn_, sp_env_->id()), future_buf->buffer_cache_)) {
                  future_buf->status_ = 2;
                } else {
                  future_buf->AllocateSPDKBuffer();
                  SPDKRead(sp_env_, future_buf);
                  future_buf->status_ = 1;
                }
              }
              future_buf->mutex_.unlock();
            }
          }
        }*/
        /*for(uint64_t i = 1; i <= READAHEAD; i++) {
          if(buffer_index*READAHEAD + i < file_buffer_num_) {
            FileBuffer *future_buf = file_buffer_[buffer_index*2 + i];
            if (future_buf->status_ == 0) {
              //future_buf->mutex_.lock();
              if(future_buf->mutex_.try_lock()) {
                if (future_buf->status_ == 0) {
                  if (topfs_cache->Find(convert_lpn(future_buf->start_lpn_, sp_env_->id()), future_buf->buffer_cache_)) {
                    future_buf->status_ = 2;
                  } else {
                    future_buf->AllocateSPDKBuffer();
                    SPDKRead(sp_env_, future_buf);
                    future_buf->status_ = 1;
                  }
                }
                future_buf->mutex_.unlock();
              }
            }
          }
        }*/
      }
/////////////////////////////////////////////////////////////////////////////////

      // copy to ptr
      size_t s = 0;
      if (buffer_offset + n <= FILE_BUFFER_ENTRY_SIZE) {
        s = n;
      } else {
        s = FILE_BUFFER_ENTRY_SIZE - buffer_offset;
      }
      memcpy(ptr + size, data + buffer_offset, s);
      if (is_sst_ && buffer_offset + n >= FILE_BUFFER_ENTRY_SIZE && og_n < 5000) {
        buffer->mutex_.lock();
        if (buffer->status_ == 2) {
          buffer->buffer_cache_.reset();
          buffer->buffer_cache_ = nullptr;
          buffer->status_ = 0;
        }
        buffer->mutex_.unlock();
      }
      size += s;
      offset += s;
      n -= s;
#ifdef SPANDB_STAT
      if (hit) {
        __sync_fetch_and_add(&read_hit_, 1);
        read_hit_latency_.add(
            SPDK_TIME_DURATION(s_time, SPDK_TIME, spdk_tsc_rate_));
      } else {
        __sync_fetch_and_add(&read_miss_, 1);
        read_miss_latency_.add(SPDK_TIME_DURATION(s_time, SPDK_TIME, spdk_tsc_rate_));
      }
#endif
    }
    assert(n == 0);
    *result = Slice(scratch, size);
#ifdef SPANDB_STAT
    read_latency_.add(SPDK_TIME_DURATION(start, SPDK_TIME, spdk_tsc_rate_));
#endif
    //read_delay_.add(SPDK_TIME_DURATION(start, SPDK_TIME, spdk_tsc_rate_));
    return IOStatus::OK();
  }

#else //NO SMART_CACHE ////////////////////////////////////////////////////////////////////////////////////
  IOStatus Read(uint64_t offset, size_t n, Slice *result, char *scratch, bool is_seq = false) {
#ifdef SPANDB_STAT
    auto start = SPDK_TIME;
#endif
    assert(scratch != nullptr);
    size_t size = 0;
    char *ptr = scratch;
    while (n > 0) {
#ifdef SPANDB_STAT
      auto s_time = SPDK_TIME;
      bool hit = false;
#endif
      uint64_t buffer_index = offset / FILE_BUFFER_ENTRY_SIZE;
      uint64_t buffer_offset = offset % FILE_BUFFER_ENTRY_SIZE;
      assert(buffer_index < file_buffer_num_);
      if(buffer_index >= file_buffer_num_) {
        fprintf(stderr, "buffer_index: %lu, file_buffer_num_: %lu\n", buffer_index, file_buffer_num_);
      }
      if(file_buffer_[buffer_index] == nullptr) {
        fprintf(stderr, "file_buffer_[buffer_index] == nullptr 44\n");
        fprintf(stderr, "nullptr %s %s\n", fname_.c_str(), metadata_->fname_.c_str());
      }
      FileBuffer *buffer = file_buffer_[buffer_index];
      uint64_t lpn = buffer->start_lpn_;
      char *data = nullptr;
      std::shared_ptr<LRUEntry> value;
      if (topfs_cache->Find(convert_lpn(lpn, sp_env_->id()), value)) {
        data = value->data;
#ifdef SPANDB_STAT
        hit = true;
#endif
      } else {
        //std::lock_guard<std::mutex> lk(buffer->mutex_);
        buffer->mutex_.lock();
        if (topfs_cache->Find(convert_lpn(lpn, sp_env_->id()), value)) {
          data = value->data;
#ifdef SPANDB_STAT
          hit = true;
#endif
        } else {
          assert(buffer->data_ == nullptr);
          buffer->AllocateSPDKBuffer();
          //if (thread_local_info_.GetName() == "Worker") {
            //lemma spandb_controller_.IncreaseLDBgIO(FILE_BUFFER_ENTRY_SIZE);
          //}
#ifdef SPANDB_STAT
          auto sss = SPDK_TIME;
#endif
          SPDKRead(sp_env_, buffer);
#if SLEEP_LEMMA
          uint64_t check_count = 0;
#endif
          while (!buffer->readable_) {
            CheckComplete(sp_env_, buffer->queue_id_);
#if SLEEP_LEMMA
            if(check_count++ > 1000000) {
              check_count = 0;
              usleep(100);
            }
#endif
          }
#ifdef SPANDB_STAT
          read_disk_latency_.add(
              SPDK_TIME_DURATION(sss, SPDK_TIME, spdk_tsc_rate_));
#endif
          value.reset(new LRUEntry(lpn, buffer->data_));
          topfs_cache->Insert(convert_lpn(lpn, sp_env_->id()), value);
          data = buffer->data_;
          buffer->data_ = nullptr;
        }
        buffer->mutex_.unlock();
      }
      assert(data != nullptr);
      // copy to ptr
      size_t s = 0;
      if (buffer_offset + n <= FILE_BUFFER_ENTRY_SIZE) {
        s = n;
      } else {
        s = FILE_BUFFER_ENTRY_SIZE - buffer_offset;
      }
      memcpy(ptr + size, data + buffer_offset, s);
      size += s;
      offset += s;
      n -= s;
      if(!is_seq){
#ifdef SPANDB_STAT
        if (hit) {
          __sync_fetch_and_add(&read_hit_, 1);
          read_hit_latency_.add(
              SPDK_TIME_DURATION(s_time, SPDK_TIME, spdk_tsc_rate_));
        } else {
          __sync_fetch_and_add(&read_miss_, 1);
          read_miss_latency_.add(SPDK_TIME_DURATION(s_time, SPDK_TIME, spdk_tsc_rate_));
        }
#endif
      }
    }
    assert(n == 0);
    *result = Slice(scratch, size);
#ifdef SPANDB_STAT
    read_latency_.add(SPDK_TIME_DURATION(start, SPDK_TIME, spdk_tsc_rate_));
#endif
    return IOStatus::OK();
  }

#endif

  Status Write(uint64_t offset, const Slice &data) {
    if(offset) printf("\n");
    if(data.size()) printf("\n");
    fprintf(stderr, "%s:%d: SPDK File Write() not implemented\n", __FILE__,
            __LINE__);
    return Status::OK();
  }

  IOStatus Append(const Slice &data) {
    //auto start = SPDK_TIME;
    metadata_->last_access_time_ = SPDK_TIME;
    size_t left = data.size();
    size_t written = 0;
    //fprintf(stderr, "spdkfile append %s\n", fname_.c_str());
    while (left > 0) {
      if (UNLIKELY((uint64_t)current_buf_index_ >= file_buffer_num_)) {
        fprintf(stderr,
                "File size is too big. Allocate size: %llu, Current size: %lu, "
                "write size: %lu(left: %lu)\n",
                file_buffer_num_ * FILE_BUFFER_ENTRY_SIZE, metadata_->size_,
                data.size(), left);
        return IOStatus::IOError("File is oversize when appending");
      }
      //fprintf(stderr, "fname: %s, write size: %ld, buf index: %lu\n",fname_.c_str(),
      //     data.size(), current_buf_index_);
      const char *dat = data.data() + written;
      if (file_buffer_[current_buf_index_]->data_ == nullptr) {
        IOStatus s = file_buffer_[current_buf_index_]->AllocateSPDKBuffer();
        if (!s.ok()){
          fprintf(stderr, "%s error\n", __FUNCTION__);
          return s;
        }
        allocated_buffer.fetch_add(1);
      }
      size_t size = file_buffer_[current_buf_index_]->Append(dat, left);
      file_buffer_[current_buf_index_]->readable_ = true;
      left -= size;
      written += size;
      assert(left == 0 || file_buffer_[current_buf_index_]->Full());
      if (file_buffer_[current_buf_index_]->Full()) {
        SPDKWrite(sp_env_, file_buffer_[current_buf_index_]);
        if (current_buf_index_ - last_sync_index_ - 1 >= 10) {
#if SLEEP_LEMMA
          uint64_t check_count = 0;
#endif
          while (!file_buffer_[last_sync_index_]->synced_) {
            int queue_id = file_buffer_[last_sync_index_]->queue_id_;
            CheckComplete(sp_env_, queue_id);
#if SLEEP_LEMMA
            if(check_count++ > 1000000) {
              check_count = 0;
              usleep(100);
            }
#endif
          }
          file_buffer_[last_sync_index_]->buffer_cache_.reset(new LRUEntry(file_buffer_[last_sync_index_]->start_lpn_, file_buffer_[last_sync_index_]->data_));
/*#if LEVEL_CACHE
          topfs_cache->Insert(convert_lpn(file_buffer_[last_sync_index_]->start_lpn_, sp_env_->id()), file_buffer_[last_sync_index_]->buffer_cache_, level_);
#else
          topfs_cache->Insert(convert_lpn(file_buffer_[last_sync_index_]->start_lpn_, sp_env_->id()), file_buffer_[last_sync_index_]->buffer_cache_);
#endif*/
          file_buffer_[last_sync_index_]->data_ = nullptr;
          file_buffer_[last_sync_index_]->buffer_cache_ = nullptr;
          last_sync_index_++;
        }
        current_buf_index_++;
        /*if (current_buf_index_ - last_sync_index_ - 1 >= 4) {
          Fsync(false);
        }*/
      }
    }
#ifdef PRINT_STAT
    if (is_flush_) {
      total_flush_written_.fetch_add(data.size());
    } else {
      total_compaction_written_.fetch_add(data.size());
    }
    if (spdk_start_time_.load() == 0) {
      spdk_start_time_.store(SPDK_TIME);
      last_print_compaction_time_.store(SPDK_TIME);
      last_print_flush_time_.store(SPDK_TIME);
    } else {
      if (SPDK_TIME_DURATION(last_print_compaction_time_.load(), SPDK_TIME,
                             spdk_tsc_rate_) > 1000000) {
        print_mutex_.lock();
        fprintf(stderr, "compaction: %s %.2lf\n", ssdlogging::GetDayTime().c_str(),
               total_compaction_written_.load() * 1.0 / 1024 / 1024);
        fprintf(stderr, "flush: %s %.2lf\n", ssdlogging::GetDayTime().c_str(),
               total_flush_written_.load() * 1.0 / 1024 / 1024);
        last_print_compaction_time_.store(SPDK_TIME);
        print_mutex_.unlock();
      }
    }
#endif
    assert(left == 0);
    metadata_->modified_time_ = Now();
    metadata_->size_ += data.size();
    //write_delay_.add(SPDK_TIME_DURATION(start, SPDK_TIME, spdk_tsc_rate_));
    return IOStatus::OK();
  }

  IOStatus Fsync(bool write_last) {
    int sync_last = current_buf_index_;
    if (write_last && !file_buffer_[current_buf_index_]->Empty()) {
      assert(!file_buffer_[current_buf_index_]->synced_);
      SPDKWrite(sp_env_, file_buffer_[current_buf_index_]);
    } else {
      sync_last--;
    }
    for (int64_t i = last_sync_index_; i <= sync_last; i++) {
#if SLEEP_LEMMA
      uint64_t check_count = 0;
#endif
      while (!file_buffer_[i]->synced_) {
        int queue_id = file_buffer_[i]->queue_id_;
        CheckComplete(sp_env_, queue_id);
#if SLEEP_LEMMA
        if(check_count++ > 1000000) {
          check_count = 0;
          usleep(100);
        }
#endif
      }
    }
    return IOStatus::OK();
  }
  /*IOStatus Fsync(bool write_last) {
    // return Status::OK();
    // write [last_sync_index_ + 1, current_buf_index_ - 1]
    int sync_start = last_sync_index_ + 1;
    while (last_sync_index_ + 1 < current_buf_index_) {
      last_sync_index_++;
      assert(!file_buffer_[last_sync_index_]->synced_);
      SPDKWrite(sp_info_, file_buffer_[last_sync_index_]);
    }
    // write last buffer
    if (write_last && !file_buffer_[current_buf_index_]->Empty()) {
      assert(!file_buffer_[current_buf_index_]->synced_);
      SPDKWrite(sp_info_, file_buffer_[current_buf_index_]);
      last_sync_index_++;
    }
    // wait for finish //lemma may be splite
    for (int64_t i = sync_start; i <= last_sync_index_; i++) {
      while (!file_buffer_[i]->synced_) {
        int queue_id = file_buffer_[i]->queue_id_;
        CheckComplete(sp_info_, queue_id);
      }
    }
    return IOStatus::OK();
  }*/

  IOStatus Close() {
    //WriteMeta(&file_meta_);
    return IOStatus::OK();
  }

  void Ref() {
    refs_++;
#if PIN_L0
    refs_hist_++;
#endif
    assert(refs_ > 0);
  }

  void Unref() {
    MutexLock lock(&(sp_env_->fs_mutex_));
    refs_--;
    assert(refs_ >= 0);
    if (refs_ == 0) {
      sp_env_->spdk_file_system_.erase(fname_);
      delete this;
    }
  }

  void UnrefWithLock() {
    refs_--;
    assert(refs_ >= 0);
    if (refs_ == 0) {
      sp_env_->spdk_file_system_.erase(fname_);
      delete this;
    }
  }

#if PIN_L0
  void UnrefConditionalDelete() {
    MutexLock lock(&(sp_env_->fs_mutex_));
    refs_--;
    assert(refs_ >= 0);
    if (refs_ == 0 && refs_hist_ > 1) {
      sp_env_->spdk_file_system_.erase(fname_);
      delete this;
    }
  }
#endif

  uint64_t ModifiedTime() const { return metadata_->modified_time_; }

  int64_t GetRef() { return refs_; }

  std::string GetName() { return fname_; }

  SPDKFileMeta *GetMetadata() { return metadata_; }

  void reset_last_seq() { last_prefetch_index_ = 0; refs_hist_++; }

 private:
  uint64_t Now() {
    int64_t unix_time = 0;
    auto s = env_->GetCurrentTime(&unix_time);
    assert(s.ok());
    return static_cast<uint64_t>(unix_time);
  }

  Env *env_;
  FileBuffer **file_buffer_;
  const std::string fname_;
  mutable port::Mutex mutex_;
  bool locked_;
  Random rnd_;
  int64_t current_buf_index_;
  int64_t last_sync_index_;
  std::atomic<int64_t> refs_;
#if PIN_L0
  std::atomic<int64_t> refs_hist_;
#endif
  SPDKFileMeta *metadata_;
  uint64_t file_buffer_num_;
  SpdkEnv *sp_env_;
  ssdlogging::SPDKInfo *sp_info_;
  bool is_flush_;
  std::atomic<int> allocated_buffer;
#if SMART_CACHE
  bool is_sst_;
  int64_t first_read_unsync_index_;
  int64_t last_prefetch_index_;
  uint64_t start_lpn_;
#endif
#if LEVEL_CACHE
  int level_;
#endif
};

class SpdkDirectory : public FSDirectory {
 public:
  //explicit PosixDirectory(int fd) : fd_(fd) {}
  //~PosixDirectory();
  IOStatus Fsync(const IOOptions& /*opts*/, IODebugContext* /*dbg*/) override {
    return IOStatus::OK();
  }
};

class SpdkSequentialFile : public FSSequentialFile {
 public:
  explicit SpdkSequentialFile(SpdkFile *file, const FileOptions& option) : file_(file), offset_(0), size_(file_->GetMetadata()->size_), use_direct_reads_(option.use_direct_reads) {
    file_->Ref();
    file_->GetMetadata()->SetReadable(true);
  }

  ~SpdkSequentialFile() override {
    //fprintf(stderr, "~SpdkSequentialFile() %s\n", file_->GetName().c_str());
#if SMART_CACHE
    file_->FinishSeqPrefetch();
#endif
#if PIN_L0
    file_->UnrefConditionalDelete();
#else
    file_->Unref();
#endif
  }

  IOStatus Read(size_t n, const IOOptions& /**/, Slice *result,
              char *scratch, IODebugContext* /**/) override {
    size_t read_bytes = n;
    if(offset_ + n > size_) {
      read_bytes = size_ - offset_;
    }
    IOStatus s;
    if (use_direct_reads_) {
      s = file_->DIO_Read(offset_, read_bytes, result, scratch);
    } else {
#if SMART_CACHE
      s = file_->SeqPrefetch(offset_, read_bytes, result, scratch);
#else
      s = file_->Read(offset_, read_bytes, result, scratch);
#endif
    }
    offset_ += read_bytes;
    return s;
  }

  IOStatus Skip(uint64_t n) override {
    if(offset_ + n > size_)
      offset_ = size_;
    else
      offset_ += n;
    return IOStatus::OK();
  }

 private:
  SpdkFile *file_;
  uint64_t offset_;
  uint64_t size_;
  bool use_direct_reads_;
};

class SpdkRandomAccessFile : public FSRandomAccessFile {
 public:
  explicit SpdkRandomAccessFile(SpdkFile *file) : file_(file) {
    file_->Ref();
    file_->GetMetadata()->SetReadable(true);
  }

  ~SpdkRandomAccessFile() override {
    //fprintf(stderr, "~SpdkRandomAccessFile() %s\n", file_->GetName().c_str());
    file_->Unref();
  }

  IOStatus Read(uint64_t offset, size_t n, const IOOptions& /**/, Slice *result,
              char *scratch, IODebugContext* /**/) const override {
    return file_->Read(offset, n, result, scratch);
  }

#if SMART_CACHE
  IOStatus Prefetch(uint64_t offset, size_t n, const IOOptions& /*opts*/, IODebugContext* /*dbg*/) {
    IOStatus s = file_->Prefetch(offset, n);
    return s;
  }
#endif

 private:
  SpdkFile *file_;
};

class SpdkWritableFile : public FSWritableFile {
 public:
  SpdkWritableFile(SpdkFile *file, RateLimiter *rate_limiter)
      : file_(file), rate_limiter_(rate_limiter) {
    file_->Ref();
  }

  ~SpdkWritableFile() override {
    //fprintf(stderr, "~SpdkWritableFile() %s\n", file_->GetName().c_str());
    file_->Unref();
  }

  IOStatus Append(const Slice &data, const IOOptions& /**/, IODebugContext* /**/) override {
    size_t bytes_written = 0;
    while (bytes_written < data.size()) {
      auto bytes = RequestToken(data.size() - bytes_written);
      IOStatus s = file_->Append(Slice(data.data() + bytes_written, bytes));
      if (!s.ok()) {
        fprintf(stderr, "%s error \n", __FUNCTION__);
        return s;
      }
      bytes_written += bytes;
    }
    return IOStatus::OK();
  }

  IOStatus Append(const Slice &data, const IOOptions& /**/, const DataVerificationInfo& /**/, IODebugContext* /**/) override {
    size_t bytes_written = 0;
    while (bytes_written < data.size()) {
      auto bytes = RequestToken(data.size() - bytes_written);
      IOStatus s = file_->Append(Slice(data.data() + bytes_written, bytes));
      if (!s.ok()) {
        fprintf(stderr, "%s error \n", __FUNCTION__);
        return s;
      }
      bytes_written += bytes;
    }
    return IOStatus::OK();
  }

  IOStatus Allocate(uint64_t /*offset*/, uint64_t /*len*/, const IOOptions& /**/, IODebugContext* /**/) override {
    return IOStatus::OK();
  }

  IOStatus Truncate(uint64_t size, const IOOptions& /**/, IODebugContext* /**/) override {
    file_->Truncate(static_cast<size_t>(size));
    return IOStatus::OK();
  }
  IOStatus Close(const IOOptions& /**/, IODebugContext* /**/) override {
    return file_->Close();
  }

  IOStatus Flush(const IOOptions& /**/, IODebugContext* /**/) override { return IOStatus::OK(); }

  IOStatus Sync(const IOOptions& /**/, IODebugContext* /**/) override {
    return file_->Fsync(true);
    IOStatus::OK();
  }

  uint64_t GetFileSize(const IOOptions& /**/, IODebugContext* /**/) override { return file_->Size(); }

 private:
  inline size_t RequestToken(size_t bytes) {
    if (rate_limiter_ && io_priority_ < Env::IO_TOTAL) {
      bytes = std::min(
          bytes, static_cast<size_t>(rate_limiter_->GetSingleBurstBytes()));
      rate_limiter_->Request(bytes, io_priority_);
    }
    return bytes;
  }

  SpdkFile *file_;
  RateLimiter *rate_limiter_;
};

__attribute__((unused)) static void TopFSResetStat(){
#ifdef SPANDB_STAT
  read_hit_ = read_miss_ = 0;
  //L0_hit = L0_count = 0;
  free_list_latency_.reset();
  write_latency_.reset();
  read_latency_.reset();
  read_miss_latency_.reset();
  read_hit_latency_.reset();
  read_disk_latency_.reset();
  memcpy_latency_.reset();
  test_latency_.reset();
#endif
  topfs_cache->ResetStat();
}

static void Exit() {
  /*// 1.free meta and filesystem
  for (auto it = spdk_file_system_.begin(); it != spdk_file_system_.end();) {
    auto next = ++it;
    it--;
    assert(it->second != nullptr);
    it->second->Unref();
    it = next;
  }
  // 3.delete metadata
  for (auto &meta : file_meta_) {
    assert(meta.second != nullptr);
    delete meta.second;
  }*/
  // 4.free mem pool
  if (topfs_cache != nullptr) {
    delete topfs_cache;
  }
  if (spdk_mem_pool_ != nullptr) {
    delete spdk_mem_pool_;
  }
  if (huge_pages_ != nullptr) {
    delete huge_pages_;
  }
  /*// 5.free mutex
  if (spdk_queue_mutexes_ != nullptr) {
    for (int i = 0; i < total_queue_num_; i++) {
      delete spdk_queue_mutexes_[i];
    }
    delete spdk_queue_mutexes_;
  }*/
  // 6. output
  if (FILE_BUFFER_ENTRY_SIZE > (1ull << 20)) {
    printf("FILE_BUFFER_ENTRY_SIZE: %.2lf MB\n",
           FILE_BUFFER_ENTRY_SIZE / (1024 * 1024 * 1.0));
  } else {
    printf("FILE_BUFFER_ENTRY_SIZE: %.2lf KB\n",
           FILE_BUFFER_ENTRY_SIZE / (1024 * 1.0));
  }
#ifdef SPANDB_STAT
  printf("--------------L0 env---------------------\n");
  printf("write latency: %ld %.2lf us\n", write_latency_.size(),
         write_latency_.avg());
  printf("read latency: %ld %.2lf us\n", read_latency_.size(),
         read_latency_.avg());
  printf("read miss latency: %ld %.2lf us\n", read_miss_latency_.size(),
         read_miss_latency_.avg());
  printf("read hit latency: %ld %.2lf us\n", read_hit_latency_.size(),
         read_hit_latency_.avg());
  printf("read from disk: %ld %.2lf us\n", read_disk_latency_.size(),
         read_disk_latency_.avg());
  if (read_hit_ + read_miss_ != 0) {
    printf("read hit: %ld, read miss: %ld\n", read_hit_, read_miss_);
    printf("read hit ratio: %lf\n",
           (read_hit_ * 1.0) / (read_hit_ + read_miss_));
  }
  printf("free_list_latency: %ld %.2lf\n", free_list_latency_.size(),
         free_list_latency_.avg());
  printf("memcpy latency: %ld %.2lf\n", memcpy_latency_.size(),
         memcpy_latency_.avg());
  printf("test latency: %ld %.2lf\n", test_latency_.size(),
         test_latency_.avg());
  printf("------------------------------------------\n");
  TopFSResetStat();
#endif
}



__attribute__((unused)) static std::string NormalizeFilePath(const std::string path) {
  std::string dst;
  for (auto c : path) {
    if (!dst.empty() && c == '/' && dst.back() == '/') {
      continue;
    }
    dst.push_back(c);
  }
  return dst;
}

__attribute__((unused)) static int HashPath(const std::string path) {
  return path[20] - '0';
}

}  // namespace rocksdb
