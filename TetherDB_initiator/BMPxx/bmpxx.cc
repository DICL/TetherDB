#include "BMPxx/bmpxx.h"
#include "rocksdb/db.h"
#include "rocksdb/table.h"
#include "rocksdb/options.h"
#include "rocksdb/file_system.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"
#include "rocksdb/metadata.h"

#include <iostream>
#include <iterator>
#include <fstream>
#include <vector>
#include <algorithm> // for std::copy
#include <iomanip>
#include <time.h>
#include <thread>
#include <sys/time.h>
#include <random>

#include <fcntl.h>
#include <unistd.h>
#include <vector>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>

#define NUM_AI_THREAD 4
#define EP 5
#define TOKEN 0
#define SPLIT 0
#if ROCKSDB_SPDK
#define SPDKFS 1
#else
#define SPDKFS 0
#endif

#if SPLIT
rocksdb::GreeterClient* greeter2;
#endif

std::string GetDayTime_t(struct timeval now_tv) {
  const int kBufferSize = 100;
  char buffer[kBufferSize];
  gettimeofday(&now_tv, nullptr);
  const time_t seconds = now_tv.tv_sec;
  struct tm t;
  localtime_r(&seconds, &t);
  snprintf(buffer, kBufferSize,
            "%04d/%02d/%02d-%02d:%02d:%02d.%06d",
            t.tm_year + 1900,
            t.tm_mon + 1,
            t.tm_mday,
            t.tm_hour,
            t.tm_min,
            t.tm_sec,
            static_cast<int>(now_tv.tv_usec));
  return std::string(buffer);
}

#if SPDKFS
void remote_preprocess(std::vector<std::string> &input_filenames, rocksdb::Options &options, rocksdb::GreeterClient* greeter, long &sec, long &usec) {
#else
void remote_preprocess(std::vector<std::string> &input_filenames, rocksdb::Options &/*options*/, rocksdb::GreeterClient* greeter, long &sec, long &usec) {
#endif
  compaction_data::PreprocessRequest requests;
  compaction_data::PreprocessReply reply;
  sec = 0;
  usec = 0;
  for(auto input_filename : input_filenames) {
    compaction_data::PreprocessRequest::Req *req = requests.add_req();
#if SPDKFS
    uint64_t start_lpn;
    uint64_t end_lpn;
    options.spdk_fs->GetFileAddr(input_filename, &start_lpn, &end_lpn);
    uint64_t size;
    options.spdk_fs->GetFileSize(input_filename, rocksdb::IOOptions(), &size, nullptr);

    req->set_start_lpn(start_lpn);
    req->set_end_lpn(end_lpn);
    req->set_size(size);
#endif
    req->set_fname(input_filename);
  }
  greeter->Preprocess(requests, &reply);
  if (reply.bmps_size() == 0) {
#if TOKEN
    sec = reply.sec();
    usec = reply.usec();
#else
    sec = 1;
    usec = 1;
#endif
    /*struct timeval tt;
    tt.tv_sec = reply.sec();
    tt.tv_usec = reply.usec();
    printf("next time: %s\n", GetDayTime_t(tt).c_str());*/
  } /*else {
    std::ofstream outfile1("out_test1.txt", std::ios::binary);
    outfile1 << reply.bmps(5);
    outfile1.close();
  }*/
}

#if SPDKFS
void local_preprocess(std::vector<std::string> &input_filenames, rocksdb::Options &options) {
#else
void local_preprocess(std::vector<std::string> &input_filenames, rocksdb::Options &/*options*/) {
#endif
  for(auto input_filename : input_filenames) {
    std::vector<uint8_t> inputImage;
    const size_t blockSize = 4096*16; // 512, 1024, 4096
    void* buffer = nullptr;
    // 
    int ret = posix_memalign(&buffer, blockSize, blockSize);
    if(ret){
      fprintf(stderr, "posix_memalign: %s\n", strerror(ret));
      return;
    }

#if SPDKFS
    rocksdb::Slice slice_buffer;
    std::unique_ptr<rocksdb::FSSequentialFile> f;
    rocksdb::FileOptions foption;
    foption.use_direct_reads = true;
    options.spdk_fs->NewSequentialFile(input_filename, foption, &f, nullptr);
    rocksdb::Status status;
    uint64_t fileSize;
    options.spdk_fs->GetFileSize(input_filename, rocksdb::IOOptions(), &fileSize, nullptr);
    inputImage.reserve(fileSize); // 
    while(1) {
      status = f->Read(blockSize, rocksdb::IOOptions(), &slice_buffer, (char*)buffer, nullptr);
      if (status.ok())
        inputImage.insert(inputImage.end(), static_cast<uint8_t*>(buffer), static_cast<uint8_t*>(buffer) + slice_buffer.size());
      if (!status.ok() || slice_buffer.size() < static_cast<size_t>(blockSize)) break;
    }
#else
    int fd = open(input_filename.c_str(), O_RDONLY | O_DIRECT);
    if (fd == -1) {
        perror("open");
        return;
    }
    off_t fileSize = lseek(fd, 0, SEEK_END);
    if (fileSize == -1) {
      perror("lseek");
      close(fd);
      return;
    }
    lseek(fd, 0, SEEK_SET); //

    inputImage.reserve(fileSize); //

    ssize_t bytesRead;
    while ((bytesRead = read(fd, buffer, blockSize)) > 0) {
      inputImage.insert(inputImage.end(), static_cast<uint8_t*>(buffer), static_cast<uint8_t*>(buffer) + bytesRead);
    }

    if (bytesRead == -1) {
      perror("read");
      close(fd);
      free(buffer);
      return;
    }

    // 
    close(fd);
#endif
    free(buffer);

    //decode input
    auto decoded_input = bmpxx::bmp::decode(inputImage);
    std::vector<uint8_t> decodedImage = decoded_input.first;
    bmpxx::BmpDesc input_description = decoded_input.second;
    auto crop_result = bmpxx::bmp::crop(decodedImage, input_description, 0, 0, 256, 256);
    std::vector<uint8_t> cropImage = crop_result.first;
    bmpxx::BmpDesc crop_description = crop_result.second;

    auto rotImage = bmpxx::bmp::rotate(cropImage, crop_description, 10);

    std::vector<uint8_t> outputImage = bmpxx::bmp::encode(rotImage, crop_description);
  }
}

/*void preprocess(std::vector<std::string> &input_filenames, rocksdb::Options &options, bool is_remote) {
  if (is_remote) {
    int remote_result = 0;
    size_t i = 0;
    std::vector<std::string> remote_filenames;
    std::vector<std::string> local_filenames;
    for(; i < input_filenames.size()/2; i++) {
      remote_filenames.push_back(input_filenames[i]);
    }
    std::thread t(remote_preprocess, std::ref(remote_filenames), std::ref(options), std::ref(remote_result));
    for(; i<input_filenames.size(); i++) {
      local_filenames.push_back(input_filenames[i]);
    }
    local_preprocess(local_filenames, options);
    t.join();
    if (remote_result) {
      local_preprocess(remote_filenames, options);
    }
  } else {
    local_preprocess(input_filenames, options);
  }
}*/

std::string GetDayTime() {
  const int kBufferSize = 100;
  char buffer[kBufferSize];
  struct timeval now_tv;
  gettimeofday(&now_tv, nullptr);
  const time_t seconds = now_tv.tv_sec;
  struct tm t;
  localtime_r(&seconds, &t);
  snprintf(buffer, kBufferSize,
            "%04d/%02d/%02d-%02d:%02d:%02d.%06d",
            t.tm_year + 1900,
            t.tm_mon + 1,
            t.tm_mday,
            t.tm_hour,
            t.tm_min,
            t.tm_sec,
            static_cast<int>(now_tv.tv_usec));
  return std::string(buffer);
}

int main(int ac, char **av)
{
  rocksdb::Options options;
  std::string dbpath("/lemma/rocksdb");
  std::string data_path("/lemma/Images/openimages/test");
  options.wal_dir = dbpath;
  double total_time = 0.0;
  int total_file_num = 4492;
#if TOKEN
  struct timeval cur;
#endif
  long next_sec = 0;
  long next_usec = 0;

  bool is_m = (ac > 1 && av[1][0] == 'm');
  bool is_remote = (ac > 1 && av[1][0] == 'r');
  bool og_is_remote = (ac > 1 && av[1][0] == 'r');
  std::string nvme_addr = "trtype:RDMA adrfam:IPv4 traddr:11.0.0.91 trsvcid:4521 subnqn:nqn.2019-04.pos:subsystem";
  /*if (ac > 2) {
    if(atoi(av[2]) < 5)
      nvme_addr = "trtype:RDMA adrfam:IPv4 traddr:10.0.0.90 trsvcid:4421 subnqn:nqn.2019-04.pos:subsystem";
    nvme_addr += av[2];
  } else {*/
    nvme_addr += "7";
  //}
#if SPDKFS
  if (!is_m)
    options.spdk_open_mode = 2;
  options.SetSpdkEnv(nvme_addr);
#endif
  if (is_remote) {
    options.greeter = new rocksdb::GreeterClient("11.0.0.91:50052");
#if SPLIT
    greeter2 = new rocksdb::GreeterClient("11.0.0.9:50052");
#endif
  }
#if SPDKFS
  if (is_m)
    options.spdk_fs->SpdkMigration(dbpath, data_path);
#endif

  rocksdb::DB* db;
  rocksdb::Status s = rocksdb::DB::Open(options, dbpath, &db);

  if (is_m)
    return 0;
  if(!db) printf("db nullptr\n");

  printf("start time: %s\n", GetDayTime().c_str());
  /*std::vector<int> numbers(total_file_num);
  std::random_device rd;
  std::mt19937 g(rd());
  for(int i = 0; i < total_file_num; i++)
    numbers[i] = i;*/
  int batch_size = 64;

  int per=0;
  if (ac > 2)
    per = atoi(av[2]);
  int remote_num = (batch_size*per)/10;
  for(int k = 0; k < EP; k++) {
    struct timespec  begin, end;
    //std::shuffle(numbers.begin(), numbers.end(), g);
    //fprintf(stderr, "%d %d\n", numbers[0], numbers[1]);
    clock_gettime(CLOCK_MONOTONIC, &begin);

    int total = 0;

    while (total < total_file_num) {
      /*std::vector<std::string> input_filenames;
      for(int i = 0; i < batch_size; i++) {
        input_filenames.push_back("/lemma/rocksdb/test" + std::to_string(total++) + ".bmp");
        if (total > 4996)
          break;
      }*/
      int remain = batch_size;
      std::vector<std::string> remote_inputs; 
      std::vector<std::string> remote_inputs2; 
      std::thread remote_t;
#if SPLIT
      std::thread remote_t2;
#endif

#if TOKEN
      if (og_is_remote && !is_remote) {
        gettimeofday(&cur, nullptr);
        if (cur.tv_sec >= next_sec && cur.tv_usec >= next_usec) {
          is_remote = true;
        }
      }
#endif
      if (is_remote) {
#if SPLIT
        for(int i = 0; i < remote_num/2; i++) {
          remote_inputs.push_back("/lemma/AI3/test" + std::to_string(total++) + ".bmp");
          remain--;
        }
        remote_t = std::thread(remote_preprocess, std::ref(remote_inputs), std::ref(options), options.greeter, std::ref(next_sec), std::ref(next_usec));
        for(int i = remote_num/2; i < remote_num; i++) {
          remote_inputs2.push_back("/lemma/AI3/test" + std::to_string(total++) + ".bmp");
          remain--;
        }
        remote_t2 = std::thread(remote_preprocess, std::ref(remote_inputs2), std::ref(options), greeter2, std::ref(next_sec), std::ref(next_usec));
#else
        for(int i = 0; i < remote_num; i++) {
          remote_inputs.push_back("/lemma/AI3/test" + std::to_string(total++) + ".bmp");
          remain--;
        }
        remote_t = std::thread(remote_preprocess, std::ref(remote_inputs), std::ref(options), options.greeter, std::ref(next_sec), std::ref(next_usec));
#endif
      }

      std::vector<std::vector<std::string>> inputs(NUM_AI_THREAD);
      std::vector<std::thread> t;
      for(int i = 0; i < NUM_AI_THREAD; i++) {
        int tmp = remain/(NUM_AI_THREAD-i);
        for(int j = 0; j < tmp; j++) {
          inputs[i].push_back("/lemma/AI3/test" + std::to_string(total++) + ".bmp");
          remain--;
          if (total >= total_file_num)
            break;
        }
        if (total >= total_file_num)
          break;
      }
      for(int i = 0; i < NUM_AI_THREAD; i++) {
        t.push_back(std::thread(local_preprocess, std::ref(inputs[i]), std::ref(options)));
      }
      for(int i = 0; i < NUM_AI_THREAD; i++) {
        t[i].join();
      }
      if (is_remote) {
        remote_t.join();
#if SPLIT
        remote_t2.join();
#endif
      }
      if (next_sec) {
#if TOKEN
        is_remote = false;
#endif
        std::vector<std::thread> t2;
        remain = remote_inputs.size();
        int remote_tmp = 0;
        std::vector<std::vector<std::string>> inputs2(NUM_AI_THREAD);
        for(int i = 0; i < NUM_AI_THREAD; i++) {
          int tmp = remain/(NUM_AI_THREAD-i);
          for(int j = 0; j < tmp; j++) {
            inputs2[i].push_back(remote_inputs[remote_tmp++]);
            remain--;
          }
          t2.push_back(std::thread(local_preprocess, std::ref(inputs2[i]), std::ref(options)));
        }
        for(int i = 0; i < NUM_AI_THREAD; i++) {
          t2[i].join();
        }
      }
    }

    clock_gettime(CLOCK_MONOTONIC, &end);
    total_time += (end.tv_sec - begin.tv_sec) + (end.tv_nsec - begin.tv_nsec) / 1000000000.0;
    std::cout << (end.tv_sec - begin.tv_sec) + (end.tv_nsec - begin.tv_nsec) / 1000000000.0 << std::endl;
  }
  printf("end time: %s\n", GetDayTime().c_str());
  std::cout << "avg:" << total_time / EP << std::endl;
  if (og_is_remote) {
    options.greeter->ReturnToken();
#if SPLIT
    greeter2->ReturnToken();
#endif
  }
  return 0;
}
