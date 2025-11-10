/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
#pragma once

#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>
#include <stdio.h>
#include "grpc/compaction_data.grpc.pb.h"
#include "rocksdb/types.h"
#include "rocksdb/env.h"
#include "lemma.h"
//#include "db/compaction/compaction.h"
//#include "options/db_options.h"

namespace ROCKSDB_NAMESPACE {

struct FileMetaData;
class Compaction;
class VersionStorageInfo;

using grpc::Channel;
using grpc::ClientContext;
using compaction_data::CompactionRequest;
using compaction_data::CompactionReply;
using compaction_data::CacheRequest;
using compaction_data::CacheReply;
using compaction_data::Greeter;
using compaction_data::LocalCompactionRequest;
using compaction_data::LocalCompactionReply;
using compaction_data::PingTestRequest;
using compaction_data::PingTestReply;
using compaction_data::TokenRequest;
using compaction_data::TokenReply;

using compaction_data::PreprocessRequest;
using compaction_data::PreprocessReply;

#if REMOTE_RANGE
class MYSpinLock {
public:
    MYSpinLock() : flag_(false){}
    void lock(){
        bool expect = false;
        while (!flag_.compare_exchange_weak(expect, true)){
            expect = false;
        }
    }
    void unlock(){
        flag_.store(false);
    }

private:
    std::atomic<bool> flag_;
};
#endif

class GreeterClient {
 public:
  GreeterClient(std::string target_str);
  ~GreeterClient();

  std::vector<FileMetaData> SayHello(Compaction* c, int job_id, bool shutting_down,
          const SequenceNumber preserve_deletes_seqnum, SequenceNumber earliest_write_conflict_snapshot,
          Env::Priority thread_pri,
          const std::atomic<int>* manual_compaction_paused, const std::string& db_id, const std::string& db_session_id,
          std::string full_history_ts_low);

#if ROCKSDB_SPDK
  void Caching(FileMetaData* meta, uint64_t start_lpn, uint64_t end_lpn);
#else
  void Caching(FileMetaData* meta);
#endif

  void LocalCompactionResult(LocalCompactionRequest& request);

  void PingTest();

  void Preprocess(PreprocessRequest& request, PreprocessReply* reply);

  void ReturnToken();

#if REMOTE_RANGE
  bool CheckRange(Compaction* c);
#endif

 private:
  std::unique_ptr<Greeter::Stub> stub_;
  uint64_t num_num;
  uint64_t sum_num;

#if REMOTE_RANGE
  std::atomic<int> rejected_ = 0;
  std::string compaction_range_;
  MYSpinLock reject_range_lock_;
  int range_factor_ = 1;

  std::string GetMidSlice(std::string A, std::string B) {
    uint64_t min = A[4] - '0';
    uint64_t max = B[4] - '0';
    for(int i = 5; i < 12; i++) {
      min *= 10;
      max *= 10;
      min += A[i] - '0';
      max += B[i] - '0';
    }
    uint64_t mid = min + (max - min) / range_factor_;
    std::string result(12, '\0');
    for (int i = 11; i >= 4; i--) {
      result[i] = static_cast<char>(mid % 10 + '0');
      mid /= 10;
    }
    result[0] = 'u';
    result[1] = 's';
    result[2] = 'e';
    result[3] = 'r';
    fprintf(stderr, "%s\n", result.c_str());
    return result;
  }

/*  std::string GetMidSlice(std::string A, std::string B) {
    size_t minLength = std::min(A.size(), B.size());
    std::string trimmedA = A.substr(0, minLength);
    std::string trimmedB = B.substr(0, minLength);

    std::string median;
    for (size_t i = 0; i < minLength; ++i) {
      // 
      //char medianChar = (trimmedA[i] + trimmedB[i]) / 2;

      char smaller = std::min(trimmedA[i], trimmedB[i]);
      char larger = std::max(trimmedA[i], trimmedB[i]);

      char quarterValue = (larger - smaller) / 8;

      char medianChar = smaller + quarterValue;
      median += medianChar;
    }
    return median;
  }*/
#endif
};

}
