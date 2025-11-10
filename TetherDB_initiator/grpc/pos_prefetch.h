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
#include "lemma.h"
#include "grpc/prefetch.grpc.pb.h"
#include <tbb/concurrent_queue.h>

using grpc::Channel;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::ClientAsyncResponseReader;
using grpc::Status;
using prefetch::PrefetchRequest;
using prefetch::PrefetchMsg;
using prefetch::PrefetchReply;
using prefetch::Prefetcher;

class PrefetchClient {
 public:
  PrefetchClient(std::string target_str, uint32_t subsys_id, uint32_t ns_id);

  void PrefetchData(uint64_t pba);

 private:
  void AsyncCompleteRpc();
  void RequestTread();

  struct AsyncClientCall {
    PrefetchReply reply;
    ClientContext context;
    Status status;
    std::unique_ptr<ClientAsyncResponseReader<PrefetchReply>> response_reader;
  };

  std::unique_ptr<Prefetcher::Stub> stub_;
  CompletionQueue cq_;
  uint32_t subsys_id_;
  uint32_t ns_id_;
  tbb::concurrent_queue<uint64_t> queue_;
  std::mutex mutex_;
};
