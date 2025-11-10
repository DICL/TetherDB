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

#include "grpc/pos_prefetch.h"
#include "monitoring/thread_status_util.h"
#include <time.h>

PrefetchClient::PrefetchClient(std::string target_str, uint32_t subsys_id, uint32_t ns_id)
  : stub_(Prefetcher::NewStub(grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials()))),
    subsys_id_(subsys_id),
    ns_id_(ns_id) {
  std::thread thread_ = std::thread(&PrefetchClient::AsyncCompleteRpc, this);
  std::thread request_thread_ = std::thread(&PrefetchClient::RequestTread, this);
  thread_.detach();
  request_thread_.detach();
  srand(time(NULL));
}

void PrefetchClient::PrefetchData(uint64_t pba) {
  queue_.push(pba);
}

void PrefetchClient::RequestTread() {
  uint64_t pba;
  std::set<uint64_t> pbas;
  while(1) {
    if(queue_.try_pop(pba)) {
      //if (rand() % 100 > 99) {
        //pba = (pba / 32) * 32;
        pbas.insert(pba);
        if (pbas.size() > 1000) {
          PrefetchRequest req;
          for(auto iter = pbas.begin(); iter != pbas.end(); iter++) {
            if (*iter != pba) {
              PrefetchMsg* msg = req.add_msgs();
              msg->set_subsys_id(subsys_id_);
              msg->set_ns_id(ns_id_);
              msg->set_pba(*iter * 4096);
            }
          }
          AsyncClientCall* call = new AsyncClientCall;
          call->response_reader =
            stub_->PrepareAsyncPrefetchData(&call->context, req, &cq_);
          call->response_reader->StartCall();
          call->response_reader->Finish(&call->reply, &call->status, (void*)call);
          pbas.clear();
          pbas.insert(pba);
        }
      //}
    } else {
      usleep(10);
    }
  }
}

void PrefetchClient::AsyncCompleteRpc() {
  void* got_tag;
  bool ok = false;
  while (cq_.Next(&got_tag, &ok)) {
    AsyncClientCall* call = static_cast<AsyncClientCall*>(got_tag);
    GPR_ASSERT(ok);
    if (!call->status.ok())
      fprintf(stderr, "POS Prefetch failed! \n");
    delete call;
  }
}

