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

#include "grpc/grpc_lemma.h"
#include "monitoring/thread_status_util.h"
#include "options/db_options.h"
#include "db/compaction/compaction.h"

namespace ROCKSDB_NAMESPACE {

struct RangeWithSize {
  Range range;
  uint64_t size;

  RangeWithSize(const Slice& a, const Slice& b, uint64_t s = 0)
      : range(a, b), size(s) {}
};

#if REMOTE_RANGE
bool GreeterClient::CheckRange(Compaction* c) {
  auto cfd = c->column_family_data();
  reject_range_lock_.lock();
  if (compaction_range_.size() == 0) {
    range_factor_ = 8;
    VersionStorageInfo* vstorage = cfd->current()->storage_info();
    compaction_range_ = GetMidSlice(vstorage->GetSmallestUserKey().ToString(), vstorage->GetLargestUserKey().ToString());
  }
  int cmp_result = cfd->user_comparator()->Compare(c->GetLargestUserKey(), compaction_range_);
  reject_range_lock_.unlock();
  return (cmp_result <= 0);
}
#endif

GreeterClient::GreeterClient(std::string target_str) {
  auto cargs = grpc::ChannelArguments();
  cargs.SetMaxReceiveMessageSize(1024 * 1024 * 1024); // 1 GB
  cargs.SetMaxSendMessageSize(1024 * 1024 * 1024);
  stub_.reset(Greeter::NewStub(grpc::CreateCustomChannel(target_str, grpc::InsecureChannelCredentials(), cargs)).release());
  num_num = 0;
  sum_num = 0;
  TokenRequest request;
  TokenReply reply;
  ClientContext context;
  request.set_get(true);
  stub_->Token(&context, request, &reply);
}

void GreeterClient::ReturnToken() {
  TokenRequest request;
  TokenReply reply;
  ClientContext context;
  request.set_get(false);
  stub_->Token(&context, request, &reply);
}

void GreeterClient::PingTest() {
  PingTestRequest request;
  PingTestReply reply;
  ClientContext context;
  stub_->PingTest(&context, request, &reply);
}

void GreeterClient::LocalCompactionResult(LocalCompactionRequest& request) {
  LocalCompactionReply reply;
  ClientContext context;
  stub_->LocalCompactionResult(&context, request, &reply);
}

#if ROCKSDB_SPDK
void GreeterClient::Caching(FileMetaData* meta, uint64_t start_lpn, uint64_t end_lpn) {
#else
void GreeterClient::Caching(FileMetaData* meta) {
#endif
  //fprintf(stderr, "Caching %lu\n", meta->fd.GetNumber());
  CacheRequest request;
  request.set_smallest(meta->smallest.Encode().data());
  request.set_largest(meta->largest.Encode().data());
  request.set_file_size(meta->fd.file_size);
  request.set_num_entries(meta->num_entries);
  request.set_packed_number_and_path_id(meta->fd.packed_number_and_path_id);
  request.set_largest_seqno(meta->fd.largest_seqno);
  request.set_smallest_seqno(meta->fd.smallest_seqno);
#if ROCKSDB_SPDK
  request.set_start_lpn(start_lpn);
  request.set_end_lpn(end_lpn);
#endif
#if LEMMA_L0
  uint64_t* offsets = meta->log_sst->Offsets();
  uint64_t* footers = meta->log_sst->Footers();
  for(uint64_t i = 0; i < meta->num_entries; i++) {
    request.add_offset(offsets[i]);
    request.add_footer(footers[i]);
  }
#endif
  CacheReply reply;
  ClientContext context;
  stub_->Caching(&context, request, &reply);
}

std::vector<FileMetaData> GreeterClient::SayHello(Compaction* c, int job_id, bool shutting_down,
          const SequenceNumber preserve_deletes_seqnum, SequenceNumber earliest_write_conflict_snapshot,
          Env::Priority thread_pri,
          const std::atomic<int>* manual_compaction_paused, const std::string& db_id, const std::string& db_session_id,
          std::string full_history_ts_low) {
  size_t input_size = c->CalculateTotalInputSize();
  size_t result_file_count = input_size / c->max_output_file_size();
  /*if (c->start_level() == 0){
    num_num++;
    sum_num += result_file_count;
    fprintf(stderr, "%lu %lf\n", result_file_count, (double)sum_num/num_num);
  }*/
  //size_t max_subcompactions = result_file_count / 7 + 1;
  size_t max_subcompactions = 1;
  if(c->output_level() == 1) {
    max_subcompactions = c->max_subcompactions();
  }
  result_file_count += std::max((size_t)5, max_subcompactions*2 + 10);
  //if(max_subcompactions > 10)
  //  max_subcompactions = 10;
  if(c->output_level() == 0) {
    max_subcompactions = 1;
    result_file_count = 1;
  }
#if DEBUG_PRINT
  fprintf(stderr, "\nmax_subcompactions %lu\n", max_subcompactions);
#endif
  CompactionRequest request;
  request.set_total_len(c->TotalLen());
  request.set_base_index(c->BaseIndex());
  request.set_min_input_file_oldest_ancester_time_(c->MinInputFileOldestAncesterTime());
  request.set_output_level_(c->output_level());
  request.set_max_output_file_size_(c->max_output_file_size());
  //Version* input_version_;
  request.set_number_levels_(c->number_levels());
  //ColumnFamilyData* cfd_; //@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
  request.set_output_path_id_(c->output_path_id());
  request.set_output_compression_(c->output_compression());
  CompactionRequest::CompressionOptions* output_compression_opts = request.mutable_output_compression_opts_();
  output_compression_opts->set_window_bits(c->output_compression_opts().window_bits);
  output_compression_opts->set_level(c->output_compression_opts().level);
  output_compression_opts->set_strategy(c->output_compression_opts().strategy);
  output_compression_opts->set_max_dict_bytes(c->output_compression_opts().max_dict_bytes);
  output_compression_opts->set_zstd_max_train_bytes(c->output_compression_opts().zstd_max_train_bytes);
  output_compression_opts->set_parallel_threads(c->output_compression_opts().parallel_threads);
  output_compression_opts->set_enabled(c->output_compression_opts().enabled);
#if ROCKSDB_SPDK
  const ImmutableCFOptions* ioptions = c->column_family_data()->ioptions();
#endif

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  int start_lvl = c->start_level();
  std::vector<Slice> boundaries;
  if(max_subcompactions > 1 && start_lvl == 0) {
    auto* cfd = c->column_family_data();
    const Comparator* cfd_comparator = cfd->user_comparator();
    std::vector<Slice> bounds;
    int out_lvl = c->output_level();

    // Add the starting and/or ending key of certain input files as a potential
    // boundary
    for (size_t lvl_idx = 0; lvl_idx < c->num_input_levels(); lvl_idx++) {
      int lvl = c->level(lvl_idx);
      if (lvl >= start_lvl && lvl <= out_lvl) {
        const LevelFilesBrief* flevel = c->input_levels(lvl_idx);
        size_t num_files = flevel->num_files;

        if (num_files == 0) {
          continue;
        }

        if (lvl == 0) {
          // For level 0 add the starting and ending key of each file since the
          // files may have greatly differing key ranges (not range-partitioned)
          for (size_t i = 0; i < num_files; i++) {
            bounds.emplace_back(flevel->files[i].smallest_key);
            bounds.emplace_back(flevel->files[i].largest_key);
          }
        } else {
          // For all other levels add the smallest/largest key in the level to
          // encompass the range covered by that level
          bounds.emplace_back(flevel->files[0].smallest_key);
          bounds.emplace_back(flevel->files[num_files - 1].largest_key);
          if (lvl == out_lvl) {
            // For the last level include the starting keys of all files since
            // the last level is the largest and probably has the widest key
            // range. Since it's range partitioned, the ending key of one file
            // and the starting key of the next are very close (or identical).
            for (size_t i = 1; i < num_files; i++) {
              bounds.emplace_back(flevel->files[i].smallest_key);
            }
          }
        }
      }
    }

    std::sort(bounds.begin(), bounds.end(),
              [cfd_comparator](const Slice& a, const Slice& b) -> bool {
                return cfd_comparator->Compare(ExtractUserKey(a),
                                               ExtractUserKey(b)) < 0;
              });
    // Remove duplicated entries from bounds
    bounds.erase(
        std::unique(bounds.begin(), bounds.end(),
                    [cfd_comparator](const Slice& a, const Slice& b) -> bool {
                      return cfd_comparator->Compare(ExtractUserKey(a),
                                                     ExtractUserKey(b)) == 0;
                    }),
        bounds.end());

    // Combine consecutive pairs of boundaries into ranges with an approximate
    // size of data covered by keys in that range
    uint64_t sum = 0;
    std::vector<RangeWithSize> ranges;
    // Get input version from CompactionState since it's already referenced
    // earlier in SetInputVersioCompaction::SetInputVersion and will not change
    // when db_mutex_ is released below
    auto* v = c->input_version();
    for (auto it = bounds.begin();;) {
      const Slice a = *it;
      ++it;

      if (it == bounds.end()) {
        break;
      }

      const Slice b = *it;

      // ApproximateSize could potentially create table reader iterator to seek
      // to the index block and may incur I/O cost in the process. Unlock db
      // mutex to reduce contention
      uint64_t size = v->version_set()->ApproximateSize(SizeApproximationOptions(), v, a,
                                                 b, start_lvl, out_lvl + 1,
                                                 TableReaderCaller::kCompaction);
      ranges.emplace_back(a, b, size);
      sum += size;
    }

    // Group the ranges into subcompactions
    const double min_file_fill_percent = 4.0 / 5;
    int base_level = v->storage_info()->base_level();
    uint64_t max_output_files = static_cast<uint64_t>(std::ceil(
        sum / min_file_fill_percent /
        MaxFileSizeForLevel(*(c->mutable_cf_options()), out_lvl,
            c->immutable_cf_options()->compaction_style, base_level,
            c->immutable_cf_options()->level_compaction_dynamic_level_bytes)));
    uint64_t subcompactions =
        std::min({static_cast<uint64_t>(ranges.size()),
                  static_cast<uint64_t>(max_subcompactions),
                  max_output_files});
    max_subcompactions = subcompactions;

    if (subcompactions > 1) {
      double mean = sum * 1.0 / subcompactions;
      // Greedily add ranges to the subcompaction until the sum of the ranges'
      // sizes becomes >= the expected mean size of a subcompaction
      sum = 0;
      for (size_t i = 0; i + 1 < ranges.size(); i++) {
        sum += ranges[i].size;
        if (subcompactions == 1) {
          // If there's only one left to schedule then it goes to the end so no
          // need to put an end boundary
          continue;
        }
        if (sum >= mean) {
          boundaries.emplace_back(ExtractUserKey(ranges[i].range.limit));
////////////////////////////////////////////////////////////////((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((
          /*fprintf(stderr, "\n\nboundaries\n\n");
          for(size_t tmpi = 0; tmpi<boundaries.back().size(); tmpi++) {
            char tmp_c = boundaries.back().data()[tmpi];
            if((tmp_c <= 'z' && tmp_c >= 'a') || (tmp_c <= 'Z' && tmp_c >= 'A') || (tmp_c <= '9' && tmp_c >= '0'))
              fprintf(stderr, " %c", tmp_c);
            else
              fprintf(stderr, " |%d|", tmp_c);
          }
          fprintf(stderr, "\n\nboundaries end\n\n");*/
          request.add_boundaries(boundaries.back().ToString());
          request.add_boundaries_len(boundaries.back().size());
          subcompactions--;
          sum = 0;
        }
      }
    }
  } else {
    max_subcompactions = 1;
  }
#if DEBUG_PRINT
  fprintf(stderr, "max_subcompactions %lu\n\n", max_subcompactions);
#endif
  request.set_max_subcompactions_(max_subcompactions);
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////





  for (auto i : *c->inputs()){
    CompactionRequest::CompactionInputFiles* compaction_input_files = request.add_inputs_(); 
    compaction_input_files->set_level(i.level);
    for (auto j : i.files){
      CompactionRequest::FileMetaData* file_meta_data = compaction_input_files->add_files();
      file_meta_data->set_packed_number_and_path_id(j->fd.packed_number_and_path_id);
#if LEMMA_L0
      if(i.level == 0 && boundaries.size() > 0 && j->log_sst != nullptr) {
        for(auto& k : boundaries) {
          file_meta_data->add_indexs(j->log_sst->GetIndexOf(k));
        }
      }
#endif
#if ROCKSDB_SPDK == 0
      file_meta_data->set_num_entries(j->num_entries);
      file_meta_data->set_file_size(j->fd.file_size);
      file_meta_data->set_largest_seqno(j->fd.largest_seqno);
      file_meta_data->set_smallest(j->smallest.Encode().data());
      file_meta_data->set_largest(j->largest.Encode().data());
#endif
    }
    for (auto j : i.atomic_compaction_unit_boundaries){
      CompactionRequest::FileMetaData* atomic_compaction_unit_boundaries = compaction_input_files->add_atomic_compaction_unit_boundaries();
      atomic_compaction_unit_boundaries->set_smallest(j.smallest->Encode().data());
      atomic_compaction_unit_boundaries->set_largest(j.largest->Encode().data());
    }
  }
  for (auto i : c->grandparents()){
    CompactionRequest::FileMetaData* grandparent = request.add_grandparents_();
    grandparent->set_packed_number_and_path_id(i->fd.packed_number_and_path_id);
    grandparent->set_smallest(i->smallest.Encode().data());
    grandparent->set_largest(i->largest.Encode().data());
    grandparent->set_file_size(i->fd.GetFileSize());
  }
  request.set_score_(c->score());
  request.set_bottommost_level_(c->bottommost_level());
  request.set_is_full_compaction_(c->is_full_compaction());
  request.set_is_manual_compaction_(c->is_manual_compaction());
  request.set_smallest_user_key_(c->GetSmallestUserKey().data());
  request.set_largest_user_key_(c->GetLargestUserKey().data());
  request.set_compaction_reason_(static_cast<int>(c->compaction_reason()));

  request.set_job_id(job_id);
  request.set_shutting_down(shutting_down);
  request.set_preserve_deletes_seqnum(preserve_deletes_seqnum);
  request.set_earliest_write_conflict_snapshot(earliest_write_conflict_snapshot);
  request.set_thread_pri(thread_pri);
  if(manual_compaction_paused == nullptr){
    request.set_is_manual(0);
  } else {
    request.set_manual_compaction_paused(*manual_compaction_paused);
    request.set_is_manual(1);
  }
  request.set_db_id(db_id);
  request.set_db_session_id(db_session_id);
  request.set_full_history_ts_low(full_history_ts_low);

  request.set_result_file_count(result_file_count);
#if REMOTE_COMPACTION
  request.set_result_file_number(c->input_version()->version_set()->NewFileNumber(result_file_count));
#endif
  request.set_base_level(c->GetInputBaseLevel());
#if ROCKSDB_SPDK
  uint64_t* start_lpns = (uint64_t*)malloc(sizeof(uint64_t)*result_file_count);
  uint64_t page_num  = ioptions->spdk_fs->GetFreeBlocks(c->SpdkOutputFilePreallocationSize(), result_file_count, start_lpns);
  for(size_t i=0; i<result_file_count; i++)
    request.add_start_lpns(start_lpns[i]);
#endif

  CompactionReply reply;

  ClientContext context;

//  const auto* cfd = c->column_family_data();
//  ThreadStatusUtil::SetColumnFamily(cfd, cfd->ioptions()->env,
//                                    db_options->enable_thread_tracking);
  grpc::Status status = stub_->SayHello(&context, request, &reply);

#if ROCKSDB_SPDK
  if (reply.meta_size() > 0) {
    for(int64_t i=0; i<reply.result_file_count(); i++)
      ioptions->spdk_fs->ReturnFreeBlocks(start_lpns[i], start_lpns[i] + page_num -1);
  } else {
    for(size_t i=0; i<result_file_count; i++)
      ioptions->spdk_fs->ReturnFreeBlocks(start_lpns[i], start_lpns[i] + page_num -1);
#if REMOTE_RANGE
    if (!reply.by_token()) {
      rejected_++;
      if (rejected_ > 3) {
        reject_range_lock_.lock();
        if (rejected_ > 3) {
          if (range_factor_ < 32) {
            auto cfd = c->column_family_data();
            VersionStorageInfo* vstorage = cfd->current()->storage_info();
            range_factor_ *= 2;
            compaction_range_ = GetMidSlice(vstorage->GetSmallestUserKey().ToString(), vstorage->GetLargestUserKey().ToString());
          }
          rejected_ = 0;
        }
        reject_range_lock_.unlock();
      }
    }
#endif
  }

  free(start_lpns);
#endif
  std::vector<FileMetaData> results;
  if (status.ok()) {
    for(auto i : reply.meta()){
      FileMetaData result;
      //result.smallest.DecodeFrom(Slice(i.smallest().data().c_str(), i.smallest().size()));
      //result.largest.DecodeFrom(Slice(i.largest().data().c_str(), i.largest().size()));
      result.smallest.Set(Slice(i.smallest().data()), i.smallest().seq(), static_cast<ValueType>(i.smallest().v_type()));
      result.largest.Set(Slice(i.largest().data()), i.largest().seq(), static_cast<ValueType>(i.largest().v_type()));
      result.fd.file_size = i.file_size();
      result.fd.packed_number_and_path_id = i.packed_number_and_path_id();
      result.fd.largest_seqno = i.largest_seqno();
      result.fd.smallest_seqno = i.smallest_seqno();
      result.oldest_ancester_time = i.oldest_ancester_time();
      result.file_creation_time = i.file_creation_time();
      results.push_back(result);
      /*fprintf(stderr, "end!! %lu %s\n", result.fd.GetNumber(), result.largest.user_key().ToString().c_str());*/
      /*fprintf(stderr, "result: %lu\n", result.fd.GetNumber());

      for(size_t tmpi = 0; tmpi<result.smallest.Encode().size(); tmpi++) {
        char tmp_c = result.smallest.Encode().data()[tmpi];
        if((tmp_c <= 'z' && tmp_c >= 'a') || (tmp_c <= 'Z' && tmp_c >= 'A') || (tmp_c <= '9' && tmp_c >= '0'))
          fprintf(stderr, " %c", tmp_c);
        else
          fprintf(stderr, " |%d|", tmp_c);
      }
      fprintf(stderr, "\n\n");
      for(size_t tmpi = 0; tmpi<result.largest.Encode().size(); tmpi++) {
        char tmp_c = result.largest.Encode().data()[tmpi];
        if((tmp_c <= 'z' && tmp_c >= 'a') || (tmp_c <= 'Z' && tmp_c >= 'A') || (tmp_c <= '9' && tmp_c >= '0'))
          fprintf(stderr, " %c", tmp_c);
        else
          fprintf(stderr, " |%d|", tmp_c);
      }
      fprintf(stderr, "\n\n");*/



#if ROCKSDB_SPDK
      std::string fname = TableFileName(ioptions->cf_paths, result.fd.GetNumber(), result.fd.GetPathId());
      ioptions->spdk_fs->AddInMeta(fname, i.start_lpn(), i.start_lpn() + page_num -1, i.file_size());
#endif
    }
  } else {
    fprintf(stderr, "no!!!!!!!!!!! %s\n\n", status.error_message().c_str());
    std::cout << status.error_code() << ": " << status.error_message()
              << std::endl;
  }
  return results;
}

void GreeterClient::Preprocess(PreprocessRequest& request, PreprocessReply* reply) {
  ClientContext context;
  grpc::Status status = stub_->Preprocess(&context, request, reply);
  if(!status.ok())
    fprintf(stderr, "grpc status not ok %d %s\n", status.error_code(), status.error_message().c_str());
}


}
