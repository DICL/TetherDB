//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "lemma/remote_compaction.h"

#include <cinttypes>
#include <vector>

#include "db/column_family.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/sst_partitioner.h"
#include "test_util/sync_point.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

//const uint64_t kRangeTombstoneSentinel =
//    PackSequenceAndType(kMaxSequenceNumber, kTypeRangeDeletion);

CompressionOptions newCompressionOptions(CompressionOptions c){
  CompressionOptions k(c.window_bits, c.level, c.strategy, c.max_dict_bytes, c.zstd_max_train_bytes, c.parallel_threads, c.enabled);
  return k;
}

Slice newSlice(Slice s){
  Slice ss(s.data(), s.size());
  return ss;
}

std::vector<CompactionInputFiles> NewInput(std::vector<CompactionInputFiles> in){
  std::vector<CompactionInputFiles> return_input;
  for (auto i : in){
    CompactionInputFiles compaction_input_files;
    compaction_input_files.level = i.level;
    for (auto j : i.files){
      FileMetaData* file_meta_data = new FileMetaData;
      file_meta_data->num_entries = j->num_entries;
      file_meta_data->fd.file_size = j->fd.file_size;
      file_meta_data->fd.packed_number_and_path_id = j->fd.packed_number_and_path_id;
      file_meta_data->fd.largest_seqno = j->fd.largest_seqno;
      compaction_input_files.files.push_back(file_meta_data);
    }
    for (auto j : i.atomic_compaction_unit_boundaries){
      InternalKey* smallest = new InternalKey();
      InternalKey* largest = new InternalKey();
      smallest->DecodeFrom(j.smallest->Encode());
      largest->DecodeFrom(j.largest->Encode());
      AtomicCompactionUnitBoundary atomic_compaction_unit_boundary(smallest, largest);
      compaction_input_files.atomic_compaction_unit_boundaries.push_back(atomic_compaction_unit_boundary);
    }
    return_input.push_back(compaction_input_files);
  }
  return return_input;
}

RemoteCompaction::RemoteCompaction(Compaction* c)
  : start_level_(c->start_level()),
    output_level_(c->output_level()),
    max_output_file_size_(c->max_output_file_size()),
    max_compaction_bytes_(c->max_compaction_bytes()),
    max_subcompactions_(c->max_subcompactions()),
    immutable_cf_options_(c->immutable_cf_options()),
    mutable_cf_options_(c->mutable_cf_options()),
    input_version_(c->input_version()),
    edit_(c->edit()),
    number_levels_(c->number_levels()),
    cfd_(c->column_family_data()),
    output_path_id_(c->output_path_id()),
    output_compression_(c->output_compression()),
    output_compression_opts_(newCompressionOptions(c->output_compression_opts())),
    deletion_compaction_(c->deletion_compaction()),
    inputs_(NewInput(*c->inputs())),
    //grandparents_(c->grandparents()),
    score_(c->score()),
    bottommost_level_(c->bottommost_level()),
    is_full_compaction_(c->is_full_compaction()),
    is_manual_compaction_(c->is_manual_compaction()),
    smallest_user_key_(newSlice(c->GetSmallestUserKey())),
    largest_user_key_(newSlice(c->GetLargestUserKey())),
    compaction_reason_(c->compaction_reason()) {

  fprintf(stderr, "options: %d, %d, %lu, %d, %d, %d, %s, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %s, %d\n", immutable_cf_options_->min_write_buffer_number_to_merge, immutable_cf_options_->max_write_buffer_number_to_maintain, immutable_cf_options_->max_write_buffer_size_to_maintain, immutable_cf_options_->inplace_update_support, immutable_cf_options_->allow_mmap_reads, immutable_cf_options_->allow_mmap_writes, immutable_cf_options_->db_paths[0].path.c_str(), immutable_cf_options_->advise_random_on_open, immutable_cf_options_->bloom_locality, immutable_cf_options_->purge_redundant_kvs_while_flush, immutable_cf_options_->use_fsync, immutable_cf_options_->level_compaction_dynamic_level_bytes, immutable_cf_options_->new_table_reader_for_compaction_inputs, immutable_cf_options_->num_levels, immutable_cf_options_->optimize_filters_for_hits, immutable_cf_options_->force_consistency_checks, immutable_cf_options_->allow_ingest_behind, immutable_cf_options_->preserve_deletes, immutable_cf_options_->cf_paths[0].path.c_str(), immutable_cf_options_->allow_data_in_errors);//lemma

  fprintf(stderr, "options: %d, %d, %lu, %d, %d, %d, %s, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %s, %d\n", cfd_->ioptions()->min_write_buffer_number_to_merge, cfd_->ioptions()->max_write_buffer_number_to_maintain, cfd_->ioptions()->max_write_buffer_size_to_maintain, cfd_->ioptions()->inplace_update_support, cfd_->ioptions()->allow_mmap_reads, cfd_->ioptions()->allow_mmap_writes, cfd_->ioptions()->db_paths[0].path.c_str(), cfd_->ioptions()->advise_random_on_open, cfd_->ioptions()->bloom_locality, cfd_->ioptions()->purge_redundant_kvs_while_flush, cfd_->ioptions()->use_fsync, cfd_->ioptions()->level_compaction_dynamic_level_bytes, cfd_->ioptions()->new_table_reader_for_compaction_inputs, cfd_->ioptions()->num_levels, cfd_->ioptions()->optimize_filters_for_hits, cfd_->ioptions()->force_consistency_checks, cfd_->ioptions()->allow_ingest_behind, cfd_->ioptions()->preserve_deletes, cfd_->ioptions()->cf_paths[0].path.c_str(), cfd_->ioptions()->allow_data_in_errors);//lemma

  fprintf(stderr, "%lu %d %lu %lf %d %lu %lu %lu %d %lu %lu %d %d %d %lu %lu %d %lu %lf %lu %lu %d %lu %lu %d %lf %lu %d %d %d %lu \n", mutable_cf_options_->write_buffer_size, mutable_cf_options_->max_write_buffer_number, mutable_cf_options_->arena_block_size, mutable_cf_options_->memtable_prefix_bloom_size_ratio, mutable_cf_options_->memtable_whole_key_filtering, mutable_cf_options_->memtable_huge_page_size, mutable_cf_options_->max_successive_merges, mutable_cf_options_->inplace_update_num_locks, mutable_cf_options_->disable_auto_compactions, mutable_cf_options_->soft_pending_compaction_bytes_limit, mutable_cf_options_->hard_pending_compaction_bytes_limit, mutable_cf_options_->level0_file_num_compaction_trigger, mutable_cf_options_->level0_slowdown_writes_trigger, mutable_cf_options_->level0_stop_writes_trigger, mutable_cf_options_->max_compaction_bytes, mutable_cf_options_->target_file_size_base, mutable_cf_options_->target_file_size_multiplier, mutable_cf_options_->max_bytes_for_level_base, mutable_cf_options_->max_bytes_for_level_multiplier, mutable_cf_options_->ttl, mutable_cf_options_->periodic_compaction_seconds, mutable_cf_options_->enable_blob_files, mutable_cf_options_->min_blob_size, mutable_cf_options_->blob_file_size, mutable_cf_options_->enable_blob_garbage_collection, mutable_cf_options_->blob_garbage_collection_age_cutoff, mutable_cf_options_->max_sequential_skip_in_iterations, mutable_cf_options_->check_flush_compaction_key_order, mutable_cf_options_->paranoid_file_checks, mutable_cf_options_->report_bg_io_stats, mutable_cf_options_->sample_for_compression);//lemma

  min_input_file_oldest_ancester_time_ = c->MinInputFileOldestAncesterTime();

  //make input_levels_
  for (auto i : inputs_){
    size_t num_files = i.files.size();
    LevelFilesBrief level_files_brief;
    level_files_brief.num_files = num_files;
    level_files_brief.files = new FdWithKeyRange[num_files];
    for(size_t j=0; j<num_files; j++){
      level_files_brief.files[j].smallest_key = i.files[j]->smallest.Encode();
      level_files_brief.files[j].largest_key = i.files[j]->largest.Encode();
    }
    input_levels_.push_back(level_files_brief);
  }

  //make grandparents_
  for (auto i : c->grandparents()){
    FileMetaData* file_meta_data = new FileMetaData;
    file_meta_data->smallest.DecodeFrom(i->smallest.Encode());
    file_meta_data->largest.DecodeFrom(i->largest.Encode());
    file_meta_data->fd.file_size = i->fd.GetFileSize();
    grandparents_.push_back(file_meta_data);
  }

  //make input_vstorage_LevelFiles_
  for(int lvl = output_level_ + 1; lvl < number_levels_; lvl++){
    std::vector<FileMetaData*> level_files;
    for (auto f : c->LevelFiles(lvl)){
      FileMetaData* level_file = new FileMetaData;
      level_file->largest.DecodeFrom(f->largest.Encode());
      level_file->smallest.DecodeFrom(f->smallest.Encode());
      level_files.push_back(level_file);
    }
    input_vstorage_LevelFiles_.push_back(level_files);
  }
}

RemoteCompaction::~RemoteCompaction() {
/*  if (input_version_ != nullptr) {
    input_version_->Unref();
  }
  if (cfd_ != nullptr) {
    cfd_->UnrefAndTryDelete();
  }*/
}

void RemoteCompaction::AddInputDeletions(VersionEdit* out_edit) {
  for (size_t which = 0; which < num_input_levels(); which++) {
    for (size_t i = 0; i < inputs_[which].size(); i++) {
      out_edit->DeleteFile(level(which), inputs_[which][i]->fd.GetNumber());
    }
  }
}

bool RemoteCompaction::KeyNotExistsBeyondOutputLevel(
    const Slice& user_key, std::vector<size_t>* level_ptrs) const {
  assert(input_version_ != nullptr);
  assert(level_ptrs != nullptr);
  assert(level_ptrs->size() == static_cast<size_t>(number_levels_));
  if (bottommost_level_) {
    return true;
  } else if (output_level_ != 0 &&
             cfd_->ioptions()->compaction_style == kCompactionStyleLevel) {
    // Maybe use binary search to find right entry instead of linear search?
    const Comparator* user_cmp = cfd_->user_comparator();
    for (int lvl = output_level_ + 1; lvl < number_levels_; lvl++) {
      const std::vector<FileMetaData*>& files =
          input_vstorage_LevelFiles_[lvl - output_level_ - 1];
      for (; level_ptrs->at(lvl) < files.size(); level_ptrs->at(lvl)++) {
        auto* f = files[level_ptrs->at(lvl)];
        if (user_cmp->Compare(user_key, f->largest.user_key()) <= 0) {
          // We've advanced far enough
          // In the presence of user-defined timestamp, we may need to handle
          // the case in which f->smallest.user_key() (including ts) has the
          // same user key, but the ts part is smaller. If so,
          // Compare(user_key, f->smallest.user_key()) returns -1.
          // That's why we need CompareWithoutTimestamp().
          if (user_cmp->CompareWithoutTimestamp(user_key,
                                                f->smallest.user_key()) >= 0) {
            // Key falls in this file's range, so it may
            // exist beyond output level
            return false;
          }
          break;
        }
      }
    }
    return true;
  }
  return false;
}

// Sample output:
// If compacting 3 L0 files, 2 L3 files and 1 L4 file, and outputting to L5,
// print: "3@0 + 2@3 + 1@4 files to L5"
const char* RemoteCompaction::InputLevelSummary(
    RemoteCompaction::InputLevelSummaryBuffer* scratch) const {
  int len = 0;
  bool is_first = true;
  for (auto& input_level : inputs_) {
    if (input_level.empty()) {
      continue;
    }
    if (!is_first) {
      len +=
          snprintf(scratch->buffer + len, sizeof(scratch->buffer) - len, " + ");
      len = std::min(len, static_cast<int>(sizeof(scratch->buffer)));
    } else {
      is_first = false;
    }
    len += snprintf(scratch->buffer + len, sizeof(scratch->buffer) - len,
                    "%" ROCKSDB_PRIszt "@%d", input_level.size(),
                    input_level.level);
    len = std::min(len, static_cast<int>(sizeof(scratch->buffer)));
  }
  snprintf(scratch->buffer + len, sizeof(scratch->buffer) - len,
           " files to L%d", output_level());

  return scratch->buffer;
}

uint64_t RemoteCompaction::CalculateTotalInputSize() const {
  uint64_t size = 0;
  for (auto& input_level : inputs_) {
    for (auto f : input_level.files) {
      size += f->fd.GetFileSize();
    }
  }
  return size;
}

namespace {
int InputSummary(const std::vector<FileMetaData*>& files, char* output,
                 int len) {
  *output = '\0';
  int write = 0;
  for (size_t i = 0; i < files.size(); i++) {
    int sz = len - write;
    int ret;
    char sztxt[16];
    AppendHumanBytes(files.at(i)->fd.GetFileSize(), sztxt, 16);
    ret = snprintf(output + write, sz, "%" PRIu64 "(%s) ",
                   files.at(i)->fd.GetNumber(), sztxt);
    if (ret < 0 || ret >= sz) break;
    write += ret;
  }
  // if files.size() is non-zero, overwrite the last space
  return write - !!files.size();
}
}  // namespace

void RemoteCompaction::Summary(char* output, int len) {
  int write =
      snprintf(output, len, "Base version %" PRIu64 " Base level %d, inputs: [",
               input_version_->GetVersionNumber(), start_level_);
  if (write < 0 || write >= len) {
    return;
  }

  for (size_t level_iter = 0; level_iter < num_input_levels(); ++level_iter) {
    if (level_iter > 0) {
      write += snprintf(output + write, len - write, "], [");
      if (write < 0 || write >= len) {
        return;
      }
    }
    write +=
        InputSummary(inputs_[level_iter].files, output + write, len - write);
    if (write < 0 || write >= len) {
      return;
    }
  }

  snprintf(output + write, len - write, "]");
}

uint64_t RemoteCompaction::OutputFilePreallocationSize() const {
  uint64_t preallocation_size = 0;

  for (const auto& level_files : inputs_) {
    for (const auto& file : level_files.files) {
      preallocation_size += file->fd.GetFileSize();
    }
  }

  if (max_output_file_size_ != port::kMaxUint64 &&
      (immutable_cf_options_->compaction_style == kCompactionStyleLevel ||
       output_level() > 0)) {
    preallocation_size = std::min(max_output_file_size_, preallocation_size);
  }

  // Over-estimate slightly so we don't end up just barely crossing
  // the threshold
  // No point to prellocate more than 1GB.
  return std::min(uint64_t{1073741824},
                  preallocation_size + (preallocation_size / 10));
}

std::unique_ptr<SstPartitioner> RemoteCompaction::CreateSstPartitioner() const {
  if (!immutable_cf_options_->sst_partitioner_factory) {
    return nullptr;
  }

  SstPartitioner::Context context;
  context.is_full_compaction = is_full_compaction_;
  context.is_manual_compaction = is_manual_compaction_;
  context.output_level = output_level_;
  context.smallest_user_key = smallest_user_key_;
  context.largest_user_key = largest_user_key_;
  return immutable_cf_options_->sst_partitioner_factory->CreatePartitioner(
      context);
}

bool RemoteCompaction::IsOutputLevelEmpty() const {
  return inputs_.back().level != output_level_ || inputs_.back().empty();
}

bool RemoteCompaction::ShouldFormSubcompactions() const {
  if (max_subcompactions_ <= 1 || cfd_ == nullptr) {
    return false;
  }
  if (cfd_->ioptions()->compaction_style == kCompactionStyleLevel) {
    return (start_level_ == 0 || is_manual_compaction_) && output_level_ > 0 &&
           !IsOutputLevelEmpty();
  } else if (cfd_->ioptions()->compaction_style == kCompactionStyleUniversal) {
    return number_levels_ > 1 && output_level_ > 0;
  } else {
    return false;
  }
}

/*uint64_t RemoteCompaction::MinInputFileOldestAncesterTime() const {
  uint64_t min_oldest_ancester_time = port::kMaxUint64;
  for (const auto& level_files : inputs_) {
    for (const auto& file : level_files.files) {
      uint64_t oldest_ancester_time = file->TryGetOldestAncesterTime();
      if (oldest_ancester_time != 0) {
        min_oldest_ancester_time =
            std::min(min_oldest_ancester_time, oldest_ancester_time);
      }
    }
  }
  return min_oldest_ancester_time;
}*/

}  // namespace ROCKSDB_NAMESPACE
