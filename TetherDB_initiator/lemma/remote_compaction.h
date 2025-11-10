//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include "db/version_set.h"
#include "memory/arena.h"
#include "options/cf_options.h"
#include "rocksdb/sst_partitioner.h"
#include "util/autovector.h"
#include "db/compaction/compaction.h"

namespace ROCKSDB_NAMESPACE {
// The file contains class Compaction, as well as some helper functions
// and data structures used by the class.

/*// An AtomicCompactionUnitBoundary represents a range of keys [smallest,
// largest] that exactly spans one ore more neighbouring SSTs on the same
// level. Every pair of  SSTs in this range "overlap" (i.e., the largest
// user key of one file is the smallest user key of the next file). These
// boundaries are propagated down to RangeDelAggregator during compaction
// to provide safe truncation boundaries for range tombstones.
struct AtomicCompactionUnitBoundary {
  const InternalKey* smallest = nullptr;
  const InternalKey* largest = nullptr;
};

// The structure that manages compaction input files associated
// with the same physical level.
struct CompactionInputFiles {
  int level;
  std::vector<FileMetaData*> files;
  std::vector<AtomicCompactionUnitBoundary> atomic_compaction_unit_boundaries;
  inline bool empty() const { return files.empty(); }
  inline size_t size() const { return files.size(); }
  inline void clear() { files.clear(); }
  inline FileMetaData* operator[](size_t i) const { return files[i]; }
};*///lemmalemmalemma

class Version;
class ColumnFamilyData;
class VersionStorageInfo;
class CompactionFilter;
struct AtomicCompactionUnitBoundary;
struct CompactionInputFiles;

// A Compaction encapsulates metadata about a compaction.
class RemoteCompaction {
 public:
  RemoteCompaction(Compaction* c);

  // No copying allowed
  RemoteCompaction(const RemoteCompaction&) = delete;
  void operator=(const RemoteCompaction&) = delete;

  ~RemoteCompaction();

  // Returns the level associated to the specified compaction input level.
  // If compaction_input_level is not specified, then input_level is set to 0.
  int level(size_t compaction_input_level = 0) const {
    return inputs_[compaction_input_level].level;
  }

  int start_level() const { return start_level_; }

  // Outputs will go to this level
  int output_level() const { return output_level_; }

  // Returns the number of input levels in this compaction.
  size_t num_input_levels() const { return inputs_.size(); }

  // Return the object that holds the edits to the descriptor done
  // by this compaction.
  VersionEdit* edit() { return edit_; }

  // Returns the number of input files associated to the specified
  // compaction input level.
  // The function will return 0 if when "compaction_input_level" < 0
  // or "compaction_input_level" >= "num_input_levels()".
  size_t num_input_files(size_t compaction_input_level) const {
    if (compaction_input_level < inputs_.size()) {
      return inputs_[compaction_input_level].size();
    }
    return 0;
  }

  // Returns input version of the compaction
  Version* input_version() const { return input_version_; }

  // Returns the ColumnFamilyData associated with the compaction.
  ColumnFamilyData* column_family_data() const { return cfd_; }

  // Returns the file meta data of the 'i'th input file at the
  // specified compaction input level.
  // REQUIREMENT: "compaction_input_level" must be >= 0 and
  //              < "input_levels()"
  FileMetaData* input(size_t compaction_input_level, size_t i) const {
    assert(compaction_input_level < inputs_.size());
    return inputs_[compaction_input_level][i];
  }

  const std::vector<AtomicCompactionUnitBoundary>* boundaries(
      size_t compaction_input_level) const {
    assert(compaction_input_level < inputs_.size());
    return &inputs_[compaction_input_level].atomic_compaction_unit_boundaries;
  }

  // Returns the list of file meta data of the specified compaction
  // input level.
  // REQUIREMENT: "compaction_input_level" must be >= 0 and
  //              < "input_levels()"
  const std::vector<FileMetaData*>* inputs(
      size_t compaction_input_level) const {
    assert(compaction_input_level < inputs_.size());
    return &inputs_[compaction_input_level].files;
  }

  const std::vector<CompactionInputFiles>* inputs() { return &inputs_; }

  // Returns the LevelFilesBrief of the specified compaction input level.
  const LevelFilesBrief* input_levels(size_t compaction_input_level) const {
    return &input_levels_[compaction_input_level];
  }

  // Maximum size of files to build during this compaction.
  uint64_t max_output_file_size() const { return max_output_file_size_; }

  // What compression for output
  CompressionType output_compression() const { return output_compression_; }

  // What compression options for output
  CompressionOptions output_compression_opts() const {
    return output_compression_opts_;
  }

  // Whether need to write output file to second DB path.
  uint32_t output_path_id() const { return output_path_id_; }

  // If true, then the compaction can be done by simply deleting input files.
  bool deletion_compaction() const { return deletion_compaction_; }

  // Add all inputs to this compaction as delete operations to *edit.
  void AddInputDeletions(VersionEdit* edit);

  // Returns the summary of the compaction in "output" with maximum "len"
  // in bytes.  The caller is responsible for the memory management of
  // "output".
  void Summary(char* output, int len);

  // Return the score that was used to pick this compaction run.
  double score() const { return score_; }

  // Is this compaction creating a file in the bottom most level?
  bool bottommost_level() const { return bottommost_level_; }

  // Does this compaction include all sst files?
  bool is_full_compaction() const { return is_full_compaction_; }

  // Was this compaction triggered manually by the client?
  bool is_manual_compaction() const { return is_manual_compaction_; }

  // How many total levels are there?
  int number_levels() const { return number_levels_; }

  // Return the ImmutableCFOptions that should be used throughout the compaction
  // procedure
  const ImmutableCFOptions* immutable_cf_options() const {
    return immutable_cf_options_;
  }

  // Return the MutableCFOptions that should be used throughout the compaction
  // procedure
  const MutableCFOptions* mutable_cf_options() const {
    return mutable_cf_options_;
  }

  // Returns the size in bytes that the output file should be preallocated to.
  // In level compaction, that is max_file_size_. In universal compaction, that
  // is the sum of all input file sizes.
  uint64_t OutputFilePreallocationSize() const;

  struct InputLevelSummaryBuffer {
    char buffer[128];
  };

  const char* InputLevelSummary(InputLevelSummaryBuffer* scratch) const;

  uint64_t CalculateTotalInputSize() const;

  // Create a SstPartitioner from sst_partitioner_factory
  std::unique_ptr<SstPartitioner> CreateSstPartitioner() const;

  // Is the input level corresponding to output_level_ empty?
  bool IsOutputLevelEmpty() const;

  // Should this compaction be broken up into smaller ones run in parallel?
  bool ShouldFormSubcompactions() const;

  Slice GetSmallestUserKey() const { return smallest_user_key_; }

  Slice GetLargestUserKey() const { return largest_user_key_; }

  CompactionReason compaction_reason() { return compaction_reason_; }

  const std::vector<FileMetaData*>& grandparents() const {
    return grandparents_;
  }

  uint64_t max_compaction_bytes() const { return max_compaction_bytes_; }

  uint32_t max_subcompactions() const { return max_subcompactions_; }

  uint64_t MinInputFileOldestAncesterTime() const { return min_input_file_oldest_ancester_time_; }

  bool KeyNotExistsBeyondOutputLevel(const Slice& user_key, std::vector<size_t>* level_ptrs) const;

 private:
  Compaction* compaction_;
  uint64_t min_input_file_oldest_ancester_time_;
  std::vector<std::vector<FileMetaData*>> input_vstorage_LevelFiles_;
  const int start_level_;    // the lowest level to be compacted
  const int output_level_;  // levels to which output files are stored
  uint64_t max_output_file_size_;
  uint64_t max_compaction_bytes_;
  uint32_t max_subcompactions_;
  const ImmutableCFOptions* immutable_cf_options_;
  const MutableCFOptions* mutable_cf_options_;
  Version* input_version_;
  VersionEdit* edit_;
  const int number_levels_;
  ColumnFamilyData* cfd_;

  const uint32_t output_path_id_;
  CompressionType output_compression_;
  CompressionOptions output_compression_opts_;
  // If true, then the comaction can be done by simply deleting input files.
  const bool deletion_compaction_;

  // Compaction input files organized by level. Constant after construction
  const std::vector<CompactionInputFiles> inputs_;

  // A copy of inputs_, organized more closely in memory
  autovector<LevelFilesBrief, 2> input_levels_;

  // State used to check for number of overlapping grandparent files
  // (grandparent == "output_level_ + 1")
  std::vector<FileMetaData*> grandparents_;
  const double score_;         // score that was used to pick this compaction.

  // Is this compaction creating a file in the bottom most level?
  const bool bottommost_level_;
  // Does this compaction include all sst files?
  const bool is_full_compaction_;

  // Is this compaction requested by the client?
  const bool is_manual_compaction_;

  // smallest user keys in compaction
  Slice smallest_user_key_;

  // largest user keys in compaction
  Slice largest_user_key_;

  // Reason for compaction
  CompactionReason compaction_reason_;
};

}  // namespace ROCKSDB_NAMESPACE
