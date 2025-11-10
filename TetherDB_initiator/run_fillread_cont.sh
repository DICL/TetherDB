#!/bin/bash 

#ROCKSDB_DIR="/scratch/rocksdb-7.0.2"
#ROCKSDB_DIR="/scratch/rocksdb-6.17.3"
ROCKSDB_DIR="/lemma2/rocksdb_local"
#DB_DIR="/scratch-ssd/mixgraph"
#DB_DIR="/mnt/nvme/mixgraph"
DB_DIR="/lemma/rocksdb"
DB_BENCH="$ROCKSDB_DIR/db_bench"

K=$((1000))
M=$((1000*1000))

#duration=3600 # sec

key_size=48
value_size=1000
num_kvs=$((200 * M))
query_num=$((40 * M))
#num_kvs=$((2000000))
#query_num=$((100000))

num_threads=32

const_params="-db=$DB_DIR \
  -threads=$num_threads \
  -cache_size=$((8*1024*1024)) \
  -disable_wal=false \
  -use_direct_io_for_flush_and_compaction=false \
  -use_direct_reads=false \
  -key_size=$key_size \
  --value_size=$value_size \
  -max_background_jobs=20 \
  -subcompactions=5 \
  -compression_type=none \
  -soft_pending_compaction_bytes_limit=549755813888 \
  -hard_pending_compaction_bytes_limit=1099511627776 \
  -max_write_buffer_number=6 \
  -delayed_write_rate=16777216 \
  -new_table_reader_for_compaction_inputs=false \
  -index_shortening_mode=1 \
  -stats_interval_seconds=1 \
  --statistics \
  -report_interval_seconds=1 \
  -report_file=report.csv \
  -readwritepercent=25 \
  -pin_top_level_index_and_filter=true \
  -writes=$((num_kvs / num_threads)) \
  -reads=$((query_num / num_threads)) \
  -num=$((num_kvs))"

  #-level0_slowdown_writes_trigger=50 \
  #-level0_stop_writes_trigger=60 \

clear_db() {
  if [ -d $DB_DIR ]
  then
    echo "clear $DB_DIR"
    rm -rf $DB_DIR
  fi

  echo "mkdir $DB_DIR"
  mkdir $DB_DIR
}

#fill random
gen_db() {
  echo "[INFO] Generate DB (fillrandom)" 
  echo""

  $DB_BENCH --benchmarks="fillrandom" $const_params
}

run_mixgraph() {
  #run_params="$const_params \
  #  -use_existing_db=true"
  $DB_BENCH --benchmarks="fillrandom,readrandomwriterandom,readrandomwriterandom" $const_params
}

bench_type=$1

if [ "$bench_type" == "load" ]
then
  clear_db

  start=$(date +%s.%N)
  gen_db
  finish=$(date +%s.%N)
  diff=$(echo "$finish - $start" | bc -l )
  echo "gen elapsed: $diff"
else
  #~/drop_cache.sh 

  start=$(date +%s.%N)
  run_mixgraph
  finish=$(date +%s.%N)
  diff=$( echo "$finish - $start" | bc -l )
  echo "run elapsed: $diff"
fi

