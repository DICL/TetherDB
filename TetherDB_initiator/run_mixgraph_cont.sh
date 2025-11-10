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
query_num=$((25 * M))

num_threads=32

get_ratio=0.83
put_ratio=0.17
seek_ratio=0.0

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
  -writes=$((num_kvs / num_threads)) \
  -num=$((num_kvs))"
# -compression_type=none \
io_mix_ratio_params="-mix_get_ratio=$get_ratio \
  -mix_put_ratio=$put_ratio  \
  -mix_seek_ratio=$seek_ratio"

all_random_params="-keyrange_num=1 \
  -value_k=0.2615 \
  -value_sigma=25.45 \
  -iter_k=2.517 \
  -iter_sigma=14.236"

all_dist_params="-key_dist_a=0.002312 \
  -key_dist_b=0.3467 \
  -keyrange_num=1 \
  -value_k=0.2615 \
  -value_sigma=25.45 \
  -iter_k=2.517 \
  -iter_sigma=14.236"

prefix_random_params="-keyrange_dist_a=14.18 \
  -keyrange_dist_b=-2.917 \
  -keyrange_dist_c=0.0164 \
  -keyrange_dist_d=-0.08082 \
  -keyrange_num=30 \
  -value_k=0.2615 \
  -value_sigma=25.45 \
  -iter_k=2.517 \
  -iter_sigma=14.236"

prefix_dist_params="-key_dist_a=0.002312 \
  -key_dist_b=0.3467 \
  -keyrange_dist_a=14.18 \
  -keyrange_dist_b=-2.917 \
  -keyrange_dist_c=0.0164 \
  -keyrange_dist_d=-0.08082 \
  -keyrange_num=30 \
  -value_k=0.2615 \
  -value_sigma=25.45 \
  -iter_k=2.517 \
  -iter_sigma=14.236"

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
  hostname=$(hostname)

  echo "[INFO] Run DB (mixgraph)"
  echo""

  if [ "$qps_intensity" == "light" ]; then
    sine_a=$((200 * K))
    #sine_b=0.015625
    sine_b=0.03
    sine_d=$((900 * K))
  elif [ "$qps_intensity" == "moderate" ]; then
    sine_a=$((6 * K))
    sine_b=0.03125
    sine_d=$((12 * K))
  elif [ "$qps_intensity" == "heavy" ]; then
    sine_a=$((200 * K))
    sine_b=0.03
    #sine_d=$((18 * K))
    sine_d=$((600 * K))

  else 
    echo "[ERR] qps_intensity=(light,moderate,heavy) $qps_intensity"
    exit 1
  fi

  qps_params="-sine_mix_rate_interval_milliseconds=5000 \
    -sine_a=${sine_a} \
    -sine_b=${sine_b} \
    -sine_d=${sine_d} \
    -sine_mix_rate=true \
    -reads=$query_num"
  #-sine_mix_rate_noise=0.5 \

  run_params="$const_params \
    $io_mix_ratio_params \
    $qps_params \
    -report_interval_seconds=1 \
    -report_file=mixgraph/${bench_type}_${qps_intensity}_${hostname}.csv"

  if [ "$bench_type" == "prefix_dist" ]; then
    run_params2=$prefix_dist_params
  elif [ "$bench_type" == "prefix_random" ]; then
    run_params2=$prefix_random_params
  elif [ "$bench_type" == "all_dist" ]; then
    run_params2=$all_dist_params
  elif [ "$bench_type" == "all_random" ]; then
    run_params2=$all_random_params   
  else
    echo "[ERR] bench_type=(prefix_dist, prefix_random, all_dist, all_random) $bench_type"
    exit 1
  fi
  
  echo $run_params 
  echo $run_params2 

  $DB_BENCH --benchmarks="fillrandom,mixgraph" $run_params $run_params2
}

bench_type=$1
qps_intensity=$2

if [ "$bench_type" != "load" ] && [ $# -ne 2 ]; then
  echo "$0 <load>"
  echo "$0 <prefix_dist/prefix_random> <light/moderate/heavy>"
  exit 1
fi


if [ "$bench_type" == "load" ]
then
  clear_db

  start=$(date +%s.%N)
  gen_db
  finish=$(date +%s.%N)
  diff=$(echo "$finish - $start" | bc -l )
  echo "gen elapsed: $diff"
else
  ~/drop_cache.sh 

  start=$(date +%s.%N)
  run_mixgraph
  finish=$(date +%s.%N)
  diff=$( echo "$finish - $start" | bc -l )
  echo "run elapsed: $diff"
fi

  #elif [ "$bench_type" == "prefix_dist" ] || 
    #  [ "$bench_type" == "prefix_random" ] || 
    #  [ "$bench_type" == "all_dist" ] || 
    #  [ "$bench_type" == "all_random" ]
  #then
  #echo "[ERR] Unknown bench_type=$bench_type"
