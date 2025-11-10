#pragma once

#define DEBUG_PRINT 0
#define REMOTE_COMPACTION 1
#define ROCKSDB_SPDK 1
#define LEMMA_PATH 0
#define LEMMA_L0 1
#define SMART_CACHE 1
#define POS_IO 1
#define POS_CACHE 0
#define WAL_IN_REMOTE 1
//#define SPANDB_STAT 1
#if LEMMA_L0
#define LEMMA_L02L0 1
#endif
#define OPTION_FOR_COMPACTION 1

#define MULTI_NUM 1
#define MY_ID 5

#if REMOTE_COMPACTION
#define REMOTE_RANGE 1
#else
#define REMOTE_RANGE 0
#endif
