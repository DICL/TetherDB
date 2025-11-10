#pragma once

#define DEBUG_PRINT 0
#define LEMMA_PATH 0
#define ROCKSDB_SPDK 1
#define LEMMA_L0 1
//#define SPANDB_STAT 1
#define SMART_CACHE 1 //use cache or not hint

#if SMART_CACHE
#define PIN_L0 1 //mru & cache priority hint
#define LEVEL_CACHE 0
#endif

#if LEMMA_L0
#define LEMMA_L02L0 1
#endif
