#pragma once

#include "env/topfs_cache.h"
#include <unordered_map>
#include "lemma/util.h"
#include "grpc/pos_prefetch.h"

namespace rocksdb {

template <class TKey, class TValue>
class SegLRUCache: public TopFSCache<TKey, TValue> {
	struct ListNode {
	    ListNode() : prev(nullptr), next(nullptr), valid(true) {}

	    ListNode(const TKey& k, const TValue& v)
	        : key(k), value(v),
	          prev(nullptr), next(nullptr), valid(true) {}
	    
	    bool Valid(){return valid;}

	    TKey key;
	    TValue value;
	    ListNode* prev;
	    ListNode* next;
	    bool valid;
	    bool by_compaction = false;
	    bool call_by_compaction = false;
	    bool call_by_get = false;
	    bool is_l0 = false;
	    
	};

	class LRUCache{
	private:
		uint64_t capacity;
		uint64_t size;
		ListNode *head;
		ListNode *tail;
		std::unordered_map<TKey, ListNode*> hash_map;
#if POS_CACHE
		PrefetchClient* prefetcher_;//lemma
#endif

		//typedef std::mutex LRUMutex;
		typedef TopFSSpinLock LRUMutex;
		LRUMutex mutex_;

		//ssdlogging::statistics::AvgStat find_lat;

		void DeleteNode(ListNode *node){
			if(node->prev != NULL)
				node->prev->next = node->next;
			else
				head = node->next;
			if(node->next != NULL)
				node->next->prev = node->prev;
			else
				tail = node->prev;
			node->prev = node->next = NULL;
			size--;
			if (node->by_compaction) {
				if (node->call_by_compaction && node->call_by_get) {
					if (node->is_l0)
						count_c_b_0++;
					else
						count_c_b++;
				} else if (node->call_by_compaction) {
					if (node->is_l0)
						count_c_c_0++;
					else
						count_c_c++;
				} else if (node->call_by_get) {
					if (node->is_l0)
						count_c_g_0++;
					else
						count_c_g++;
				} else {
					if (node->is_l0)
						count_c_n_0++;
					else
						count_c_n++;
				}
			} else {
				if (node->call_by_compaction && node->call_by_get) {
					if (node->is_l0)
						count_g_b_0++;
					else
						count_g_b++;
				} else if (node->call_by_compaction) {
					if (node->is_l0)
						count_g_c_0++;
					else
						count_g_c++;
				} else if (node->call_by_get) {
					if (node->is_l0)
						count_g_g_0++;
					else
						count_g_g++;
				} else {
					if (node->is_l0)
						count_g_n_0++;
					else
						count_g_n++;
				}
			}
		}

		void PushToFront(ListNode *node){
			ListNode *tmp = head;
			node->next = tmp;
			node->prev = NULL;
			head = node;
			if(tmp != NULL)
				tmp->prev = node;
			else
				tail = head;
			size++;
		}

		void InternalEvict(){
			ListNode* node = tail;
			if (node == nullptr) {
				fprintf(stderr, "InternalEvict() no cache err!!! %lu\n", size);
				return;
			}
			TKey key = node->key;
			DeleteNode(node);
			hash_map.erase(key);
			delete node;
#if POS_CACHE
			if(std::is_same<uint64_t, TKey>::value)
	                        prefetcher_->PrefetchData(key);
#endif
		}

	public:
		uint64_t count_all_c = 0;
		uint64_t count_hit_c = 0;
		uint64_t count_all_q = 0;
		uint64_t count_hit_q = 0;
		uint64_t count_all_c_0 = 0;
		uint64_t count_hit_c_0 = 0;
		uint64_t count_all_q_0 = 0;
		uint64_t count_hit_q_0 = 0;

		uint64_t count_c_n = 0;
		uint64_t count_c_g = 0;
		uint64_t count_c_c = 0;
		uint64_t count_c_b = 0;
		uint64_t count_g_n = 0;
		uint64_t count_g_g = 0;
		uint64_t count_g_c = 0;
		uint64_t count_g_b = 0;
		uint64_t count_c_n_0 = 0;
		uint64_t count_c_g_0 = 0;
		uint64_t count_c_c_0 = 0;
		uint64_t count_c_b_0 = 0;
		uint64_t count_g_n_0 = 0;
		uint64_t count_g_g_0 = 0;
		uint64_t count_g_c_0 = 0;
		uint64_t count_g_b_0 = 0;
		LRUCache(uint64_t max_size):
			capacity(max_size),
			size(0),
			head(nullptr),
			tail(nullptr){}
		
#if POS_CACHE
		LRUCache(uint64_t max_size, PrefetchClient* prefetcher):
			capacity(max_size),
			size(0),
			head(nullptr),
			tail(nullptr),
                        prefetcher_(prefetcher){}
#endif
		~LRUCache(){
			Clear();
		}

		void Evict(){
			std::unique_lock<LRUMutex> lock(mutex_);
			InternalEvict();
			/*ListNode* node = tail;
			if (node == nullptr) {
				fprintf(stderr, "Evict() no cache err!!! %lu\n", size);
				return;
			}
			TKey key = node->key;
			DeleteNode(node);
			hash_map.erase(key);
			delete node;*/
		}

		bool Find(const TKey& key, TValue& value, bool for_compaction, int level){
			std::unique_lock<LRUMutex> lock(mutex_);
			if(for_compaction)
				count_all_c++;
				/*if (level == 0)
					count_all_c_0++;
				else
					count_all_c++;*/
			else {
				if (level == 0)
					count_all_q_0++;
				else
					count_all_q++;
                        }
			//auto start = SSDLOGGING_TIME_NOW;
			auto it = hash_map.find(key);
			if(it == hash_map.end())
				return false;
			if(!it->second->Valid())
				return false;
			ListNode* node = it->second;
			value =  node->value;
			if(for_compaction)
				count_hit_c++;
				/*if (level == 0)
					count_hit_c_0++;
				else
					count_hit_c++;*/
			else {
				if (level == 0)
					count_hit_q_0++;
				else
					count_hit_q++;
			}
			if (for_compaction) {
				node->call_by_compaction = true;
			} else {
				node->call_by_get = true;
			}
			DeleteNode(node);
			PushToFront(node);
			//find_lat.add(SSDLOGGING_TIME_DURATION(start, SSDLOGGING_TIME_NOW));
			return true;
		}

		bool Insert(const TKey& key, const TValue& value, bool is_l0, bool by_compaction){
			std::unique_lock<LRUMutex> lock(mutex_);
			auto it = hash_map.find(key);
			if(it != hash_map.end()){
				ListNode* node = it->second;
				if(node->Valid())
					return false;
				node->value = value;
				DeleteNode(node);
				PushToFront(node);
				return true;
			}
			ListNode* node = new(std::nothrow) ListNode(key, value);
			node->by_compaction = by_compaction;
			node->is_l0 = is_l0;
			if(node){
				hash_map[key] = node;
				PushToFront(node);
				if(size >= capacity)
					InternalEvict();
			} else {
				fprintf(stderr, "new error!!!!!!!!!\n");
			}
			return true;
		}

		void DeleteKey(const TKey& key){
			std::unique_lock<LRUMutex> lock(mutex_);
			auto it = hash_map.find(key);
			if(it == hash_map.end())
				return ;
			ListNode* node = it->second;
			DeleteNode(node);
			hash_map.erase(node->key);
			delete node;
		}

		void Clear(){
			//printf("find_lat: %ld %.2lf us\n", find_lat.size(), find_lat.avg());
			std::unique_lock<LRUMutex> lock(mutex_);
			while(tail != nullptr){
				InternalEvict();
			}
		}
		uint64_t Size() { return size; }
	};

private:
	const int SEG_NUM = 80;
	uint64_t capacity;
	LRUCache **cache_list = nullptr;
#if POS_CACHE
	PrefetchClient* prefetcher_;//lemma
#endif
	FILE * fp;

	int GetLRUIndex(TKey key){
		return key % SEG_NUM;
	}

std::string GetDayTime() {
  const int kBufferSize = 100;
  char buffer[kBufferSize];
  struct timeval now_tv;
  gettimeofday(&now_tv, nullptr);
  const time_t seconds = now_tv.tv_sec;
  struct tm t;
  localtime_r(&seconds, &t);
  snprintf(buffer, kBufferSize,
            "%04d/%02d/%02d-%02d:%02d:%02d.%06d",
            t.tm_year + 1900,
            t.tm_mon + 1,
            t.tm_mday,
            t.tm_hour,
            t.tm_min,
            t.tm_sec,
            static_cast<int>(now_tv.tv_usec));
  return std::string(buffer);
}


	ssdlogging::statistics::AvgStat insert_lat;
	ssdlogging::statistics::AvgStat find_lat;
	ssdlogging::statistics::AvgStat delete_lat;
	ssdlogging::statistics::AvgStat evict_lat;
	ssdlogging::statistics::AvgStat test_lat;

public:
	SegLRUCache(uint64_t max_size):capacity(max_size){
		printf("Own LRU Cache: %d\n", SEG_NUM);
		cache_list = new LRUCache* [SEG_NUM];
		for(int i=0; i<SEG_NUM; i++){
			cache_list[i] = new LRUCache(max_size / SEG_NUM);
		}
		fp = fopen("cache.txt", "w");
	}

#if POS_CACHE
	SegLRUCache(uint64_t max_size, uint32_t subsys_id, uint32_t ns_id):capacity(max_size){
		prefetcher_ = new PrefetchClient("10.0.0.90:50051", subsys_id, ns_id);
		printf("Own LRU Cache: %d\n", SEG_NUM);
		cache_list = new LRUCache* [SEG_NUM];
		for(int i=0; i<SEG_NUM; i++){
			cache_list[i] = new LRUCache(max_size / SEG_NUM, prefetcher_);
		}
	}
#endif
	void PrintHit(bool flush){
		uint64_t all_c = 0;
		uint64_t hit_c = 0;
		uint64_t all_q = 0;
		uint64_t hit_q = 0;
		//uint64_t all_c_0 = 0;
		//uint64_t hit_c_0 = 0;
		uint64_t all_q_0 = 0;
		uint64_t hit_q_0 = 0;
		uint64_t size_all = 0;
		for(int i=0; i<SEG_NUM; i++){
			all_c += cache_list[i]->count_all_c;
			hit_c += cache_list[i]->count_hit_c;
			all_q += cache_list[i]->count_all_q;
			hit_q += cache_list[i]->count_hit_q;
			//all_c_0 += cache_list[i]->count_all_c_0;
			//hit_c_0 += cache_list[i]->count_hit_c_0;
			all_q_0 += cache_list[i]->count_all_q_0;
			hit_q_0 += cache_list[i]->count_hit_q_0;
			size_all += cache_list[i]->Size();
		}
		//fprintf(fp,"%s, all_c: %lu, hit_c: %lu, rate_c: %lf, all_c_0: %lu, hit_c_0: %lu, rate_c_0: %lf, all_q: %lu, hit_q: %lu, rate_q: %lf, all_q_0: %lu, hit_q_0: %lu, rate_q_0: %lf, size: %lu, max: %lu\n", GetDayTime().c_str(), all_c, hit_c, ((double)hit_c)/ ((double) all_c), all_c_0, hit_c_0, ((double)hit_c_0)/ ((double) all_c_0), all_q, hit_q, ((double)hit_q)/ ((double) all_q), all_q_0, hit_q_0, ((double)hit_q_0)/ ((double) all_q_0), size_all, capacity);
		fprintf(fp,"%s, all_c: %lu, hit_c: %lu, rate_c: %lf, all_q: %lu, hit_q: %lu, rate_q: %lf, all_q_0: %lu, hit_q_0: %lu, rate_q_0: %lf, size: %lu, max: %lu\n", GetDayTime().c_str(), all_c, hit_c, ((double)hit_c)/ ((double) all_c), all_q, hit_q, ((double)hit_q)/ ((double) all_q), all_q_0, hit_q_0, ((double)hit_q_0)/ ((double) all_q_0), size_all, capacity);
		//fprintf(fp,"%s, all_c: %lu, hit_c: %lu, rate_c: %lf, all_q: %lu, hit_q: %lu, rate_q: %lf, size: %lu, max: %lu\n", GetDayTime().c_str(), all_c, hit_c, ((double)hit_c)/ ((double) all_c), all_q, hit_q, ((double)hit_q)/ ((double) all_q), size_all, capacity);
		if (flush)
		  fflush(fp);
	}

	bool Insert(const TKey& key, const TValue& value, bool is_l0, bool by_compaction){
		auto start = SSDLOGGING_TIME_NOW;
		int index = GetLRUIndex(key);
		bool res =  cache_list[index]->Insert(key, value, is_l0, by_compaction);
		insert_lat.add(SSDLOGGING_TIME_DURATION(start, SSDLOGGING_TIME_NOW));
		return res;
	}

	bool Find(const TKey& key, TValue& value, bool for_compaction, int level){
		auto start = SSDLOGGING_TIME_NOW;
		int index = GetLRUIndex(key);
		bool res = cache_list[index]->Find(key, value, for_compaction, level);
		find_lat.add(SSDLOGGING_TIME_DURATION(start, SSDLOGGING_TIME_NOW));
		return res;
	}

	void Evict(){
		auto start = SSDLOGGING_TIME_NOW;
		int index = ssdlogging::random::Random::GetTLSInstance()->Next() % SEG_NUM;
		cache_list[index]->Evict();
		evict_lat.add(SSDLOGGING_TIME_DURATION(start, SSDLOGGING_TIME_NOW));
	}

	void Evict(int num){
		for(int i = 0; i<num; i++){
			int index = ssdlogging::random::Random::GetTLSInstance()->Next() % SEG_NUM;
			cache_list[index]->Evict();
		}
	}

	void DeleteKey(const TKey& key){
		auto start = SSDLOGGING_TIME_NOW;
		int index = GetLRUIndex(key);
		cache_list[index]->DeleteKey(key);
		delete_lat.add(SSDLOGGING_TIME_DURATION(start, SSDLOGGING_TIME_NOW));
	}

	void ResetStat(){
		insert_lat.reset();
		find_lat.reset();
		delete_lat.reset();
		evict_lat.reset();
		test_lat.reset();
	}

	virtual ~SegLRUCache(){
		printf("------------Seg LRU Cache---------------\n");
		printf("delete_lat: %ld %.2lf us\n", delete_lat.size(), delete_lat.avg());
		printf("insert_lat: %ld %.2lf us\n", insert_lat.size(), insert_lat.avg());
		printf("find_lat: %ld %.2lf us\n", find_lat.size(), find_lat.avg());
		printf("evict_lat: %ld %.2lf us\n", evict_lat.size(), evict_lat.avg());
		uint64_t count_c_n = 0;
		uint64_t count_c_g = 0;
		uint64_t count_c_c = 0;
		uint64_t count_c_b = 0;
		uint64_t count_g_n = 0;
		uint64_t count_g_g = 0;
		uint64_t count_g_c = 0;
		uint64_t count_g_b = 0;
		uint64_t count_c_n_0 = 0;
		uint64_t count_c_g_0 = 0;
		uint64_t count_c_c_0 = 0;
		uint64_t count_c_b_0 = 0;
		uint64_t count_g_n_0 = 0;
		uint64_t count_g_g_0 = 0;
		uint64_t count_g_c_0 = 0;
		uint64_t count_g_b_0 = 0;
		for(int i=0; i<SEG_NUM; i++){
			while (cache_list[i]->Size())
				cache_list[i]->Evict();
			count_c_n += cache_list[i]->count_c_n;
			count_c_g += cache_list[i]->count_c_g;
			count_c_c += cache_list[i]->count_c_c;
			count_c_b += cache_list[i]->count_c_b;
			count_g_n += cache_list[i]->count_g_n;
			count_g_g += cache_list[i]->count_g_g;
			count_g_c += cache_list[i]->count_g_c;
			count_g_b += cache_list[i]->count_g_b;
			count_c_n_0 += cache_list[i]->count_c_n_0;
			count_c_g_0 += cache_list[i]->count_c_g_0;
			count_c_c_0 += cache_list[i]->count_c_c_0;
			count_c_b_0 += cache_list[i]->count_c_b_0;
			count_g_n_0 += cache_list[i]->count_g_n_0;
			count_g_g_0 += cache_list[i]->count_g_g_0;
			count_g_c_0 += cache_list[i]->count_g_c_0;
			count_g_b_0 += cache_list[i]->count_g_b_0;
			delete cache_list[i];
		}
                fprintf(stderr, "c_n: %lu c_g: %lu c_c: %lu c_b: %lu\n", count_c_n, count_c_g, count_c_c, count_c_b);
                fprintf(stderr, "c_n_0: %lu c_g_0: %lu c_c_0: %lu c_b_0: %lu\n", count_c_n_0, count_c_g_0, count_c_c_0, count_c_b_0);
                fprintf(stderr, "g_n: %lu g_g: %lu g_c: %lu g_b: %lu\n", count_g_n, count_g_g, count_g_c, count_g_b);
                fprintf(stderr, "g_n_0: %lu g_g_0: %lu g_c_0: %lu g_b_0: %lu\n", count_g_n_0, count_g_g_0, count_g_c_0, count_g_b_0);
		delete cache_list;
		printf("---------------------------------------\n");
	};
};


}
