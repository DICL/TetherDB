#pragma once

#include "env/topfs_cache.h"
#include <unordered_map>
#include "lemma/util.h"

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
	};

	class LRUCache{
	private:
		uint64_t capacity;
		uint64_t size;
		ListNode *head;
		ListNode *tail;
		std::unordered_map<TKey, ListNode*> hash_map;

		typedef std::mutex LRUMutex;
		//typedef TopFSSpinLock LRUMutex;
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
				//fprintf(stderr, "no cache err!!! \n");
				return;
			}
			TKey key = node->key;
			DeleteNode(node);
			hash_map.erase(key);
			delete node;
		}

	public:
		uint64_t count_all = 0;
		uint64_t count_hit = 0;
		LRUCache(uint64_t max_size):
			capacity(max_size),
			size(0),
			head(nullptr),
			tail(nullptr)
			{}
		
		~LRUCache(){
			Clear();
		}
		uint64_t Size() {
			return size;
		}

		void Evict(){
			std::unique_lock<LRUMutex> lock(mutex_);
			InternalEvict();
/*			ListNode* node = tail;
			if (node == nullptr) {
				fprintf(stderr, "no cache err!!! \n");
				return;
			}
			TKey key = node->key;
			DeleteNode(node);
			hash_map.erase(key);
			delete node;*/
		}

		bool Find(const TKey& key, TValue& value){
			std::unique_lock<LRUMutex> lock(mutex_);
			count_all++;
			//auto start = SSDLOGGING_TIME_NOW;
			auto it = hash_map.find(key);
			if(it == hash_map.end())
				return false;
			if(!it->second->Valid())
				return false;
			ListNode* node = it->second;
			value =  node->value;
			DeleteNode(node);
			count_hit++;
#if SMART_CACHE
			hash_map.erase(key);
			delete node;
#else
			PushToFront(node);
#endif
			//find_lat.add(SSDLOGGING_TIME_DURATION(start, SSDLOGGING_TIME_NOW));
			return true;
		}
		bool Insert(const TKey& key, const TValue& value){
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
			ListNode* node = new ListNode(key, value);
			hash_map[key] = node;
			PushToFront(node);
			if(size >= capacity) {
				InternalEvict();
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
	};

private:
	const int SEG_NUM = 64;
	uint64_t capacity;
	LRUCache **cache_list = nullptr;
	FILE * fp;

	int GetLRUIndex(TKey key){
		return (key / 16) % SEG_NUM;
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

#ifdef SPANDB_STAT
	ssdlogging::statistics::AvgStat insert_lat;
	ssdlogging::statistics::AvgStat find_lat;
	ssdlogging::statistics::AvgStat delete_lat;
	ssdlogging::statistics::AvgStat evict_lat;
	ssdlogging::statistics::AvgStat test_lat;
#endif

public:
	SegLRUCache(uint64_t max_size):capacity(max_size){
		cache_list = new LRUCache* [SEG_NUM];
		for(int i=0; i<SEG_NUM; i++){
			cache_list[i] = new LRUCache(max_size / SEG_NUM);
		}
		fp = fopen("cache.txt", "w");
	}

	void PrintHit(bool flush){
		uint64_t all = 0;
		uint64_t hit = 0;
		uint64_t size_all = 0;
		for(int i=0; i<SEG_NUM; i++){
			all += cache_list[i]->count_all;
			hit += cache_list[i]->count_hit;
			size_all += cache_list[i]->Size();
		}
		fprintf(fp,"%s, all: %lu, hit: %lu, rate: %lf, size: %lu, max: %lu\n", GetDayTime().c_str(), all, hit, ((double)hit )/ ((double) all), size_all, capacity);
		if (flush)
		  fflush(fp);
	}

	bool Insert(const TKey& key, const TValue& value){
#ifdef SPANDB_STAT
		auto start = SSDLOGGING_TIME_NOW;
#endif
		int index = GetLRUIndex(key);
		bool res =  cache_list[index]->Insert(key, value);
#ifdef SPANDB_STAT
		insert_lat.add(SSDLOGGING_TIME_DURATION(start, SSDLOGGING_TIME_NOW));
#endif
		return res;
	}

	bool Find(const TKey& key, TValue& value){
#ifdef SPANDB_STAT
		auto start = SSDLOGGING_TIME_NOW;
#endif
		int index = GetLRUIndex(key);
		bool res = cache_list[index]->Find(key, value);
#ifdef SPANDB_STAT
		find_lat.add(SSDLOGGING_TIME_DURATION(start, SSDLOGGING_TIME_NOW));
#endif
		return res;
	}

	void Evict(){
#ifdef SPANDB_STAT
		auto start = SSDLOGGING_TIME_NOW;
#endif
		int index = ssdlogging::random::Random::GetTLSInstance()->Next() % SEG_NUM;
		cache_list[index]->Evict();
#ifdef SPANDB_STAT
		evict_lat.add(SSDLOGGING_TIME_DURATION(start, SSDLOGGING_TIME_NOW));
#endif
	}

	void Evict(int num){
		//fprintf(stderr, "\nEvict by buffer %d\n\n", num);
		for(int i = 0; i<num; i++){
			int index = ssdlogging::random::Random::GetTLSInstance()->Next() % SEG_NUM;
			cache_list[index]->Evict();
			/*if (!cache_list[index]->Evict()) {
				fprintf(stderr, "%d\n", index);
				for(int j = 0; j < SEG_NUM; j++)
					fprintf(stderr, "%d %lu\n", j, cache_list[index]->Size());
				fprintf(stderr, "\n");
			}*/
		}
	}

	void DeleteKey(const TKey& key){
#ifdef SPANDB_STAT
		auto start = SSDLOGGING_TIME_NOW;
#endif
		int index = GetLRUIndex(key);
		cache_list[index]->DeleteKey(key);
#ifdef SPANDB_STAT
		delete_lat.add(SSDLOGGING_TIME_DURATION(start, SSDLOGGING_TIME_NOW));
#endif
	}

	void ResetStat(){
#ifdef SPANDB_STAT
		insert_lat.reset();
		find_lat.reset();
		delete_lat.reset();
		evict_lat.reset();
		test_lat.reset();
#endif
	}

	virtual ~SegLRUCache(){
		printf("------------Seg LRU Cache---------------\n");
#ifdef SPANDB_STAT
		printf("delete_lat: %ld %.2lf us\n", delete_lat.size(), delete_lat.avg());
		printf("insert_lat: %ld %.2lf us\n", insert_lat.size(), insert_lat.avg());
		printf("find_lat: %ld %.2lf us\n", find_lat.size(), find_lat.avg());
		printf("evict_lat: %ld %.2lf us\n", evict_lat.size(), evict_lat.avg());
#endif
		for(int i=0; i<SEG_NUM; i++){
			delete cache_list[i];
		}
		delete cache_list;
		printf("---------------------------------------\n");
	};
};


}
