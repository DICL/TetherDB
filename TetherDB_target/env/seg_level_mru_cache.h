#pragma once

#include "env/topfs_cache.h"
#include <unordered_map>
#include "lemma/util.h"
#define MAX_LEVEL 5

namespace rocksdb {

template <class TKey, class TValue>
class SegMRUCache: public TopFSCache<TKey, TValue> {
	struct ListNode {
	    ListNode() : prev(nullptr), next(nullptr), valid(true) {}

	    ListNode(const TKey& k, const TValue& v, int l)
	        : key(k), value(v), level(l),
	          prev(nullptr), next(nullptr), valid(true) {}
	    
	    bool Valid(){return valid;}

	    TKey key;
	    TValue value;
	    int level;
	    ListNode* prev;
	    ListNode* next;
	    bool valid;
	};

	class MRUCache{
	private:
		uint64_t capacity;
		uint64_t size[MAX_LEVEL];
		ListNode *tail[MAX_LEVEL];
		uint64_t total_size;
		std::unordered_map<TKey, ListNode*> hash_map;

		typedef std::mutex MRUMutex;
		//typedef TopFSSpinLock MRUMutex;
		MRUMutex mutex_;

		//ssdlogging::statistics::AvgStat find_lat;

		void DeleteNode(ListNode *node){
			int level = node->level;
			if(node->prev != NULL)
				node->prev->next = node->next;
			if(node->next != NULL)
				node->next->prev = node->prev;
			else
				tail[level] = node->prev;
			node->prev = node->next = NULL;

			size[level]--;
			total_size--;
		}

		void PushToBack(ListNode *node){
			int level = node->level;
			ListNode *tmp = tail[level];
			node->next = NULL;
			node->prev = tmp;
			tail[level] = node;
			if(tmp != NULL)
				tmp->next = node;
			size[level]++;
			total_size++;
		}

		void InternalEvict(){
			for(int i = MAX_LEVEL - 1 ; i >= 0; i--) {
				if (size[i] > 0) {
					ListNode* node;
					node = tail[i];
					TKey key = node->key;
					DeleteNode(node);
					hash_map.erase(key);
					delete node;
					return;
				}
			}
			fprintf(stderr, "no cache err!!! \n");
			return;
		}

	public:
		uint64_t count_all = 0;
		uint64_t count_hit = 0;
		MRUCache(uint64_t max_size):
			capacity(max_size),
			total_size(0) {
			for(int i=0; i<MAX_LEVEL; i++){
				size[i] = 0;
				tail[i] = nullptr;
			}
		}
		
		~MRUCache(){
			Clear();
		}
		uint64_t Size(){
			return total_size;
		}

		void Evict(){
			std::unique_lock<MRUMutex> lock(mutex_);
			InternalEvict();
		}

		bool Find(const TKey& key, TValue& value){
			std::unique_lock<MRUMutex> lock(mutex_);
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
			hash_map.erase(key);
			delete node;
			count_hit++;
			return true;
		}

		bool Insert(const TKey& key, const TValue& value, int level){
			std::unique_lock<MRUMutex> lock(mutex_);
			//if(size + 1 >= capacity)
			auto it = hash_map.find(key);
			if(it != hash_map.end()){
				ListNode* node = it->second;
				if(node->Valid())
					return false;
				node->value = value;
				node->level = level;
				return true;
			}
			ListNode* node = new ListNode(key, value, level);
			hash_map[key] = node;
			PushToBack(node);
			if(total_size >= capacity )
				InternalEvict();
			return true;
		}

		void DeleteKey(const TKey& key){
			std::unique_lock<MRUMutex> lock(mutex_);
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
			std::unique_lock<MRUMutex> lock(mutex_);
			while(total_size > 0 ){
				InternalEvict();
			}
		}
	};

private:
	const int SEG_NUM = 40;
	uint64_t capacity;
	MRUCache **cache_list = nullptr;
	FILE * fp;

	int GetMRUIndex(TKey key){
		return (key / 16) % SEG_NUM;
	}

	ssdlogging::statistics::AvgStat insert_lat;
	ssdlogging::statistics::AvgStat find_lat;
	ssdlogging::statistics::AvgStat delete_lat;
	ssdlogging::statistics::AvgStat evict_lat;
	ssdlogging::statistics::AvgStat test_lat;

public:
	SegMRUCache(uint64_t max_size):capacity(max_size){
		printf("Own MRU Cache: %d\n", SEG_NUM);
		cache_list = new MRUCache* [SEG_NUM];
		for(int i=0; i<SEG_NUM; i++){
			cache_list[i] = new MRUCache(max_size / SEG_NUM);
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
		fprintf(fp,"all: %lu, hit: %lu, rate: %lf, size: %lu, max: %lu\n", all, hit, ((double)hit )/ ((double) all), size_all, capacity);
		if (flush)
		  fflush(fp);
	}

	bool Insert(const TKey& key, const TValue& value, int level){
		auto start = SSDLOGGING_TIME_NOW;
		int index = GetMRUIndex(key);
		bool res =  cache_list[index]->Insert(key, value, level);
		insert_lat.add(SSDLOGGING_TIME_DURATION(start, SSDLOGGING_TIME_NOW));
		return res;
	}

	bool Find(const TKey& key, TValue& value){
		auto start = SSDLOGGING_TIME_NOW;
		int index = GetMRUIndex(key);
		bool res = cache_list[index]->Find(key, value);
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
		//fprintf(stderr, "\nEvict by buffer %d\n\n", num);
		for(int i = 0; i<num; i++){
			int index = ssdlogging::random::Random::GetTLSInstance()->Next() % SEG_NUM;
			cache_list[index]->Evict();
		}
	}

	void DeleteKey(const TKey& key){
		auto start = SSDLOGGING_TIME_NOW;
		int index = GetMRUIndex(key);
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

	virtual ~SegMRUCache(){
		printf("------------Seg MRU Cache---------------\n");
		printf("delete_lat: %ld %.2lf us\n", delete_lat.size(), delete_lat.avg());
		printf("insert_lat: %ld %.2lf us\n", insert_lat.size(), insert_lat.avg());
		printf("find_lat: %ld %.2lf us\n", find_lat.size(), find_lat.avg());
		printf("evict_lat: %ld %.2lf us\n", evict_lat.size(), evict_lat.avg());
		for(int i=0; i<SEG_NUM; i++){
			delete cache_list[i];
		}
		delete cache_list;
		printf("---------------------------------------\n");
	};
};


}
