#pragma once

#include "env/topfs_cache.h"
#include <unordered_map>
#include "lemma/util.h"

namespace rocksdb {

template <class TKey, class TValue>
class SegMRUCache: public TopFSCache<TKey, TValue> {
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

	class MRUCache{
	private:
		uint64_t capacity;
		uint64_t size;
		ListNode *tail;
		std::unordered_map<TKey, ListNode*> hash_map;

		// typedef std::mutex MRUMutex;
		typedef TopFSSpinLock MRUMutex;
		MRUMutex mutex_;

		//ssdlogging::statistics::AvgStat find_lat;

		void DeleteNode(ListNode *node){
			if(node->prev != NULL)
				node->prev->next = node->next;
			if(node->next != NULL)
				node->next->prev = node->prev;
			else
				tail = node->prev;
			node->prev = node->next = NULL;

			size--;
		}

		void PushToBack(ListNode *node){
			ListNode *tmp = tail;
			node->next = NULL;
			node->prev = tmp;
			tail = node;
			if(tmp != NULL)
				tmp->next = node;
			size++;
		}

		void InternalEvict(){
			ListNode* node;
			node = tail;
			if (node == nullptr) {
				fprintf(stderr, "no cache err!!! \n");
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
		MRUCache(uint64_t max_size):
			capacity(max_size),
			size(0),
			tail(nullptr) {}
		
		~MRUCache(){
			Clear();
		}
		uint64_t Size(){
			return size;
		}

		void Evict(){
			std::unique_lock<MRUMutex> lock(mutex_);
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
//			hash_map.erase(key);
//			delete node;
			count_hit++;
			//find_lat.add(SSDLOGGING_TIME_DURATION(start, SSDLOGGING_TIME_NOW));
			PushToBack(node);
			return true;
		}

		bool Insert(const TKey& key, const TValue& value){
			std::unique_lock<MRUMutex> lock(mutex_);
			//if(size + 1 >= capacity)
			auto it = hash_map.find(key);
			if(it != hash_map.end()){
				ListNode* node = it->second;
				if(node->Valid())
					return false;
				node->value = value;
				return true;
			}
			if(size + 1 >= capacity )
				return true;
			ListNode* node = new ListNode(key, value);
			hash_map[key] = node;
			PushToBack(node);
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
			while(tail != nullptr){
				InternalEvict();
			}
		}
	};

private:
	const int SEG_NUM = 40;
	uint64_t capacity;
	MRUCache **cache_list = nullptr;

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
	}

	void PrintHit(){
		uint64_t all = 0;
		uint64_t hit = 0;
		uint64_t size_all = 0;
		for(int i=0; i<SEG_NUM; i++){
			all += cache_list[i]->count_all;
			hit += cache_list[i]->count_hit;
			size_all += cache_list[i]->Size();
		}
		printf("all: %lu, hit: %lu, rate: %lf, size: %lu, max: %lu\n", all, hit, ((double)hit )/ ((double) all), size_all, capacity);
		for(int i=0; i<4; i++){
			printf("%d: %lu\n", i, cache_list[i]->Size());
		}
	}

	bool Insert(const TKey& key, const TValue& value){
		auto start = SSDLOGGING_TIME_NOW;
		int index = GetMRUIndex(key);
		bool res =  cache_list[index]->Insert(key, value);
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
