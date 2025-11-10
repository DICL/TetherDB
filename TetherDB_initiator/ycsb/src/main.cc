//#include "rocksdb/options.h"
#include "src/rocksdb_client.h" 
#include "src/config.h"
#include "iostream"
#include "cmath"
#include <sys/vfs.h> 
#include "src/ycsb_runner.h"

int main(int argc, char* argv[]){
	utils::Properties common_props;
  std::vector<char> wl_chars;
  parse_command_line_arguments(argc, argv, &common_props, &wl_chars);

  // Workload
  std::vector<CoreWorkload*> workloads;
  //bool warmup_check = true;
  bool warmup_check = false;
  // db options
  rocksdb::Options options;
  //options.spdk_open_mode = 2;
  bool is_load = false;
  for (auto& wl_char : wl_chars) {
    auto wl = new CoreWorkload();
    if (wl_char == 'l') {
      auto wl_props = gen_workload_props('a', common_props);
      wl->Init(wl_props, /*is_load=*/true);
      is_load = true;
    } else {
      if (warmup_check) {
        auto wl_warmup = new CoreWorkload();
        auto wl_props_warmup = gen_workload_props(wl_char, common_props);
        wl_warmup->Init(wl_props_warmup, /*is_load=*/false);
        wl_warmup->warmup_operation_count();
        workloads.push_back(wl_warmup);
        warmup_check = false;
      }
      auto wl_props = gen_workload_props(wl_char, common_props);
      wl->Init(wl_props, /*is_load=*/false);
    }
    workloads.push_back(wl);
  }

  // dbpath
  //char user_name[100] = "lemma";
  std::string dbpath("/lemma/rocksdb");
  std::string data_path("/lemma/rocksdb2");
  //dbpath.append(user_name);

  //options.wal_dir = dbpath + "/wal";
  options.wal_dir = dbpath;
  options.enable_pipelined_write=true;
  options.create_if_missing = true;
  options.max_write_buffer_number = std::stoi(common_props.GetProperty("max_write_buffer_number"));
  options.max_background_jobs = std::stoi(common_props.GetProperty("max_background_jobs"));
  options.max_subcompactions = 3;
  options.compression = rocksdb::kNoCompression;
  //options.max_open_files = 500;
  // *** DCPMM
  //options.env = rocksdb::NewDCPMMEnv(rocksdb::DCPMMEnvOptions());
  // Key-value separation
  // - allocate values with libpmemobj
  /*
  options.dcpmm_kvs_enable = true;
  options.dcpmm_kvs_level = 0;
  options.dcpmm_kvs_mmapped_file_fullpath = dbpath + "/kvs";
  options.dcpmm_kvs_mmapped_file_size = 200*(1ull<<30);  // 200 GB
  options.dcpmm_kvs_value_thres = 64;  // minimal size to do kv sep
  options.dcpmm_compress_value = false;
  */
#if 0
  // Optimized mmap read for pmem
  options.use_mmap_reads = true;
  options.cache_index_and_filter_blocks_for_mmap_read = true;
  rocksdb::BlockBasedTableOptions bbto;
  bbto.block_size = 256;
  options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(bbto));
#endif

  // Open DB
//  std::string data_dir = dbpath + "/db";

#if REMOTE_COMPACTION
  options.greeter = new rocksdb::GreeterClient("11.0.0.91:50052");
#endif
#if ROCKSDB_SPDK
  options.SetSpdkEnv("trtype:RDMA adrfam:IPv4 traddr:11.0.0.91 trsvcid:4521 subnqn:nqn.2019-04.pos:subsystem6");
  if(!is_load) {
    options.spdk_fs->SpdkMigration(dbpath, data_path);
    system("sh ~/drop_cache_test.sh");
  }
#endif
  std::string data_dir = dbpath;
  rocksdb::DB* db;
  printf("OPEN DB %s\n", data_dir.c_str());
  rocksdb::Status s = rocksdb::DB::Open(options, data_dir, &db);
  if(!db) printf("db nullptr\n");

//std::string pout;
//db->GetProperty("rocksdb.block-cache-capacity", &pout);
//std::cerr << pout << std::endl;
////fprintf(stderr, "block_cache_size=%zu\n", options.block-cache-size);
//exit(0);

  // Init and Run Workloads
  int num_threads = std::stoi(common_props.GetProperty("threadcount"));
  YCSBRunner runner(num_threads, workloads, options, data_dir, db);
  runner.run_all();
  fflush(stdout);
#if REMOTE_COMPACTION
  options.greeter->ReturnToken();
#endif
 
  std::this_thread::sleep_for(std::chrono::seconds(60));
  //db->PrintRemoteLocal();
  db->FlushWAL(true); 
  delete db;
#if ROCKSDB_SPDK
  delete options.spdk_fs;
#endif
  return 0;
}
