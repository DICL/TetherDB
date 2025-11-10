cmake_minimum_required(VERSION 3.10)

project(rocksdb-ycsb)

set(CMAKE_CXX_COMPILER "g++")
set(CMAKE_C_FLAGS "-Wall -Wsign-compare -O3 -g -DNDEBUG")

##
# Compiler options
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wfatal-errors ")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DON_DCPMM")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -I/lemma2/rocksdb_local/include")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -isystem /home/lemma/.local/include")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -isystem /lemma2/spdk_rocksdb/spdk/include")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -march=native -D_GNU_SOURCE -fPIC -fstack-protector -fno-common -I/lemma2/spdk_rocksdb/spdk/isa-l/.. -U_FORTIFY_SOURCE -D_FORTIFY_SOURCE=2 -DSPDK_GIT_COMMIT=4ac203b2d -lssl -Wl,-z,relro,-z,now -Wl,-z,noexecstack -fuse-ld=bfd -L/lemma2/spdk_rocksdb/spdk/build/lib -Wl,--whole-archive -Wl,--no-as-needed -lspdk_sock_posix -lspdk_nvme -lspdk_sock -lspdk_rpc -lspdk_jsonrpc -lspdk_json -lspdk_rdma -lspdk_util -lspdk_dma -lspdk_vmd -lspdk_log -Wl,--no-whole-archive /lemma2/spdk_rocksdb/spdk/build/lib/libspdk_env_dpdk.a -Wl,-rpath-link /lemma2/spdk_rocksdb/spdk/dpdk/build/lib -Wl,--whole-archive /lemma2/spdk_rocksdb/spdk/dpdk/build/lib/librte_eal.a /lemma2/spdk_rocksdb/spdk/dpdk/build/lib/librte_mempool.a /lemma2/spdk_rocksdb/spdk/dpdk/build/lib/librte_ring.a /lemma2/spdk_rocksdb/spdk/dpdk/build/lib/librte_mbuf.a /lemma2/spdk_rocksdb/spdk/dpdk/build/lib/librte_bus_pci.a /lemma2/spdk_rocksdb/spdk/dpdk/build/lib/librte_pci.a /lemma2/spdk_rocksdb/spdk/dpdk/build/lib/librte_mempool_ring.a /lemma2/spdk_rocksdb/spdk/dpdk/build/lib/librte_power.a /lemma2/spdk_rocksdb/spdk/dpdk/build/lib/librte_ethdev.a /lemma2/spdk_rocksdb/spdk/dpdk/build/lib/librte_net.a /lemma2/spdk_rocksdb/spdk/dpdk/build/lib/librte_telemetry.a /lemma2/spdk_rocksdb/spdk/dpdk/build/lib/librte_kvargs.a /lemma2/spdk_rocksdb/spdk/dpdk/build/lib/librte_vhost.a /lemma2/spdk_rocksdb/spdk/dpdk/build/lib/librte_cryptodev.a /lemma2/spdk_rocksdb/spdk/dpdk/build/lib/librte_hash.a /lemma2/spdk_rocksdb/spdk/dpdk/build/lib/librte_rcu.a -Wl,--no-whole-archive  -lnuma -ldl  -libverbs -lrdmacm -L/lemma2/spdk_rocksdb/spdk/isa-l/.libs -lisal -pthread -lrt -luuid -lcrypto -lm -libverbs -lrdmacm -laio -ltbb")
set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -std=c++17 -march=native -fno-rtti ")

#include_directories(../build/include)
#include_directories(${PROJECT_SOURCE_DIR})
#link_directories(../build/lib)
link_directories(../)
link_directories(/lemma2/spdk_rocksdb/spdk)
link_directories(/home/lemma/.local/)
#link_directories(../build)

add_executable(ycsb
  src/main.cc
  src/rocksdb_client.cc
  core/core_workload.cc
  )
#../build/lib/librocksdb.a

target_include_directories(ycsb PRIVATE
  #../build/include
  ../include
  ../
  /home/lemma/.local/lib/
  /lemma2/spdk_rocksdb/spdk/
  ${PROJECT_SOURCE_DIR}
  )
target_link_libraries(ycsb
  pthread
  stdc++fs
  numa
  rocksdb
  dl
  zstd
  lz4
  bz2
  snappy
  z
  /home/lemma/.local/lib/libgrpc++_reflection.a
  /home/lemma/.local/lib/libgrpc++.a
  /home/lemma/.local/lib/libprotobuf.a
  /home/lemma/.local/lib/libgrpc.a
  /home/lemma/.local/lib/libssl.a
  /home/lemma/.local/lib/libcrypto.a
  /home/lemma/.local/lib/libz.a
  /home/lemma/.local/lib/libcares.a
  /home/lemma/.local/lib/libre2.a
  /home/lemma/.local/lib/libabsl_statusor.a
  /home/lemma/.local/lib/libabsl_hash.a
  /home/lemma/.local/lib/libabsl_bad_variant_access.a
  /home/lemma/.local/lib/libabsl_city.a
  /home/lemma/.local/lib/libabsl_raw_hash_set.a
  /home/lemma/.local/lib/libabsl_hashtablez_sampler.a
  /home/lemma/.local/lib/libabsl_exponential_biased.a
  /home/lemma/.local/lib/libgpr.a
  /home/lemma/.local/lib/libabsl_status.a
  /home/lemma/.local/lib/libabsl_cord.a
  /home/lemma/.local/lib/libabsl_bad_optional_access.a
  /home/lemma/.local/lib/libabsl_synchronization.a
  /home/lemma/.local/lib/libabsl_stacktrace.a
  /home/lemma/.local/lib/libabsl_symbolize.a
  /home/lemma/.local/lib/libabsl_debugging_internal.a
  /home/lemma/.local/lib/libabsl_demangle_internal.a
  /home/lemma/.local/lib/libabsl_graphcycles_internal.a
  /home/lemma/.local/lib/libabsl_time.a
  /home/lemma/.local/lib/libabsl_civil_time.a
  /home/lemma/.local/lib/libabsl_time_zone.a
  /home/lemma/.local/lib/libabsl_malloc_internal.a
  /home/lemma/.local/lib/libabsl_str_format_internal.a
  /home/lemma/.local/lib/libabsl_strings.a
  /home/lemma/.local/lib/libabsl_strings_internal.a
  /home/lemma/.local/lib/libabsl_int128.a
  /home/lemma/.local/lib/libabsl_throw_delegate.a
  /home/lemma/.local/lib/libabsl_base.a
  /home/lemma/.local/lib/libabsl_raw_logging_internal.a
  /home/lemma/.local/lib/libabsl_log_severity.a
  /home/lemma/.local/lib/libabsl_spinlock_wait.a
  /home/lemma/.local/lib/libaddress_sorting.a
  /home/lemma/.local/lib/libupb.a)
