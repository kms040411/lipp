/*
 * The code is part of the SIndex project.
 *
 *    Copyright (C) 2020 Institute of Parallel and Distributed Systems (IPADS),
 * Shanghai Jiao Tong University. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include <getopt.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <memory>
#include <random>
#include <string>
#include <vector>
#include <time.h>
#include <csignal>
#include <iostream>
#include <sstream>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <signal.h>

#include "../test_config.h"
#include "../lock.h"
#include "../helper.h"
#include <lipp.h>
#include "mkl.h"

  double current_sec = 0.0;

struct alignas(CACHELINE_SIZE) FGParam;

typedef FGParam fg_param_t;
typedef std::string index_key_t;
typedef LIPP<index_key_t, int, MAX_KEY_SIZE> lipp_t;

inline void prepare_sindex(lipp_t *&table);

void run_benchmark(lipp_t *table, size_t sec);

void *run_fg(void *param);

inline void parse_args(int, char **);

// parameters
double read_ratio = 1;
double insert_ratio = 0;
double update_ratio = 0;
double delete_ratio = 0;
double scan_ratio = 0;
size_t table_size = 1000000;
size_t runtime = 10;
size_t fg_n = 1;
size_t bg_n = 1;

char workload_type = 'a';
char* workload_length = "10m_100m";

volatile bool running = false;
std::atomic<size_t> ready_threads(0);

struct alignas(CACHELINE_SIZE) FGParam {
  lipp_t *table;
  uint64_t throughput;
  uint32_t thread_id;
  bool alive;

  double latency_sum;
  int latency_count;
#ifdef LATENCY_BREAKDOWN
  double group_traversal_sum = 0.0;
  uint32_t group_traversal_count = 0;
  double inference_sum = 0.0;
  uint32_t inference_count = 0;
  double linear_search_sum = 0.0;
  uint32_t linear_search_count = 0;
  double range_search_sum = 0.0;
  uint32_t range_search_count = 0;
  double buffer_search_sum = 0.0;
  uint32_t buffer_search_count = 0;
#endif
};

int main(int argc, char **argv) {
  parse_args(argc, argv);
  lipp_t *tab_xi;
  prepare_sindex(tab_xi);

  is_initial = false;

  mkl_set_num_threads(mkl_threads);
  run_benchmark(tab_xi, runtime);
  if (tab_xi != nullptr) delete tab_xi;
}

inline void prepare_sindex(lipp_t *&table) {
  char filename[256];
  //sprintf(filename, "/home/mskim/workspace/ycsb/%s/workload%c_load.trace", workload_length, workload_type);
  sprintf(filename, "/home/mskim/%s/Workload%c/workload_%c_load", workload_length, workload_type, workload_type);
  std::cout << filename << std::endl;


  struct stat buf;
  int fd = open(filename, O_RDONLY);
  if ( fd < 0 ) {
      printf("Error: %s\n", strerror(errno));
  }
  fstat(fd, &buf);

  char *file = (char*) mmap(0, buf.st_size, PROT_READ|PROT_WRITE, MAP_PRIVATE, fd, 0);
  const char *delim = "\n";
  char *remains = NULL;
  char *token = strtok_r(file, delim, &remains);

  std::vector<std::pair<index_key_t, int>> exist_keys;
  exist_keys.reserve(table_size);
  while (token)
  {
      const char * line = token;
      // skip 6 digits for %c user
      index_key_t query_key(line + 6);
      token = strtok_r(NULL, delim, &remains);

      exist_keys.push_back(std::make_pair(query_key, 1));
  }

  COUT_VAR(exist_keys.size());

  // initilize SIndex (sort keys first)
  std::sort(exist_keys.begin(), exist_keys.end(),
    [](auto const& a, auto const& b) { return a.first < b.first; });
  table = new lipp_t();
  table->bulk_load(exist_keys.data(), exist_keys.size());
}

void *run_fg(void *param) {
  fg_param_t &thread_param = *(fg_param_t *)param;
  uint32_t thread_id = thread_param.thread_id;
  lipp_t *table = thread_param.table;

  std::mt19937 gen(SEED);
  std::uniform_real_distribution<> ratio_dis(0, 1);

  // Read workload trace file of the thread
    char filename[256];
    //sprintf(filename, "/home/mskim/workspace/ycsb/%s/run/workload%c_%d", workload_length, workload_type, thread_id);
    sprintf(filename, "/home/mskim/%s/Workload%c/workload_%c_worker_%d", workload_length, workload_type, workload_type, thread_id);

    struct stat buf;
    int fd = open(filename, O_RDONLY);
    fstat(fd, &buf);

    char *file = (char*) mmap(0, buf.st_size, PROT_READ|PROT_WRITE, MAP_PRIVATE, fd, 0);

    printf("[ycsb] Worker %d Ready.\n", thread_id);

    uint64_t dummy_value = 1234;

    const char *delim = "\n";
    char *remains = NULL;
    char *token = strtok_r(file, delim, &remains);

    ready_threads++;

  while (!running)
    ;

  struct timespec begin_t, end_t;

  while (running) {
    char op;
    int readcount;
    const char *line = token;
    int64_t key;

    if (training_threads > 0) {
        std::unique_lock<std::mutex> lck(training_threads_mutex);             // Acquire lock
        training_threads_cond.wait(lck, []{return (training_threads == 0);}); // Release lock & wait
        lck.unlock();
    }

    #ifdef LIMIT_THROUGHPUT
    if (thread_param.throughput > (current_sec + 1) * MAX_THROUGHPUT) continue;   // Limit Maximum Throughput
    #endif

    // We are not using the `key` here, but just leaving it for now
    //sscanf(line, "%c user%ld %d", &op, &key, &readcount);
    //index_key_t query_key(line + 6);
    op = line[0];
    index_key_t query_key(line + 2);
    token = strtok_r(NULL, delim, &remains);

    if (!token) break;

#ifdef PRINT_LATENCY
    clock_gettime(CLOCK_MONOTONIC, &begin_t);
#endif
    
    switch (op) {
      case 'r':
      {
        volatile auto res = table->at(query_key);
        break;
      }
      case 'u':
      {
        table->insert(std::make_pair(query_key, dummy_value));
        break;
      }
      case 'i':
      {
        table->insert(std::make_pair(query_key, dummy_value));
        break;
      }
      case 'd':
      {
        COUT_N_EXIT("delete not supported");
        break;
      }
      case 's':
      {
        std::array<index_key_t, 10> res;
        table->range_query_len(res.data(), query_key, 10);
        break;
      }
    }
#ifdef PRINT_LATENCY
    clock_gettime(CLOCK_MONOTONIC, &end_t);
    thread_param.latency_sum += (end_t.tv_sec - begin_t.tv_sec) + (end_t.tv_nsec - begin_t.tv_nsec) / 1000000000.0;
    thread_param.latency_count++;
#endif

    thread_param.throughput++;
  }
  thread_param.alive = false;

  #ifdef LATENCY_BREAKDOWN
  thread_param.group_traversal_sum = lt.group_traversal_sum;
  thread_param.group_traversal_count = lt.group_traversal_count;
  thread_param.inference_sum = lt.inference_sum;
  thread_param.inference_count = lt.inference_count;
  thread_param.linear_search_sum = lt.linear_search_sum;
  thread_param.linear_search_count = lt.linear_search_count;
  thread_param.range_search_sum = lt.range_search_sum;
  thread_param.range_search_count = lt.range_search_count;
  thread_param.buffer_search_sum = lt.buffer_search_sum;
  thread_param.buffer_search_count = lt.buffer_search_count;
  #endif

  pthread_exit(nullptr);
}

void sig_handler(int signum) {
    return;
}

void run_benchmark(lipp_t *table, size_t sec) {
  pthread_t threads[fg_n];
  fg_param_t fg_params[fg_n];
  // check if parameters are cacheline aligned
  for (size_t i = 0; i < fg_n; i++) {
    if ((uint64_t)(&(fg_params[i])) % CACHELINE_SIZE != 0) {
      COUT_N_EXIT("wrong parameter address: " << &(fg_params[i]));
    }
  }

  signal(SIGALRM, sig_handler);
  throughput_pid = getpid();

  running = false;
  for (size_t worker_i = 0; worker_i < fg_n; worker_i++) {
    fg_params[worker_i].table = table;
    fg_params[worker_i].thread_id = worker_i;
    fg_params[worker_i].throughput = 0;
    fg_params[worker_i].alive = true;
    fg_params[worker_i].latency_count = 0;
    fg_params[worker_i].latency_sum = 0.0;
    int ret = pthread_create(&threads[worker_i], nullptr, run_fg,
                             (void *)&fg_params[worker_i]);
    if (ret) {
      COUT_N_EXIT("Error:" << ret);
    }
  }

  COUT_THIS("[micro] prepare data ...");
  while (ready_threads < fg_n) sleep(1);

  running = true;
  std::vector<size_t> tput_history(fg_n, 0);
  struct timespec begin_t, end_t;
  uint64_t temp_throughput = 0;
  double temp_sec = 0.0;
  while (current_sec < sec) {
      if (training_threads > 0) {
        std::unique_lock<std::mutex> lck(training_threads_mutex);             // Acquire lock
        training_threads_cond.wait(lck, []{return (training_threads == 0);}); // Release lock & wait
      }

      clock_gettime(CLOCK_MONOTONIC, &begin_t);
      sleep(1);   // Sleep will immediately return when the thread got SIGALRM signal
      clock_gettime(CLOCK_MONOTONIC, &end_t);
      double interval = (end_t.tv_sec - begin_t.tv_sec) + (end_t.tv_nsec - begin_t.tv_nsec) / 1000000000.0;

      uint64_t tput = 0;
      bool threads_alive = false;
      for (size_t i = 0; i < fg_n; i++) {
          tput += fg_params[i].throughput - tput_history[i];
          tput_history[i] = fg_params[i].throughput;
          threads_alive |= fg_params[i].alive;
      }

      std::ostringstream throughput_buf;
      current_sec += interval;
      throughput_buf << "[micro] >>> sec " << current_sec << " throughput: " << (int)(tput / interval) << std::endl;
      std::cout << throughput_buf.str();
      std::flush(std::cout);
      if (!threads_alive) {
          std::ostringstream temp_buf;
          temp_buf << "temp throughput: " << (int)(temp_throughput / temp_sec) << std::endl;
          std::cout << temp_buf.str();
          std::flush(std::cout);
          break;
      } else {
          temp_throughput += tput;
          temp_sec = current_sec;
      }
  }

  running = false;
  void *status;

  #ifdef PRINT_LATENCY
  double all_latency_sum = 0.0;
  int all_latency_count = 0;
  #endif
  for (size_t i = 0; i < fg_n; i++) {
    #ifdef PRINT_LATENCY
    all_latency_count += fg_params[i].latency_count;
    all_latency_sum += fg_params[i].latency_sum;
    #endif

    #ifdef LATENCY_BREAKDOWN
    lt.group_traversal_sum = fg_params[i].group_traversal_sum;
    lt.group_traversal_count = fg_params[i].group_traversal_count;
    lt.inference_sum = fg_params[i].inference_sum;
    lt.inference_count = fg_params[i].inference_count;
    lt.linear_search_sum = fg_params[i].linear_search_sum;
    lt.linear_search_count = fg_params[i].linear_search_count;
    lt.range_search_sum = fg_params[i].range_search_sum;
    lt.range_search_count = fg_params[i].range_search_count;
    lt.buffer_search_sum = fg_params[i].buffer_search_sum;
    lt.buffer_search_count = fg_params[i].buffer_search_count;
    #endif

    int rc = pthread_join(threads[i], &status);
    if (rc) {
      COUT_N_EXIT("Error:unable to join," << rc);
    }
  }

  size_t throughput = 0;
  for (auto &p : fg_params) {
    throughput += p.throughput;
  }
  std::ostringstream final_buf;
  final_buf << "[micro] Throughput(op/s): " << (int)(throughput / current_sec) << std::endl;
  #ifdef PRINT_LATENCY
  final_buf << "[micro] Latency: " << (all_latency_sum / all_latency_count) << std::endl;
  #endif
  std::cout << final_buf.str();
  std::flush(std::cout);

  #ifdef LATENCY_BREAKDOWN
  std::ostringstream latency_buf;
  latency_buf << "[micro] group traverse latency: " << lt.group_traversal_sum / lt.group_traversal_count << std::endl;
  latency_buf << "[micro] inference latency: " << lt.inference_sum / lt.inference_count << std::endl;
  latency_buf << "[micro] linear search latency: " << lt.linear_search_sum / lt.linear_search_count << std::endl;
  latency_buf << "[micro] range search latency: " << lt.range_search_sum / lt.range_search_count << std::endl;
  latency_buf << "[micro] buffer search latency: " << lt.buffer_search_sum / lt.buffer_search_count << std::endl;
  std::cout << latency_buf.str();
  std::flush(std::cout); 
  #endif
}

inline void parse_args(int argc, char **argv) {
  struct option long_options[] = {
      {"read", required_argument, 0, 'a'},
      {"insert", required_argument, 0, 'b'},
      {"remove", required_argument, 0, 'c'},
      {"update", required_argument, 0, 'd'},
      {"scan", required_argument, 0, 'e'},
      {"table-size", required_argument, 0, 'f'},
      {"runtime", required_argument, 0, 'g'},
      {"fg", required_argument, 0, 'h'},
      {"bg", required_argument, 0, 'i'},
      {"workload-length", required_argument, 0, 'w'},
      {"workload-type", required_argument, 0, 't'},
      {"mkl-threads", required_argument, 0, 'z'},
      {0, 0, 0, 0}};
  std::string ops = "a:b:c:d:e:f:g:h:i:w:t:z:";
  int option_index = 0;

  while (1) {
    int c = getopt_long(argc, argv, ops.c_str(), long_options, &option_index);
    if (c == -1) break;

    switch (c) {
      case 0:
        if (long_options[option_index].flag != 0) break;
        abort();
        break;
      case 'a':
        read_ratio = strtod(optarg, NULL);
        INVARIANT(read_ratio >= 0 && read_ratio <= 1);
        break;
      case 'b':
        insert_ratio = strtod(optarg, NULL);
        INVARIANT(insert_ratio >= 0 && insert_ratio <= 1);
        break;
      case 'c':
        delete_ratio = strtod(optarg, NULL);
        INVARIANT(delete_ratio >= 0 && delete_ratio <= 1);
        break;
      case 'd':
        update_ratio = strtod(optarg, NULL);
        INVARIANT(update_ratio >= 0 && update_ratio <= 1);
        break;
      case 'e':
        scan_ratio = strtod(optarg, NULL);
        INVARIANT(scan_ratio >= 0 && scan_ratio <= 1);
        break;
      case 'f':
        table_size = strtoul(optarg, NULL, 10);
        INVARIANT(table_size > 0);
        break;
      case 'g':
        runtime = strtoul(optarg, NULL, 10);
        INVARIANT(runtime > 0);
        break;
      case 'h':
        fg_n = strtoul(optarg, NULL, 10);
        INVARIANT(fg_n > 0);
        break;
      case 'i':
        bg_n = strtoul(optarg, NULL, 10);
        break;
      case 'w':
          workload_length = optarg;
          break;
      case 't':
          memcpy(&workload_type, optarg, 1);
          break;
      case 'z':
        mkl_threads = strtol(optarg, NULL, 10);
        break;
      default:
        abort();
    }
  }

  COUT_THIS("[micro] Read:Insert:Update:Delete:Scan = "
            << read_ratio << ":" << insert_ratio << ":" << update_ratio << ":"
            << delete_ratio << ":" << scan_ratio)
  double ratio_sum =
      read_ratio + insert_ratio + delete_ratio + scan_ratio + update_ratio;
  INVARIANT(ratio_sum > 0.9999 && ratio_sum < 1.0001);  // avoid precision lost
  COUT_VAR(runtime);
  COUT_VAR(fg_n);
  COUT_VAR(bg_n);
  COUT_VAR(workload_length);
  COUT_VAR(workload_type);
}
