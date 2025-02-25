#ifndef LOCK_H
#define LOCK_H

#include "test_config.h"
#include <mutex>
#include <condition_variable>
#include <pthread.h>
#include <signal.h>
#include <unistd.h>
#include <string.h>
#include <atomic>

#include "mkl.h"
#include "mkl_lapacke.h"

uint32_t training_threads = 0;
uint32_t training_iter = 0;
std::mutex training_threads_mutex;
std::condition_variable training_threads_cond;
pthread_t bg_thread[16];

bool finished = false;
pid_t throughput_pid;

// Used for SIndex
size_t mkl_threads = 16;

// Used for LIA Simulation
double total_training_time = 0.0;
int te_n = 4;
bool is_initial = true;

#ifndef THISISCUCKOO
// Used for Latency Breakdown
typedef struct {
    double group_traversal_sum = 0.0;   // Used as tree traversal time in
    uint32_t group_traversal_count = 0; // traditional indexes
    double inference_sum = 0.0;
    uint32_t inference_count = 0;
    double linear_search_sum = 0.0;
    uint32_t linear_search_count = 0;
    double range_search_sum = 0.0;
    uint32_t range_search_count = 0;
    double buffer_search_sum = 0.0;
    uint32_t buffer_search_count = 0;
    double hash_sum = 0.0;
    uint32_t hash_count = 0;
} LatencyData_t;
thread_local LatencyData_t lt;
#endif

void gen_virtual_bg_thread();
void *virtual_bg_thread(void *);
void join_virtual_bg_thread();

void gen_virtual_bg_thread() {
    for (int i=0; i<16; i++) {
        int ret = pthread_create(&bg_thread[i], nullptr, virtual_bg_thread, nullptr);
        if (ret) {
            COUT_N_EXIT("Error: unable to create bg task thread");
        }
    }
    return;
}

void *virtual_bg_thread(void *param) {
    while(!finished) {
        // std::unique_lock<std::mutex> lck(training_threads_mutex);
        // training_threads++;
        // lck.unlock();
        // kill(throughput_pid, SIGALRM);

        // Flush Cache Memory (Assume 25MB Cache)
        // for (int i=0; i<10; i++) {
        //     void *temp_mem = malloc(25 * 1000 * 1000);
        //     memset(temp_mem, 1, 25 * 1000 * 1000);
        //     free(temp_mem);
        // }
        for (int iter=0; iter<10; iter++) {
            const size_t m = exist_keys.size();
            const size_t n = MAX_KEY_SIZE;
            double *a = (double *) malloc(sizeof(double) * m * n);
            double *b = (double *) malloc(sizeof(double) * std::max(m, n));
            for (size_t i=0; i<m; i++) {
                for (size_t j=0; j<n; j++) {
                    a[i * n + j] = (double)(exist_keys[i].first[j]);
                }
                b[i] = i;
            }

            LAPACKE_dgels(LAPACK_ROW_MAJOR, 'N', m, n, 1, a, n, b, 1);

            free(a);
            free(b);
        }
        // lck.lock();
        // training_threads--;
        // training_threads_cond.notify_all();
        // lck.unlock();
    }
}

void join_virtual_bg_thread() {
    void *status;
    finished = true;
    for (int i=0; i<16; i++) {
        int ret = pthread_join(bg_thread[i], &status);
    }
    return;
}

#define GET_INTERVAL(begin_t, end_t) (((end_t).tv_sec - (begin_t).tv_sec) + ((end_t).tv_nsec - (begin_t).tv_nsec) / 1000000000.0)

#endif