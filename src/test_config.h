#ifndef TEST_CONFIG
#define TEST_CONFIG

#include <iostream>

// #define MAX_KEY_SIZE 44
#define SEED 385726
//#define INSERTED_RANDOM
#define ENTIRE_RANDOM
#define PRINT_LATENCY
//#define SINGLE_THREADED
#define MAX_RANGE_RECORDS 300

// Used for Latency Breakdown
//#define LATENCY_BREAKDOWN

// Used for setting training Interval
#define IDEAL_TRAINING_INTERVAL 300

#define SIM_PATH "/home/mskim/Least_Square_Accelerator_Simulator/build"
#define TMP_INPUT_PATH "/tmp/mskim/input_matrix/"
#define TMP_OUTPUT_PATH "/tmp/mskim/output_matrix/"
#define CYCLE_TABLE_PATH "/home/mskim/index_microbench/cached_cycle"

#define USE_PREFIX

//#define LIMIT_THROUGHPUT
//#define MAX_THROUGHPUT 7500

/*****************************
 * Microbench Query Patterns *
 *****************************/

// #define SEQUENTIAL_DIST
//#define UNIFORM_DIST
// #define LATEST_DIST
// #define EXPONENT_DIST
// #define ZIPF_DIST
// #define HOTSPOT_DIST
#define EXP_LAMBDA 10 // Used in exponent dist
#define HOTSPOT_LENGTH 1000 // Used in hotspot dist

#define COUT_THIS(this) std::cout << this << std::endl;
#define COUT_VAR(this) std::cout << #this << ": " << this << std::endl;
#define COUT_POS() COUT_THIS("at " << __FILE__ << ":" << __LINE__)
#define COUT_N_EXIT(msg) \
  COUT_THIS(msg);        \
  COUT_POS();            \
  abort();
#define INVARIANT(cond)            \
  if (!(cond)) {                   \
    COUT_THIS(#cond << " failed"); \
    COUT_POS();                    \
    abort();                       \
  }

#endif
