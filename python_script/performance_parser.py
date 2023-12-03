import os
import sys

LOG_PREFIX="LIPP_SIA"
#INSERTION_RATIO_LIST=["0.25", "0.5", "0.75", "1.0"]
#INSERTION_RATIO_LIST=["D", "E"]
#INSERTION_RATIO_LIST=["d", "e"]
INSERTION_RATIO_LIST=["LATEST_DIST_0.5", "UNIFORM_DIST_0.5", "ZIPF_DIST_0.5", "EXPONENT_DIST_0.5"]
#INSERTION_RATIO_LIST=["31.1",]
#INSERTION_RATIO_LIST=["16", "32", "64", "128"]
TEST_ITERATION=3

if len(sys.argv) < 2:
    print(f"Usage: {sys.argv[0]} [LOG_DIR_PATH]")

log_dir_path = sys.argv[1]

for insertion_ratio in INSERTION_RATIO_LIST:
    counter = 0
    throughput_list = list()
    latency_list = list()
    print(insertion_ratio)

    for iter in range(TEST_ITERATION):
        filename = log_dir_path + "/" + LOG_PREFIX + "_" + insertion_ratio + "_" + str(iter) + ".txt"

        with open(filename, "r") as f:
            f.readline()
            while True:
                line = f.readline()
                if line == "":
                    break
                if "Throughput(op/s)" in line:
                    splitted = line.split()
                    throughput = float(splitted[-1])
                    throughput_list.append(throughput)
                elif "Latency" in line:
                    splitted = line.split()
                    latency = float(splitted[-1])
                    counter += 1
                    latency_list.append(latency)
                    break
    
    assert(len(throughput_list) == counter)
    assert(len(latency_list) == counter)
    assert(counter == TEST_ITERATION)
    
    print(f"=== Insertion ratio: {insertion_ratio} ===")
    print(f"average throughput: {sum(throughput_list) / counter}")
    print(f"average latency : {sum(latency_list) * 1.0 / counter}")
    print(f"===============================================")
