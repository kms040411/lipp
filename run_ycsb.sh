#!/bin/bash

TEST_ITER=3
DATASET_LIST=(amazon memetracker)
KEY_LENGTH_LIST=(12 128)
WORKLOAD_LIST=(D E)

mkdir results
for (( iter=0; iter<${TEST_ITER}; iter++ ))
do
    for WORKLOAD in ${WORKLOAD_LIST[@]}
    do
        for (( i=0; i<${#DATASET_LIST[@]}; i++ ))
        do
            DATASET=${DATASET_LIST[$i]}
            KEY_LENGTH=${KEY_LENGTH_LIST[$i]}

            echo "Running experiment LIPP_YCSB_${DATASET}_${WORKLOAD}_${iter}"

            timeout 10m ./build/ycsb_${KEY_LENGTH} \
                --fg=16 \
                --workload-length=${DATASET} \
                --workload-type=${WORKLOAD} \
                --table-size=1000000 \
                --mkl-threads=16 \
                --runtime=30 > results/LIPP_YCSB_${DATASET}_${WORKLOAD}_${iter}.txt 2> /dev/null
        done
    done
done

for (( iter=0; iter<${TEST_ITER}; iter++ ))
do
    for WORKLOAD in ${WORKLOAD_LIST[@]}
    do
        for (( i=0; i<${#DATASET_LIST[@]}; i++ ))
        do
            DATASET=${DATASET_LIST[$i]}
            KEY_LENGTH=${KEY_LENGTH_LIST[$i]}

            echo "Running experiment LIPP_YCSB_SIA_${DATASET}_${WORKLOAD}_${iter}"

            timeout 10m ./build/ycsb_${KEY_LENGTH} \
                --fg=16 \
                --workload-length=${DATASET} \
                --workload-type=${WORKLOAD} \
                --table-size=1000000 \
                --mkl-threads=1 \
                --runtime=5 > results/LIPP_YCSB_SIA_${DATASET}_${WORKLOAD}_${iter}.txt 2> /dev/null
        done
    done
done