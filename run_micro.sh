#!/bin/bash

TEST_ITER=1

QUERY_DISTS=(LATEST_DIST)

mkdir results
for (( iter=0; iter < ${TEST_ITER}; iter++ ))
do
    for DIST in ${QUERY_DISTS[@]}
    do
        if [ ${DIST} = "HOTSPOT_DIST" ]
        then
            INSERTION_RATIO=0.333
            READ_RATIO=0.334
            SCAN_RATIO=0.0
            REMOVE_RATIO=0.333
        else
            INSERTION_RATIO=0.01
            READ_RATIO=0.99
            SCAN_RATIO=0.0
            REMOVE_RATIO=0.0
        fi
        
        # echo "Running experiment LIPP_${DIST}_${INSERTION_RATIO}_${iter}"

        # timeout 10m ./build/bench_${DIST} \
        #     --fg=16 \
        #     --bg=1 \
        #     --insert=${INSERTION_RATIO} \
        #     --read=${READ_RATIO} \
        #     --scan=${SCAN_RATIO} \
        #     --remove=${REMOVE_RATIO} \
        #     --initial-size=1000000 \
        #     --target-size=10000000 \
        #     --table-size=10000000 \
        #     --mkl-threads=16 \
        #     --runtime=30 > results/LIPP_${DIST}_${INSERTION_RATIO}_${iter}.txt 2> /dev/null

        echo "Running experiment LIPP_SIA_${DIST}_${INSERTION_RATIO}_${iter}"

        ./build/bench_${DIST} \
            --fg=16 \
            --bg=1 \
            --insert=0.01 \
            --read=0.99 \
            --scan=${SCAN_RATIO} \
            --remove=${REMOVE_RATIO} \
            --initial-size=2000000 \
            --target-size=20000000 \
            --table-size=20000000 \
            --mkl-threads=1 \
            --runtime=30 > results/LIPP_SIA_${DIST}_${INSERTION_RATIO}_${iter}.txt 2> /dev/null

    done
done
