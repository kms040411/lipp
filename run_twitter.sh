#!/bin/bash

TEST_ITER=1
DATASET_LIST=(12.2 15.5 31.1 37.3)
KEY_LENGTH_LIST=(44 19 47 82)

mkdir results
for (( iter=0; iter<${TEST_ITER}; iter++ ))
do
    for (( i=0; i<${#DATASET_LIST[@]}; i++ ))
    do
        DATASET=${DATASET_LIST[$i]}
        KEY_LENGTH=${KEY_LENGTH_LIST[$i]}

        echo "Running experiment LIPP_TWITTER_${DATASET}_${iter}"

        timeout 10m ./build/twitter_${KEY_LENGTH} \
            --fg=16 \
            --cluster-number=${DATASET} \
            --table-size=1000000 \
            --mkl-threads=16 \
            --runtime=5 > results/LIPP_TWITTER_${DATASET}_${iter}.txt #2> /dev/null
    done
done

for (( iter=0; iter<${TEST_ITER}; iter++ ))
do
    for (( i=0; i<${#DATASET_LIST[@]}; i++ ))
    do
        DATASET=${DATASET_LIST[$i]}
        KEY_LENGTH=${KEY_LENGTH_LIST[$i]}

        echo "Running experiment LIPP_SIA_TWITTER_${DATASET}_${iter}"

        timeout 10m ./build/twitter_${KEY_LENGTH} \
            --fg=16 \
            --cluster-number=${DATASET} \
            --table-size=1000000 \
            --mkl-threads=1 \
            --runtime=5 > results/LIPP_SIA_TWITTER_${DATASET}_${iter}.txt 2> /dev/null
    done
done