#!/usr/bin/env bash

# define some colors to use for output
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'
# kill and remove any running containers
cleanup () {
  docker-compose -p ci --log-level ERROR kill 1>/dev/null 2>/dev/null
  docker-compose -p ci --log-level ERROR rm -f 1>/dev/null 2>/dev/null
}

# Runs the test specified in argument.
# @param the name of the test to run.
# @param the path to the test to run.
run_test() {
    TEST_NAME=$1
    TEST_PATH=$2
    #go into path
    cd ${TEST_PATH}
    # build and run the composed services
    docker-compose -p ci --log-level ERROR build 1>/dev/null 2>/dev/null && docker-compose -p ci --log-level ERROR up -d 1>/dev/null 2>/dev/null
    if [[ $? -ne 0 ]] ; then
      printf "${RED}Docker Compose Failed in test ${TEST_NAME} ${NC}\n"
      exit -1
    fi
    # wait for the test service to complete and grab the exit code
    echo "waiting for test to complete"
    TEST_EXIT_CODE=`docker wait ci_logisland_1`

    # inspect the output of the test and display respective message
    if [[ -z ${TEST_EXIT_CODE+x} ]] || [[ "${TEST_EXIT_CODE}" -ne 0 ]] ; then
      printf "${RED}Test ${TEST_NAME} in directory ${TEST_PATH} Failed${NC} - Exit Code: $TEST_EXIT_CODE\n"
      docker logs ci_logisland_1
      # output the logs for the test (for clarity)
    else
      printf "${GREEN}Test ${TEST_NAME} Passed${NC}\n"
    fi

    # call the cleanup fuction
    cleanup
    # return to start folder
    cd -
    return ${TEST_EXIT_CODE}
}

main() {
    declare -i FINAL_EXIT_CODE=0
    # catch unexpected failures, do cleanup and output an error message
    trap 'cleanup ; printf "${RED}Tests Failed For Unexpected Reasons${NC}\n"'\
      HUP INT QUIT PIPE TERM

#    declare -a -r test_to_run_paths=(\
#    "engine-spark_2_X/KafkaStreamProcessingEngine/KafkaRecordStreamParallelProcessing" \
#    "engine-spark_2_X/KafkaStreamProcessingEngine/KafkaRecordStreamSQLAggregator" \
#    "engine-spark_2_X/KafkaStreamProcessingEngine/KafkaRecordStreamHDFSBurner" \
#    "engine-spark_2_X/KafkaStreamProcessingEngine/StructuredStream" \
#    "engine-spark_1_6/KafkaStreamProcessingEngine/KafkaRecordStreamParallelProcessing" \
#    "engine-spark_1_6/KafkaStreamProcessingEngine/KafkaRecordStreamSQLAggregator" \
#    "engine-spark_1_6/KafkaStreamProcessingEngine/KafkaRecordStreamHDFSBurner" \
#    )
    declare -a -r test_to_run_paths=(\
    "engine-spark_2_X/KafkaStreamProcessingEngine/KafkaRecordStreamParallelProcessing" \
    "engine-spark_2_X/KafkaStreamProcessingEngine/KafkaRecordStreamSQLAggregator" \
    "engine-spark_2_X/KafkaStreamProcessingEngine/KafkaRecordStreamHDFSBurner" \
    "engine-spark_2_X/KafkaStreamProcessingEngine/StructuredStream" \
    )
#    Uncomment beneath line to test a unique test
#    declare -a -r test_to_run_paths=("engine-spark_2_X/KafkaStreamProcessingEngine/StructuredStream")
    echo "Will execute those integration tests: ${test_to_run_paths[@]}"

    for test_path in "${test_to_run_paths[@]}"
    do
        echo "launching test ${test_path}"

        run_test ${test_path} ${test_path}
        TEST_EXIT_CODE_RETURNED=${?}

        # inspect the output of the test and display respective message
        if [[ -z ${TEST_EXIT_CODE_RETURNED+x} ]] || [[ "${TEST_EXIT_CODE_RETURNED}" -ne 0 ]] ; then
          FINAL_EXIT_CODE=${TEST_EXIT_CODE}
        fi
    done

    if [[ -z ${FINAL_EXIT_CODE+x} ]] || [[ "${FINAL_EXIT_CODE}" -ne 0 ]] ; then
        printf "${RED}Some Tests Failed${NC} - Exit Code: $TEST_EXIT_CODE\n"
    else
        printf "${GREEN}All Tests Passed${NC}\n"
    fi
    # exit the script with the same code as the test service code
    exit ${FINAL_EXIT_CODE}
}

main $@