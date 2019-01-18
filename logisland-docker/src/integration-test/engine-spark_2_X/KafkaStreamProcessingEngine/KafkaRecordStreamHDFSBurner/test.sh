#!/usr/bin/env bash

my_dir="$(dirname "$0")"
source ${my_dir}/../../../util.sh

# main class that test
main() {

    echo "initializing variables"
    #SET CONSTANT AND ENVIRONMENT VARIABLES
    CONF_FILE="logisland-config.yml"
    INPUT_FILE_PATH="/conf/input"
    EXPECTED_FILE_PATH="/conf/input"
    KAFKA_INPUT_TOPIC="logisland_raw"
    KAFKA_OUTPUT_TOPIC="logisland_events"
#    KAFKA_OUTPUT_TOPIC_2="logisland_aggregations"
    KAFKA_ERROR_TOPIC="logisland_errors"
    #DEBUG="set -x"#Comment if you do not want debug

    KAFKA_BROKER_HOST="kafka"
    KAFKA_BROKER_PORT="9092"
    KAFKA_BROKER_URL="${KAFKA_BROKER_HOST}:${KAFKA_BROKER_PORT}"
    default_value KAFKACAT_BIN "/usr/local/bin/kafkacat"

    export KAFKA_BROKERS="${KAFKA_BROKER_HOST}:${KAFKA_BROKER_PORT}"
    export ZK_QUORUM="zookeeper:2181"

    echo "starting logisland with ${CONF_FILE}"
    nohup bin/logisland.sh --conf /conf/${CONF_FILE} & > ${CONF_FILE}_job.log
    sleep 10
    echo "waiting 10 seconds for job to initialize"

    echo "some check before sending data"
    file_present "${INPUT_FILE_PATH}"
    file_present "${EXPECTED_FILE_PATH}"

    # Ensure kafka topic is created before sending data.
    lookup_kafka_topics ${KAFKA_INPUT_TOPIC} ${KAFKA_OUTPUT_TOPIC} ${KAFKA_ERROR_TOPIC}

    # Sends data to kafka.
    echo "sending input in kafka"
    EXPECTED_DOCS_COUNT=$(${DEBUG}; wc "${INPUT_FILE_PATH}" | awk '{print $1}')
    echo "EXPECTED_DOCS_COUNT ${EXPECTED_DOCS_COUNT}"
#    echo "cat ${INPUT_FILE_PATH} | ${KAFKACAT_BIN} -P -b ${KAFKA_BROKER_URL} -t ${KAFKA_INPUT_TOPIC}"
    echo "cat ${INPUT_FILE_PATH} | ${KAFKA_HOME}/bin/kafka-console-producer.sh --broker-list ${KAFKA_BROKER_URL} --topic ${KAFKA_INPUT_TOPIC}"
    cat ${INPUT_FILE_PATH} | ${KAFKA_HOME}/bin/kafka-console-producer.sh --broker-list ${KAFKA_BROKER_URL} --topic ${KAFKA_INPUT_TOPIC}
    abort_if "${?}" "Unable to send input ${INPUT_FILE_PATH}  into ${KAFKA_INPUT_TOPIC}. Aborting."

    echo "check that we received it"
    #Test first stream pipe
    REAL_DOCS_COUNT=$( \
    ${KAFKA_HOME}/bin/kafka-console-consumer.sh --topic ${KAFKA_OUTPUT_TOPIC} \
    --zookeeper ${ZK_QUORUM} \
    --from-beginning \
    --timeout-ms 2000 \
    | grep '\"id\" :' \
    | wc -l \
    )
    abort_if "${?}" "Unable to count events in ${KAFKA_OUTPUT_TOPIC}. Aborting."
    echo "sent ${EXPECTED_DOCS_COUNT} inputs and got ${REAL_DOCS_COUNT} outputs"
    if [[ ${EXPECTED_DOCS_COUNT} == ${REAL_DOCS_COUNT} ]]
    then
        echo "first stream ok"
    else
        echo "first stream did not receive events"
        exit 1
    fi

    #Test second stream pipe
    sleep 5
    echo "waiting 5 seconds for job to initialize"


    if [ -d "kafka_to_hdfs" ]; then
        echo "directory kafka_to_hdfs has correctly been created"
    else
        echo "directory kafka_to_hdfs has not correctly been created"
        exit 1
    fi

    if [ -d "kafka_to_hdfs/record_daytime=1995-07-01/record_type=apache_log/" ]; then
        echo "directory 'kafka_to_hdfs/record_daytime=1995-07-01/record_type=apache_log/' has correctly been created"
    else
        echo "directory 'kafka_to_hdfs/record_daytime=1995-07-01/record_type=apache_log/' has not correctly been created"
        exit 1
    fi
}

main $@


