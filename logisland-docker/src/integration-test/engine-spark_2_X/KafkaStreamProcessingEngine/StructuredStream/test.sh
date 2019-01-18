#!/usr/bin/env bash

my_dir="$(dirname "$0")"
source ${my_dir}/../../../util.sh
# main class that test
main() {
    echo "initializing variables"
    #SET CONSTANT AND ENVIRONMENT VARIABLES
    CONF_FILE="logisland-config.yml"
    KAFKA_OUTPUT_TOPIC="logisland_raw"
    KAFKA_ERROR_TOPIC="logisland_errors"
    #DEBUG="set -x"#Comment if you do not want debug

    KAFKA_BROKER_HOST="kafka"
    KAFKA_BROKER_PORT="9092"
    KAFKA_BROKER_URL="${KAFKA_BROKER_HOST}:${KAFKA_BROKER_PORT}"
    default_value KAFKACAT_BIN "/usr/local/bin/kafkacat"

    export KAFKA_BROKERS="${KAFKA_BROKER_HOST}:${KAFKA_BROKER_PORT}"
    export ZK_QUORUM="zookeeper:2181"

    echo "installing needed dependences for kafka connect"
    bin/components.sh -i com.github.jcustenborder.kafka.connect:kafka-connect-simulator:0.1.118

    echo "starting logisland with ${CONF_FILE}"
    nohup bin/logisland.sh --conf /conf/${CONF_FILE} & > ${CONF_FILE}_job.log

    echo "waiting 20 seconds for job to initialize"
    sleep 20

    echo "some check before sending data"

    # Ensure kafka topic is created before sending data.
    lookup_kafka_topics ${KAFKA_OUTPUT_TOPIC} ${KAFKA_ERROR_TOPIC}

    echo "check that we received it"

    TOPIC_CONTENT=$( \
    ${KAFKA_HOME}/bin/kafka-console-consumer.sh --topic ${KAFKA_OUTPUT_TOPIC} \
    --zookeeper ${ZK_QUORUM} \
    --from-beginning \
    --timeout-ms 2000 \
    --max-messages 10
    )
    abort_if "${?}" "Unable to count events in ${KAFKA_OUTPUT_TOPIC}. Aborting."
    echo "TOPIC_CONTENT is '${TOPIC_CONTENT}'"
    if [[ "" == ${TOPIC_CONTENT} ]]
    then
        echo "topic ${KAFKA_OUTPUT_TOPIC} seems to be empty"
        exit 1
    else
        exit 0
    fi
}

main $@


