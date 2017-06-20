#!/usr/bin/env bash

#
# This script performs integration tests against logisland in either docker or clustered mode.
#
# A test is defined by a directory located under the 'data' directory. The test directory must
# contain an 'input' and an 'expected' file. The 'input' file is the file sent to logisland
# using the kafkacat tool. The 'expected' file contains the data as it should be after logisland
# processing.
#
# Main test flow:
# 1. send data contained in 'input' to kafka topic dedicated for the running test (<test_name>_raw)
# 2. for all supported elasticsearch versions:
#    -> wait documents to be inserted in elasticsearch
#    -> retrieve from elasticsearch the 'record_raw_value' field for all documents, sort them
#       and store them in a file
#    -> check each value of 'record_raw_value' with data sent by performing a diff with the
#       'expected' file.
#
# By default, all tests represented by directories located in data are ran. Use '-t' to specify
# one or more particular tests.
#
# Before tests are executed, that script attempts to detect that logisland is up and running.
# This is achieved by polling the status of an elasticsearch server for 'yellow' or 'green'.
# The default elasticsearch instance polled is:
# - the one running in logisland in docker mode
# - http://localhost:9200
# The URL of the elasticsearch to poll can be overridden by setting the environment variable ES_URL.
#
# Default values:
# - LOGISLAND_DOCKER_IMAGE_NAME "logisland-hdp2.4"
# - HBASE_DOCKER_IMAGE_NAME "hbase112"
# - ES23_DOCKER_IMAGE_NAME "${LOGISLAND_DOCKER_IMAGE_NAME}"
# - ES24_DOCKER_IMAGE_NAME "elasticsearch24"
# - ES5_DOCKER_IMAGE_NAME "elasticsearch5"
# - KAFKACAT_BIN "/usr/local/bin/kafkacat"
# - DOCKER_BIN `which docker 2> /dev/null`
# - CURL_BIN `which curl 2> /dev/null`
# - KAFKA_BROKER_HOST "${DEFAULT_KAFKA_BROKER_HOST}"
# - KAFKA_BROKER_PORT "9092"
# - KAFKA_URL "${KAFKA_BROKER_HOST}:${KAFKA_BROKER_PORT}"
# - ES_HOST "${DEFAULT_ES_HOST}"
# - ES_PORT "9200"
# - ES_URL "http://${ES_HOST}:${ES_PORT}"
#

#-------------------------------------------------------------------------------

#   __                  _   _                      __        _   _ _
#  / _|_   _ _ __   ___| |_(_) ___  _ __  ___     / /  _   _| |_(_) |___
# | |_| | | | '_ \ / __| __| |/ _ \| '_ \/ __|   / /  | | | | __| | / __|
# |  _| |_| | | | | (__| |_| | (_) | | | \__ \  / /   | |_| | |_| | \__ \
# |_|  \__,_|_| |_|\___|\__|_|\___/|_| |_|___/ /_/     \__,_|\__|_|_|___/
#

# Runs the test specified in argument.
# @param the name of the test to run.
run_test()
{
  require_non_null RESOURCE_DIR

  require_args "${#}" 1 "run_test <test-name>"
  local TEST="${1}"
  require_non_null TEST

  local TEST_DATA_DIR="${DATA_DIR}/${TEST}"

  # Fully qualified name of test required files.
  INPUT_FILE="${TEST_DATA_DIR}/input"
  file_present "${INPUT_FILE}"
  EXPECTED_FILE="${TEST_DATA_DIR}/expected"
  file_present "${EXPECTED_FILE}"

  KAFKA_TOPIC="${TEST}_raw"

  # Fully qualified name of input file mounted inside docker image.
  DOCKER_MOUNTED_INPUT_FILE="${INPUT_FILE:${#RESOURCE_DIR}}"

  # Sends data to kafka.
  EXPECTED_DOCS_COUNT=$(${DEBUG}; wc "${INPUT_FILE}" | awk '{print $1}')
  kafkacat "${DOCKER_MOUNTED_INPUT_FILE}"
  abort_if "${?}" "Unable to perform kafkacat ${DOCKER_MOUNTED_INPUT_FILE}. Aborting."

  # Check results for all elasticsearch supported versions.
  TEST_RESULT=0
  for ES_TAG in ${ELASTICS[@]}
  do
    URL=$(eval echo "\$${ES_TAG}_EXPOSED_9200")
    check_results "${URL}" "${TEST}" "${EXPECTED_DOCS_COUNT}"

    if test ${?} -ne 0 -a ${TEST_RESULT} -eq 0
    then
      TEST_RESULT=1
    fi
  done

  clean_up

  return "${TEST_RESULT}"
}

# Returns 0 if results are the ones expected; another value otherwise.
# First check that the expected number of documents is present in elasticsearch.
# If so, then retrieve documents from elasticsearch and compare the raw field with the input field.
# @param URL of elasticsearch to check.
# @param type of elasticsearch documents
# @param number of expected documents.
check_results()
{
  require_args "${#}" 3 "check_results <es-url> <es-type> <expected-doc-count>"

  local URL="${1}"
  local TEST="${2}"
  local EXPECTED_DOCS_COUNT="${3}"

  # Waits for documents to be present in elasticsearch.
  ensure_elasticsearch_docs_added "${URL}" "${TEST}" "${EXPECTED_DOCS_COUNT}"
  local result=$?

  if [[ ${result} -eq 0 ]]
  then
    # Dump elasticsearch to analyze inserted documents.
    mktmp_file "TMP_RESULT" "/tmp/es-"
    query_elasticsearch "${URL}/logisland.*/${TEST}/_search?pretty" \
                        "{\\\"query\\\": { \\\"match_all\\\": {} },\\\"_source\\\": [\\\"record_raw_value\\\"]}" \
                        "${TMP_RESULT}"
    mktmp_file "TMP_EXPECTED" "/tmp/es-expected"
    # Grep 'record_raw_value' and trim.
    cat "${TMP_RESULT}" | awk '$0 ~ /record_raw_value/{$1=$1;print}' | sort > "${TMP_EXPECTED}"

    diff "${EXPECTED_FILE}" "${TMP_EXPECTED}"
    result=$?
  fi

  return $result
}

# Performs cleanup by
# - removing files in TMP_FILES
clean_up()
{
  debug "Performing cleanup."

  for file in ${TMP_FILES}
  do
    if [[ -z ${KEEP_FILES} ]]
    then
      debug "Deleting file $file"
      rm "$file"
    else
      debug "Leave untouched $file"
    fi
  done
}

# Returns a temporary file that will be automatically removed when the script will end.
# @param the name of the variable that will contain the temporary filename
# @param an optional prefix (default is /tmp/<script-name>
mktmp_file()
{
  local TMP_FILE=${2:-/tmp/$SCRIPT_NAME}.$$.$RANDOM

  eval $1=$TMP_FILE

  TMP_FILES="$TMP_FILES $TMP_FILE"
  export TMP_FILES
}

# Prints the usage of the script and exit.
usage()
{
  echo "$SCRIPT_NAME [-f] [-h] [-d] [-D<property>=<value>]* [-v] [-i] [-t test[,test]]"
  echo "  -d: debug"
  echo "  -D: set property in form <property>=<value> (eg. -DmyVar=myVal)"
  echo "  -f: force deletion of logisland indices in elasticsearch before starting"
  echo "  -h: usage"
  echo "  -k: keep files"
  echo "  -i: run tests assuming logisland is installed and not running in a docker image"
  echo "  -t: test names separated by comma"
  echo "  -v: verbose"

  abort 0
}

# Parses arguments.
# @param(s) all arguments to parse
parse_args()
{
  while getopts "dD:fhikt:v" option
  do
    case ${option} in
      d) DEBUG="set -x"
         ;;

      D) property "${OPTARG}"
         ;;

      f) ES_CLEANUP_AT_STARTUP="1"
         ;;

      h) usage
         ;;

      i) NO_DOCKER="1"
         ;;

      k) KEEP_FILES="1"
         ;;

      t) TESTS_ARG="${OPTARG}"
         ;;

      v) VERBOSE="1"
         ;;

      \?) usage
         ;;
    esac
  done
}

# Evaluates the provided property assignment in the form '<var>=<val>'.
property()
{
  require_args "${#}" 1 "property <var=val>"

  eval ${1}

  abort_if "${?}" "Failed to assign property with command: ${1}"
}

# Initializes the environment by setting the relevant variables such as hosts, ports, ...
init_env()
{
  default_value LOGISLAND_DOCKER_IMAGE_NAME "logisland-hdp2.4"
  default_value HBASE_DOCKER_IMAGE_NAME "hbase112"
  # elasticsearch 2.3.3 runs in logisland docker image
  default_value ES23_DOCKER_IMAGE_NAME "${LOGISLAND_DOCKER_IMAGE_NAME}"
  default_value ES24_DOCKER_IMAGE_NAME "elasticsearch24"
  default_value ES5_DOCKER_IMAGE_NAME "elasticsearch5"

  # All supported elasticsearch versions.
  ELASTICS=( "ES23" "ES24" "ES5" )

  default_value KAFKACAT_BIN "/usr/local/bin/kafkacat"
  default_value DOCKER_BIN `which docker 2> /dev/null`
  default_value CURL_BIN `which curl 2> /dev/null`

  if [[ -z "${NO_DOCKER}" ]]
  then
    # Services run within docker images.

    # Sets variables LOGISLAND_DOCKER_CONTAINER_IP and LOGISLAND_EXPOSED_9200.
    init_docker_env "LOGISLAND"

    # Sets variables ES*_DOCKER_CONTAINER_IP and ES*_EXPOSED_9200 for all elasticsearch supported versions
    # used to reach elasticsearch servers on {ES*_DOCKER_CONTAINER_IP}:{ES*_EXPOSED_9200}
    for ES_TAG in ${ELASTICS[@]}
    do
      init_docker_env "${ES_TAG}"
    done

    DEFAULT_KAFKA_BROKER_HOST=${LOGISLAND_DOCKER_IMAGE_NAME}
    IFS=: read ES_HOST ES_PORT <<< ${ES23_EXPOSED_9200}
  else
    # Assume services run on localhost by default.
    DEFAULT_KAFKA_BROKER_HOST="localhost"
    DEFAULT_ES_HOST="localhost"
  fi

  # Assign value to host and port if not already defined by shell environment.
  default_value KAFKA_BROKER_HOST "${DEFAULT_KAFKA_BROKER_HOST}"
  default_value KAFKA_BROKER_PORT "9092"
  default_value KAFKA_URL "${KAFKA_BROKER_HOST}:${KAFKA_BROKER_PORT}"
  verbose "KAFKA_URL=${KAFKA_URL}"

  default_value ES_HOST "${DEFAULT_ES_HOST}"
  default_value ES_PORT "9200"
  default_value ES_URL "http://${ES_HOST}:${ES_PORT}"
  verbose "ES_URL=${ES_URL}"
}

# Looks for the running docker images of the specified tag and builds variables <prefix>_DOCKER_CONTAINER_IP and
# <prefix>_EXPOSED_9200 where <prefix> is provided as second parameter.
#
# @param tag a tag that denotes a valid container by evaluating ${TAG}_DOCKER_IMAGE_NAME
init_docker_env()
{
  require_args "${#}" 1 "init_docker_env <tag>"

  local TAG="${1}"
  local IMAGE_NAME=$(eval echo "\$${TAG}_DOCKER_IMAGE_NAME")

  local DOCKER_CONTAINER_IP=`${DOCKER_BIN} inspect -f "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}" "${IMAGE_NAME}"`
  require_non_null "DOCKER_CONTAINER_IP" "Unable to obtain ip address of docker container '${IMAGE_NAME}'"
  eval "${TAG}_DOCKER_CONTAINER_IP=${DOCKER_CONTAINER_IP}"

  # Retrieve port mapped from <container>:9200 on localhost.
  local ES_EXPOSED_9200=`${DOCKER_BIN} port "${IMAGE_NAME}" 9200/tcp`
  require_non_null "ES_EXPOSED_9200" "Unable to obtain elasticsearch port to query 9200/tcp in docker container '${ES_EXPOSED_9200}'"
  eval "${TAG}_EXPOSED_9200=${ES_EXPOSED_9200}"
}

# Prints the provided log if DEBUG is set.
# @param(s) the log message
debug()
{
  if [[ -n "${DEBUG}" ]]
  then
    >&2 echo "${@}"
  fi
}

# Prints the provided log if VERBOSE is set.
# @param(s) the log message
verbose()
{
  if [[ -n "${VERBOSE}" ]] || [[ -n "${DEBUG}" ]]
  then
    >&2 echo "${@}"
  fi
}

# Aborts if the specified variable is not defined.
# @param the variable name to check for nullity
# @param the message in case the specified variable name is undefined (optional).
require_non_null()
{
  require_args "${#}" 1 "require_non_null <var-name> [message]"

  local VARIABLE_NAME=\$"${1}"
  local VARIABLE_VALUE=`eval "expr \"$VARIABLE_NAME\""`

  MESSAGE="${2:-Variable ${1} is empty. Aborting.}"

  test -n "${VARIABLE_VALUE}"

  abort_if "${?}" "${MESSAGE}"
}

# Assigns the provided variable to the specified value if the variable is undefined.
# @param the variable name to check for nullity
# @param the variable value to assign in case the variable is undefined
default_value()
{
  require_args "${#}" 2 "default_value <var-name> <value>"

  local VARIABLE_NAME=\$"${1}"
  local VARIABLE_VALUE=`eval "expr \"$VARIABLE_NAME\""`

  if [[ -z "${VARIABLE_VALUE}" ]]
  then
    eval "${1}=${2}"
  fi

  debug "Default value ${1}="`eval "expr \\$${1}"`
}

# Aborts if the expected argument check failed.
# @param the number of arguments passed to the calling function
# @param the number of arguments expected by the calling function
# @param the name of the calling function
require_args()
{
  test "${1}" -ge "${2}"
  abort_if "${?}" "function '${3}' expects ${2} argument but ${1} provided. Aborting."
}

# Aborts if the condition code specified as arg #1 is not 0 and prints the aborting message specified as arg #2.
# @param the condition code
# @param the aborting message
abort_if()
{
  if [[ "${#}" -ne 2 ]]
  then
    echo "function 'abort_if' expects 2 arguments but ${#} provided. Aborting."
    abort 1
  fi

  if [[ "${1}" -ne 0 ]]
  then
    echo "$2"
    abort 1
  fi
}

# Ensures the specified file is present. Abort the script if the file is not found.
# @param the file to check
file_present()
{
  require_args "${#}" 1 "file_present <filepath>"

  test -f "$1"

  abort_if "${?}" "File '$1' was not found. Aborting."
}

# Aborts the current script performing any necessary cleanup action.
# @param the exit code of the script or -1 if none provided
abort()
{
  cd "${WORKING_DIR}"

  clean_up

  # Returns 1 in case no exit code provided.
  exit "${1:-1}"
}

# Polls by executing the specified command the specified number of attempts waiting the specified amount of seconds
# between each attempt.
# @param number of attempts
# @param amount of seconds between each attempt
# @param(s) the command to run
poll()
{
  local TOTAL_ATTEMPTS=$1
  local WAIT_IN_SECONDS=$2
  local args=( "$@" )
  local CMD=${args[@]:2}
  debug "Polling command: ${CMD}"

  local returnCode=1 # fail by default

  local attempts=0
  while [[ ${attempts} -le ${TOTAL_ATTEMPTS} ]]
  do
    attempts=$(( $attempts + 1 ))
    # Run omitting parameters #1 and #2
    ${CMD} > /dev/null
    returnCode=$?
    if [ ${returnCode} -eq 0 ]
    then
      break
    fi
    sleep ${WAIT_IN_SECONDS}
  done

  return ${returnCode}
}

# Returns 0 if the health status of elasticsearch reachable at provided url is either "yellow" or "green".
# @param url of elasticsearch
fetch_elasticsearch_status()
{
  require_args "${#}" 1 "fetch_elasticsearch_status <es-url>"

  # Fetch health status from elasticsearch trimming returned status.
  [[ `${CURL_BIN} -fs "${1}/_cat/health?h=status" | tr -d '[:space:]'` =~ ^(yellow|green)$ ]]
}

# Ensures that logisland is ready by polling the 'official' elasticsearch's health status denoted by ES_URL.
ensure_logisland_ready()
{
  debug "Polling elasticsearch on ${ES_URL}"

  # Wait up to 3 minutes for logisland to be up and running.
  poll 180 1 fetch_elasticsearch_status "${ES_URL}"
}

# Sends the specified file to Kafka using the command kafkacat within the docker container.
#
# @required KAFKACAT_BIN: the kafkacat binary
# @required KAFKA_URL: the kafka broker connection
# @required KAFKA_TOPIC: the kafka queue name
#
# In case docker is used for the tests, the properties below are also required
# @required DOCKER_BIN: the docker binary
# @required LOGISLAND_DOCKER_CONTAINER_ID: the docker container identifier
#
# @param file to send using kafkacat
kafkacat()
{
  if [[ -z "${NO_DOCKER}" ]]
  then
    require_non_null LOGISLAND_DOCKER_IMAGE_NAME
    require_non_null KAFKACAT_BIN
    require_non_null KAFKA_URL
    require_non_null KAFKA_TOPIC

    require_args "${#}" 1 "kafkacat <input-file>"

    local CMD="${DOCKER_BIN} exec -t ${LOGISLAND_DOCKER_IMAGE_NAME} bash -c 'cat $1 | ${KAFKACAT_BIN} -P -b ${KAFKA_URL} -t ${KAFKA_TOPIC}'"
  else
    local CMD="cat $1 | ${KAFKACAT_BIN} -P -b ${KAFKA_URL} -t ${KAFKA_TOPIC}"
  fi

  (${DEBUG}; bash -c "${CMD}")
}

# Creates the HBase table provided as argument.
# @param the name of the table to create.
create_hbase_table()
{
  require_args "${#}" 1 "ensure_hbase_table <table_name>"

  local table_name="${1}"

  hbase_cli "exists \'${table_name}\'" | grep -q "Table ${table_name} does exist"
  # 0 means match
  local table_exists=$?

  if [[ "${table_exists}" -ne  0 ]]
  then
    debug "HBase table '${table_name}' does not exist"

    hbase_cli "create \'${table_name}\',\'e\'"  | grep -q "ERROR: Table already exists: ${table_name}!"
    local metadata_present_in_zookeeper=$?

    if [[ "${metadata_present_in_zookeeper}" -eq  0 ]]
    then
      local znode="/hbase/table/${table_name}"
      debug "Metadata of HBase table '${table_name}' present is zookeeper. Deleting znode '${znode}'"

      # The data are not present in the hbase docker image
      # but metadata are present in zookeeper running in the logisland image.
      zookeeper_cli delete $znode 2>&1 >/dev/null

      # Re-try.
      hbase_cli "create \'${table_name}\',\'e\'"  | grep -q "ERROR: Table already exists: ${table_name}!"
      abort_if "${?}" "Fail to create hbase table '${table_name}'"
    fi
  else
    debug "HBase table '${table_name}' already exists"
  fi
}

# Executes the provided command arguments in 'hbase shell' in the HBase docker image.
# @param(s) the full command
hbase_cli()
{
  exec_in_docker "${HBASE_DOCKER_IMAGE_NAME}" /data/webanalytics/hbase/run-command.sh $@
}

# Executes the provided command arguments in a zookeeper-cli like in the logisland docker image.
# @param(s) the full command
zookeeper_cli()
{
  exec_in_docker "${LOGISLAND_DOCKER_IMAGE_NAME}" /data/webanalytics/zookeeper/run-command.sh $@
}

# Executes the specified command in the specified docker image.
#
# @required DOCKER_BIN: the docker binary
#
# @param the docker image name
# @param(s) the command to execute
exec_in_docker()
{
  if [[ -z "${NO_DOCKER}" ]]
  then
    require_non_null DOCKER_BIN

    local DOCKER_IMAGE_NAME=$1
    require_non_null DOCKER_IMAGE_NAME

    local args=( "$@" )
    local CMD=${args[@]:1}

    local DOCKER_CMD="${DOCKER_BIN} exec -t ${DOCKER_IMAGE_NAME} bash -c \"${CMD}\""

    (${DEBUG}; bash -c "${DOCKER_CMD}")
  fi
}

# Returns the number of documents from the provided elasticsearch URL.
#
# @param URL to contact elasticsearch.
# @param type of documents in elasticsearch.
get_elasticsearch_docs_count()
{
  require_args "${#}" 2 "get_elasticsearch_docs_count <es-url> <es-type>"

  (${DEBUG}; ${CURL_BIN} -f -s ${1}/logisland.*/${2}/_count?pretty | grep "count" | sed 's/[^0-9]*\([0-9]*\).*/\1/')
}

# Ensures documents were added to elasticsearch by querying for docs count every second with 30 retries.
#
# @param URL of elasticsearch to check.
# @param type of elasticsearch documents
# @param number of expected documents.
ensure_elasticsearch_docs_added()
{
  require_args "${#}" 3 "ensure_elasticsearch_docs_added <es-url> <es-type> <expected-doc-count>"

  local URL="${1}"
  local TYPE="${2}"
  local EXPECTED_DOCS_COUNT="${3}"

  local attempts=0
  debug "Checking elasticsearch documents on http://${URL}"
  while [[ $attempts -le 30 ]]
  do
    attempts=$(( $attempts + 1 ))
    local docsCount=`get_elasticsearch_docs_count ${URL} ${TYPE}`
    debug "    Attempt #${attempts} received ${docsCount} expecting ${EXPECTED_DOCS_COUNT}"
    if [[ -n "${docsCount}" ]] && [[ "${docsCount}" -eq "${EXPECTED_DOCS_COUNT}" ]]
    then
      debug "${docsCount} documents present in elasticsearch"
      break
    fi
    sleep 1
  done
}

# Queries elasticsearch from the provided URL with the specified query and stores the result in the specified filename.
# @param URL of elasticsearch to check.
# @param query to send to elasticsearch.
# @param filename to store response into.
query_elasticsearch()
{
  require_args "${#}" 3 "query_elasticsearch <es-url> <query> <output-file>"
  require_non_null CURL_BIN

  local URL="${1}"
  local QUERY="${2}"
  local FILE="${3}"

  local CMD="${CURL_BIN} -XPOST ${URL} -s -d \"${QUERY}\" -o \"${FILE}\""

  debug "query_elasticsearch"

  (${DEBUG}; bash -c "${CMD}")
}

#      _             _
#  ___| |_ __ _ _ __| |_
# / __| __/ _` | '__| __|
# \__ \ || (_| | |  | |_
# |___/\__\__,_|_|   \__|
#
SCRIPT_NAME=`basename "${0}"`
parse_args "${@}"

# Register hook that performs cleanup in case this script is killed.
trap clean_up SIGHUP SIGINT SIGTERM
#trap 'echo " # $BASH_COMMAND"' DEBUG
# Buffer of files that will be automatically removed at termination.
export TMP_FILES=""

WORKING_DIR=`pwd`

cd "`dirname "${0}"`"
SCRIPT_DIR=`pwd`
DATA_DIR="${SCRIPT_DIR}/data"
RESOURCE_DIR="${SCRIPT_DIR}"

# Initializes environment.
# Initialization of default values used by script.
init_env

# Ensures docker is ready
# Sets DOCKER_CONTAINER_ID and DOCKER_CONTAINER_IP
ensure_logisland_ready
abort_if "${?}" "Unable to contact elasticsearch on ${ES_URL}. Aborting."

# Create the table used for HBase tests.
create_hbase_table "logisland_analytics"

if [[ -z "${TESTS_ARG}" ]]
then
  # If test names not provided on command line, run all tests based on directory names in /data
  TESTS_ARG=`find ${DATA_DIR}/* -maxdepth 0 -type d -exec basename {} \;|paste -d, -s`
fi

debug "TESTS_ARG: ${TESTS_ARG}"
IFS="," read -r -a TEST_NAMES <<< "${TESTS_ARG}"

# Delete logisland indices for all supported elasticsearch running servers.
if [[ -n "${ES_CLEANUP_AT_STARTUP}" ]]
then
  for ES_TAG in ${ELASTICS[@]}
  do
    URL=$(eval echo "\$${ES_TAG}_EXPOSED_9200")
    if [[ -n "${URL}" ]]
    then
      (${DEBUG}; ${CURL_BIN} -s -f -XDELETE ${URL}/logisland.* > /dev/null)
    fi
  done
fi

debug "Tests: ${TEST_NAMES[@]}"

SCRIPT_RETURN_CODE=0
# Run test for all registered tests.
for test in "${TEST_NAMES[@]}"
do
  verbose "Running ${test}"

  OUTPUT=`run_test ${test}`

  test_result=${?}

  if [[ ${test_result} -ne 0 ]]
  then
    echo -e "\e[91mTest '${test}' failed: $OUTPUT\e[0m"
    SCRIPT_RETURN_CODE=1
  else
    echo -e "\e[32mTest '${test}' succeeded.\e[0m"
  fi
done

# Done.
abort "${SCRIPT_RETURN_CODE}"
