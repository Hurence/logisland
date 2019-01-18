#!/usr/bin/env bash

# Assigns the provided variable to the specified value if the variable is undefined.
# @param the variable name to check for nullity
# @param the variable value to assign in case the variable is undefined
default_value()
{
  require_args "${#}" 2 "default_value ${1} <value>"

  local VARIABLE_NAME=\$"${1}"
  local VARIABLE_VALUE=`eval "expr \"$VARIABLE_NAME\""`

  if [[ -z "${VARIABLE_VALUE}" ]]
  then
    eval "${1}=${2}"
  fi

  debug "Default value ${1}="`eval "expr \\$${1}"`
}

# Ensures the specified file is present. Abort the script if the file is not found.
# @param the file to check
file_present()
{
  require_args "${#}" 1 "file_present <filepath>"

  test -f "$1"

  abort_if "${?}" "File '$1' was not found. Aborting."
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

# Aborts if the expected argument check failed.
# @param the number of arguments passed to the calling function
# @param the number of arguments expected by the calling function
# @param the name of the calling function
require_args()
{
  test "${1}" -ge "${2}"
  abort_if "${?}" "function '${3}' expects ${2} argument but ${1} provided. Aborting."
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

# Returns 0 if the provided topics are present in zookeeper; something else otherwise.
# @params any number of topic to lookup.
lookup_kafka_topics()
{
    declare -a lift_topic_not_found=()
    declare -a list_topic_to_find=${@}
    echo "topics to verify existence ${list_topic_to_find[*]}"
    declare -a -r list_topic=($(${KAFKA_HOME}/bin/kafka-topics.sh --list --zookeeper ${ZK_QUORUM} | awk '{print $1}'))
    echo "found topics ${list_topic[*]}"

    for topic_to_find in "${list_topic_to_find[@]}"
    do
        skip=false
        for topic in "${list_topic[@]}"
        do
            [[ ${topic_to_find} == ${topic} ]] && { skip=true; break; }
        done
        [[ -n ${skip} ]] || lift_topic_not_found+=("${topic_to_find}")
    done

    if [[ ${#lift_topic_not_found[@]} -eq 0 ]];then
        echo "all expected topic(s) exist(s)"
      return 0
    else
      echo "Some expected topic(s) does not exist: ${lift_topic_not_found[@]}"
      return 1
    fi
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
# @param topic to data

kafkacat()
{
    echo "cat ${1} | ${KAFKACAT_BIN} -P -b ${KAFKA_BROKER_URL} -t ${2}"
    cat ${1} | ${KAFKACAT_BIN} -P -b ${KAFKA_BROKER_URL} -t ${2}
}