#!/bin/bash

#. $(dirname $0)/launcher.sh

case "$(uname -s)" in
   Darwin)
     echo "I've detected that you're running Mac OS X, using greadlink instead of readlink"
     lib_dir="$(greadlink -f "$(dirname $0)/../lib")"
     CONF_DIR="$(greadlink -f "$(dirname $0)/../conf")"
     CURRENT_JAVA_EXEC="$(greadlink -f "$(which java)")"
     ;;

   *)
     lib_dir="$(readlink -f "$(dirname $0)/../lib")"
     CONF_DIR="$(readlink -f "$(dirname $0)/../conf")"
     CURRENT_JAVA_EXEC="$(readlink -f "$(which java)")"
     ;;
esac

app_mainclass="com.hurence.logisland.runner.StreamProcessingRunner"


MODE="default"
STANDALONE=false
SPARK_MASTER="local[*]"
VERBOSE_OPTIONS=""
YARN_CLUSTER_OPTIONS=""
APP_NAME=""
OPENCV_NATIVE_LIB_PATH="/usr/local/share/java/opencv4"

# update $app_classpath so that it contains all logisland jars except for engines.
# we look for jars into specified dir recursively.
# We determine which engine jar to load latter.
#
# param 1 the dir where to look for jars.
initSparkJarsOptRecursively() {
    local -r dir="${1}"
    for entry in "$dir"/*
    do
      local name=$(basename -- "${entry}")
      if [[ ! -d "$entry" ]]
      then
          echo "add jar ${name}"
          if [[ -z "$app_classpath" ]]
          then
            app_classpath="$entry"
          else
            app_classpath="$entry,$app_classpath"
          fi
      else
          if [[ ! ${name} == "engines" ]]
          then
            initSparkJarsOptRecursively "${entry}"
          fi
      fi
    done
    return 0;
}

# Create app classpath for spark standalone mode
initSparkStandaloneClassPath() {
    for entry in `ls ${1}/*.jar`
    do
      #echo "add spark standalone jar ${entry}"
      if [[ -z "$spark_standalone_classpath" ]]
        then
          spark_standalone_classpath="$entry"
        else
          spark_standalone_classpath="$entry,$spark_standalone_classpath"
        fi
    done

    echo $spark_standalone_classpath
    return 0
}

# update $java_cp so that it contains all logisland jars except for engines.
# we look for jars into specified dir recursively.
# We determine which engine jar to load latter.
#
# param 1 the dir where to look for jars.
initJavaCpOptRecursively() {
    local -r dir="${1}"
    for entry in "$dir"/*
    do
      local name=$(basename -- "${entry}")
      if [[ ! -d "$entry" ]]
      then
          echo "add jar ${name}"
          if [[ -z "${java_cp}" ]]
          then
            java_cp="$entry"
          else
            java_cp="$entry:$java_cp"
          fi
      else
          if [[ ! ${name} == "engines" ]]
          then
            initJavaCpOptRecursively "${entry}"
          fi
      fi
    done
    return 0;
}



usage() {
  echo "Usage:"
  echo
  echo " `basename $0` --conf <yml-configuration-file> [--standalone] [--yarn-cluster] [--spark-home <spark-home-directory>]"
  echo
  echo "Options:"
  echo
  echo "  --conf <yml-configuration-file> : provides the configuration file"
  echo "  --standalone start logisland in standalone mode (no spark required)"
  echo "  --spark-home : sets the SPARK_HOME (defaults to \$SPARK_HOME environment variable)"
  echo "  --spark-standalone-dir : sets the base shared directory for logisland jars for spark standlone (experimental)"
  echo "  --help : displays help"
}

# compare versions
#
# return 0 if both version are equals
#        1 if first version is superior than second one
#        2 if first version is inferior than second one
compare_versions () {
    if [[ $1 == $2 ]]
    then
        return 0
    fi
    local IFS=.
    local i ver1=($1) ver2=($2)
    # fill empty fields in ver1 with zeros
    for ((i=${#ver1[@]}; i<${#ver2[@]}; i++))
    do
        ver1[i]=0
    done
    for ((i=0; i<${#ver1[@]}; i++))
    do
        if [[ -z ${ver2[i]} ]]
        then
            # fill empty fields in ver2 with zeros
            ver2[i]=0
        fi
        if ((10#${ver1[i]} > 10#${ver2[i]}))
        then
            return 1
        fi
        if ((10#${ver1[i]} < 10#${ver2[i]}))
        then
            return 2
        fi
    done
    return 0
}

#Parse given options by users
parse_input() {
 echo "processing options : $@"
    if [[ $# -eq 0 ]]
    then
      usage
      exit 1
    fi

    while [[ $# -gt 0 ]]
    do
      KEY="$1"
        echo "processing option : $KEY"
      case $KEY in
        --conf)
          CONF_FILE="$2"
          shift
          ;;
        --standalone)
          STANDALONE=true
          ;;
        --verbose)
          VERBOSE_OPTIONS="--verbose"
          ;;
        --spark-home)
          SPARK_HOME="$2"
          shift
          ;;
        --spark-standalone-dir)
          SPARK_STANDALONE_DIR="$2"
          shift
          ;;
        --help)
          usage
          exit 0
          ;;
        *)
          echo "Unsupported option : $KEY"
          usage
          exit 1
          ;;
      esac
      shift
    done

    if [[ "${STANDALONE}" = false ]]
    then
      if [[ -z "${SPARK_HOME}" ]]
      then
        echo "Please provide the --spark-home option or set the SPARK_HOME environment variable"
        usage
        exit 1
      fi

      if [[ ! -f ${SPARK_HOME}/bin/spark-submit ]]
      then
        echo "Invalid SPARK_HOME provided"
        exit 1
      fi
    fi

    if [[ -z "${CONF_FILE}" ]]
    then
        echo "The configuration file is missing"
        usage
        exit 1
    fi
    pwd
    if [[ ! -f "${CONF_FILE}" ]]
    then
      echo "The configuration file ${CONF_FILE} does not exist"
      usage
      exit 1
    fi
}
#run logisland job with standalone engine (vanilla)
run_standalone() {
    java_cp=""
    initJavaCpOptRecursively "${lib_dir}"
    echo "java_cp is ${java_cp}"

    engine_jar=`ls ${lib_dir}/engines/logisland-engine-vanilla-*.jar`
    MIN_MEM=`awk '{ if( $1 == "jvm.heap.min:" ){ gsub(/[ \t]+$/, "", $2);print $2 } }' ${CONF_FILE}`
    MAX_MEM=`awk '{ if( $1 == "jvm.heap.max:" ){ gsub(/[ \t]+$/, "", $2);print $2 } }' ${CONF_FILE}`

    JAVA_OPTS="${JAVA_OPTS} -Dlog4j.configuration=file:${CONF_DIR}/log4j.properties"

    if [[ ! -z "${MIN_MEM}" ]]
    then
        JAVA_OPTS="${JAVA_OPTS} -Xms${MIN_MEM}"
    fi
    if [[ ! -z "${MAX_MEM}" ]]
    then
        JAVA_OPTS="${JAVA_OPTS} -Xmx${MAX_MEM}"
    fi

    if [[ ! -z "${VERBOSE_OPTIONS}" ]]
    then
        echo "Using plain java engine with options: ${JAVA_OPTS}"
    fi

    if [[ ! -z "${JAVA_HOME}" ]]
    then
        CURRENT_JAVA_EXEC="${JAVA_HOME}/bin/java"
    fi

    ${CURRENT_JAVA_EXEC} ${JAVA_OPTS} -cp ${java_cp}:${engine_jar} \
        com.hurence.logisland.runner.StreamProcessingRunner \
        -conf ${CONF_FILE}
}


main() {
    parse_input $@

    # ----------------------------------------------------------------
    # find the spark-submit mode
    # can be either local, standalone, spark (standalone), mesos or yarn
    # ----------------------------------------------------------------
    if [[ "$STANDALONE" = true ]] ;
    then
      run_standalone
    else
        echo "build classpath"
        app_classpath=""
        initSparkJarsOptRecursively "${lib_dir}"
        echo "app_classpath is ${app_classpath}"

        # Find version to use for spark
        SPARK_VERSION=`${SPARK_HOME}/bin/spark-submit --version 2>&1 >/dev/null | grep -m 1 -o '[0-9]*\.[0-9]*\.[0-9]*'`
        engine_jar=""

        compare_versions ${SPARK_VERSION} 2.0.0
            case $? in
                2) engine_jar=`ls ${lib_dir}/engines/logisland-engine-spark_1_6-*.jar` ;;
                *) compare_versions ${SPARK_VERSION} 2.3.0
                    case $? in
                        2) engine_jar=`ls ${lib_dir}/engines/logisland-engine-spark_2_1-*.jar` ;;
                        0) engine_jar=`ls ${lib_dir}/engines/logisland-engine-spark_2_3-*.jar` ;;
                        *) engine_jar=`ls ${lib_dir}/engines/logisland-engine-spark_2_4-*.jar` ;;
                    esac
                   ;;
            esac

        export SPARK_PRINT_LAUNCH_COMMAND=1
        echo "Detected spark version ${SPARK_VERSION}. We'll automatically plug in engine jar ${engine_jar}"
        APP_NAME=`awk '{ if( $1 == "spark.app.name:" ){ print $2 } }' ${CONF_FILE}`

        #
        # YARN mode?
        #
        MODE=`awk '{ if( $1 == "spark.master:" ){ print $2 } }' ${CONF_FILE}`
        case ${MODE} in
          "yarn")
            SPARK_MASTER=${MODE}
            EXTRA_MODE=`awk '{ if( $1 == "spark.yarn.deploy-mode:" ){ print $2 } }' ${CONF_FILE}`
            if [[ -z "${EXTRA_MODE}" ]]
            then
             echo "The property \"spark.yarn.deploy-mode\" is missing in config file \"${CONF_FILE}\""
             exit 1
            fi

            if [[ ! ${EXTRA_MODE} = "cluster" && ! ${EXTRA_MODE} = "client" ]]
            then
              echo "The property \"spark.yarn.deploy-mode\" value \"${EXTRA_MODE}\" is not supported"
              exit 1
            else
              MODE=${MODE}-${EXTRA_MODE}
            fi
            ;;

        esac

        #
        # MESOS mode?
        #
        if [[ "${MODE}" =~ "mesos" ]]
        then
            SPARK_MASTER=${MODE}
            MODE="mesos"
        fi

        #
        # Spark standalone mode?
        #
        if [[ "${MODE}" =~ ^spark://.* ]] # Starts with spark:// (spark standalone url)
        then
            if [[ -z "${SPARK_STANDALONE_DIR}" ]]
            then
             echo "Spark standalone mode requires --spark-standalone-dir option to be set"
             exit 1
            fi

            SPARK_MASTER=${MODE}
            EXTRA_MODE=`awk '{ if( $1 == "spark.deploy-mode:" ){ print $2 } }' ${CONF_FILE}`
            if [[ -z "${EXTRA_MODE}" ]]
            then
             echo "The property \"spark.deploy-mode\" is missing in config file \"${CONF_FILE}\""
             exit 1
            fi

            if [[ ! ${EXTRA_MODE} = "cluster" && ! ${EXTRA_MODE} = "client" ]]
            then
              echo "The property \"spark.deploy-mode\" value \"${EXTRA_MODE}\" is not supported"
              exit 1
            else
              MODE=spark-${EXTRA_MODE}
            fi
        fi

        if [[ ! -z "${VERBOSE_OPTIONS}" ]]
        then
          echo "Starting with run mode \"${MODE}\" on master \"${SPARK_MASTER}\""
        fi

        case ${MODE} in
          local*)

            ${SPARK_HOME}/bin/spark-submit ${VERBOSE_OPTIONS} ${YARN_CLUSTER_OPTIONS} \
            --driver-library-path ${OPENCV_NATIVE_LIB_PATH} \
            --conf spark.driver.extraJavaOptions="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 -Dlog4j.configuration=\"file:${lib_dir}/../conf/log4j.properties\"" \
            --conf spark.executor.extraJavaOptions="-Dlog4j.configuration=\"file:${lib_dir}/../conf/log4j.properties\"" \
            --conf spark.metrics.namespace="${APP_NAME}"  \
            --conf spark.metrics.conf="${lib_dir}/../monitoring/metrics.properties"  \
            --class ${app_mainclass} \
            --jars ${app_classpath} ${engine_jar} \
            -conf ${CONF_FILE}

            ;;
          yarn-cluster)

            YARN_CLUSTER_OPTIONS="--master yarn --deploy-mode cluster --files ${CONF_FILE}#logisland-configuration.yml,${CONF_DIR}/../monitoring/jmx_prometheus_javaagent-0.10.jar#jmx_prometheus_javaagent-0.10.jar,${CONF_DIR}/../monitoring/spark-prometheus.yml#spark-prometheus.yml,${CONF_DIR}/../monitoring/metrics.properties#metrics.properties,${CONF_DIR}/log4j.properties#log4j.properties --conf spark.metrics.namespace=\"${APP_NAME}\" --conf spark.ui.showConsoleProgress=false"

            if [[ ! -z "${YARN_APP_NAME}" ]]
            then
              YARN_CLUSTER_OPTIONS="${YARN_CLUSTER_OPTIONS} --name ${YARN_APP_NAME}"
            else
              YARN_APP_NAME=`awk '{ if( $1 == "spark.app.name:" ){ print $2 } }' ${CONF_FILE}`
              if [[ ! -z "${YARN_APP_NAME}" ]]
              then
                YARN_CLUSTER_OPTIONS="${YARN_CLUSTER_OPTIONS} --name ${YARN_APP_NAME}"
              fi
            fi

            SPARK_YARN_QUEUE=`awk '{ if( $1 == "spark.yarn.queue:" ){ print $2 } }' ${CONF_FILE}`
            if [[ ! -z "${SPARK_YARN_QUEUE}" ]]
            then
             YARN_CLUSTER_OPTIONS="${YARN_CLUSTER_OPTIONS} --queue ${SPARK_YARN_QUEUE}"
            fi

            DRIVER_CORES=`awk '{ if( $1 == "spark.driver.cores:" ){ print $2 } }' ${CONF_FILE}`
            if [[ ! -z "${DRIVER_CORES}" ]]
            then
             YARN_CLUSTER_OPTIONS="${YARN_CLUSTER_OPTIONS} --driver-cores ${DRIVER_CORES}"
            fi

            DRIVER_MEMORY=`awk '{ if( $1 == "spark.driver.memory:" ){ print $2 } }' ${CONF_FILE}`
            if [[ ! -z "${DRIVER_MEMORY}" ]]
            then
             YARN_CLUSTER_OPTIONS="${YARN_CLUSTER_OPTIONS} --driver-memory ${DRIVER_MEMORY}"
            fi

            EXECUTORS_CORES=`awk '{ if( $1 == "spark.executor.cores:" ){ print $2 } }' ${CONF_FILE}`
            if [[ ! -z "${EXECUTORS_CORES}" ]]
            then
                 YARN_CLUSTER_OPTIONS="${YARN_CLUSTER_OPTIONS} --executor-cores ${EXECUTORS_CORES}"
            fi

            EXECUTORS_MEMORY=`awk '{ if( $1 == "spark.executor.memory:" ){ print $2 } }' ${CONF_FILE}`
            if [[ ! -z "${EXECUTORS_MEMORY}" ]]
            then
                 YARN_CLUSTER_OPTIONS="${YARN_CLUSTER_OPTIONS} --executor-memory ${EXECUTORS_MEMORY}"
            fi

            EXECUTORS_INSTANCES=`awk '{ if( $1 == "spark.executor.instances:" ){ print $2 } }' ${CONF_FILE}`
            if [[ ! -z "${EXECUTORS_INSTANCES}" ]]
            then
                 YARN_CLUSTER_OPTIONS="${YARN_CLUSTER_OPTIONS} --num-executors ${EXECUTORS_INSTANCES}"
            fi

            SPARK_YARN_MAX_APP_ATTEMPTS=`awk '{ if( $1 == "spark.yarn.maxAppAttempts:" ){ print $2 } }' ${CONF_FILE}`
            if [[ ! -z "${SPARK_YARN_MAX_APP_ATTEMPTS}" ]]
            then
                 YARN_CLUSTER_OPTIONS="${YARN_CLUSTER_OPTIONS} --conf spark.yarn.maxAppAttempts=${SPARK_YARN_MAX_APP_ATTEMPTS}"
            fi

            SPARK_YARN_AM_ATTEMPT_FAILURES_VALIDITY_INTERVAL=`awk '{ if( $1 == "spark.yarn.am.attemptFailuresValidityInterval:" ){ print $2 } }' ${CONF_FILE}`
            if [[ ! -z "${SPARK_YARN_AM_ATTEMPT_FAILURES_VALIDITY_INTERVAL}" ]]
            then
                 YARN_CLUSTER_OPTIONS="${YARN_CLUSTER_OPTIONS} --conf spark.yarn.am.attemptFailuresValidityInterval=${SPARK_YARN_AM_ATTEMPT_FAILURES_VALIDITY_INTERVAL}"
            fi

            SPARK_YARN_MAX_EXECUTOR_FAILURES=`awk '{ if( $1 == "spark.yarn.max.executor.failures:" ){ print $2 } }' ${CONF_FILE}`
            if [[ ! -z "${SPARK_YARN_MAX_EXECUTOR_FAILURES}" ]]
            then
                 YARN_CLUSTER_OPTIONS="${YARN_CLUSTER_OPTIONS} --conf spark.yarn.max.executor.failures=${SPARK_YARN_MAX_EXECUTOR_FAILURES}"
            fi

            SPARK_YARN_EXECUTOR_FAILURES_VALIDITY_INTERVAL=`awk '{ if( $1 == "spark.yarn.executor.failuresValidityInterval:" ){ print $2 } }' ${CONF_FILE}`
            if [[ ! -z "${SPARK_YARN_EXECUTOR_FAILURES_VALIDITY_INTERVAL}" ]]
            then
                 YARN_CLUSTER_OPTIONS="${YARN_CLUSTER_OPTIONS} --conf spark.yarn.executor.failuresValidityInterval=${SPARK_YARN_EXECUTOR_FAILURES_VALIDITY_INTERVAL}"
            fi

            SPARK_TASK_MAX_FAILURES=`awk '{ if( $1 == "spark.task.maxFailures:" ){ print $2 } }' ${CONF_FILE}`
            if [[ ! -z "${SPARK_TASK_MAX_FAILURES}" ]]
            then
                 YARN_CLUSTER_OPTIONS="${YARN_CLUSTER_OPTIONS} --conf spark.task.maxFailures=${SPARK_TASK_MAX_FAILURES}"
            fi

            PROPERTIES_FILE_PATH=`awk '{ if( $1 == "spark.properties.file.path:" ){ print $2 } }' ${CONF_FILE}`
            if [[ ! -z "${PROPERTIES_FILE_PATH}" ]]
            then
                 YARN_CLUSTER_OPTIONS="${YARN_CLUSTER_OPTIONS} --properties-file ${PROPERTIES_FILE_PATH}"
            fi


            SPARK_MONITORING_DRIVER_PORT=`awk '{ if( $1 == "spark.monitoring.driver.port:" ){ print $2 } }' ${CONF_FILE}`
            if [[ -z "${SPARK_MONITORING_DRIVER_PORT}" ]]
            then
                 EXTRA_DRIVER_JAVA_OPTIONS='spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j.properties'
                 EXTRA_PROCESSOR_JAVA_OPTIONS='spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j.properties'
            else
                 EXTRA_DRIVER_JAVA_OPTIONS='spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j.properties -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=0 -Dcom.sun.management.jmxremote.rmi.port=0 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -javaagent:./jmx_prometheus_javaagent-0.10.jar='${SPARK_MONITORING_DRIVER_PORT}':./spark-prometheus.yml'
                 EXTRA_PROCESSOR_JAVA_OPTIONS='spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j.properties -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=0 -Dcom.sun.management.jmxremote.rmi.port=0 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -javaagent:./jmx_prometheus_javaagent-0.10.jar='${SPARK_MONITORING_DRIVER_PORT}':./spark-prometheus.yml'
            fi

            CONF_FILE="logisland-configuration.yml"

            ${SPARK_HOME}/bin/spark-submit ${VERBOSE_OPTIONS} ${YARN_CLUSTER_OPTIONS} \
            --conf "${EXTRA_DRIVER_JAVA_OPTIONS}" \
            --conf "${EXTRA_PROCESSOR_JAVA_OPTIONS}" \
            --class ${app_mainclass} \
            --jars ${app_classpath} ${engine_jar} \
            -conf ${CONF_FILE}

            ;;
          yarn-client)

            YARN_CLUSTER_OPTIONS="--master yarn --deploy-mode client --conf spark.metrics.namespace=\"${APP_NAME}\""

            DRIVER_CORES=`awk '{ if( $1 == "spark.driver.cores:" ){ print $2 } }' ${CONF_FILE}`
            if [[ ! -z "${DRIVER_CORES}" ]]
            then
             YARN_CLUSTER_OPTIONS="${YARN_CLUSTER_OPTIONS} --driver-cores ${DRIVER_CORES}"
            fi

            DRIVER_MEMORY=`awk '{ if( $1 == "spark.driver.memory:" ){ print $2 } }' ${CONF_FILE}`
            if [[ ! -z "${DRIVER_MEMORY}" ]]
            then
             YARN_CLUSTER_OPTIONS="${YARN_CLUSTER_OPTIONS} --driver-memory ${DRIVER_MEMORY}"
            fi

            PROPERTIES_FILE_PATH=`awk '{ if( $1 == "spark.properties.file.path:" ){ print $2 } }' ${CONF_FILE}`
            if [[ ! -z "${PROPERTIES_FILE_PATH}" ]]
            then
                 YARN_CLUSTER_OPTIONS="${YARN_CLUSTER_OPTIONS} --properties-file ${PROPERTIES_FILE_PATH}"
            fi

            ${SPARK_HOME}/bin/spark-submit ${VERBOSE_OPTIONS} ${YARN_CLUSTER_OPTIONS} \
            --conf 'spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j.properties -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=0 -Dcom.sun.management.jmxremote.rmi.port=0 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -javaagent:./jmx_prometheus_javaagent-0.10.jar='${SPARK_MONITORING_DRIVER_PORT}':./spark-prometheus.yml' \
            --conf spark.metrics.conf=./metrics.properties \
            --class ${app_mainclass} \
            --jars ${app_classpath} ${engine_jar} \
            -conf ${CONF_FILE}
            ;;

          mesos)

            MESOS_OPTIONS="--master ${SPARK_MASTER} --conf spark.metrics.namespace=\"${APP_NAME}\""

            DRIVER_CORES=`awk '{ if( $1 == "spark.driver.cores:" ){ print $2 } }' ${CONF_FILE}`
            if [[ ! -z "${DRIVER_CORES}" ]]
            then
             MESOS_OPTIONS="${MESOS_OPTIONS} --driver-cores ${DRIVER_CORES}"
            fi

            DRIVER_MEMORY=`awk '{ if( $1 == "spark.driver.memory:" ){ print $2 } }' ${CONF_FILE}`
            if [[ ! -z "${DRIVER_MEMORY}" ]]
            then
             MESOS_OPTIONS="${MESOS_OPTIONS} --driver-memory ${DRIVER_MEMORY}"
            fi

            EXECUTORS_CORES=`awk '{ if( $1 == "spark.executor.cores:" ){ print $2 } }' ${CONF_FILE}`
            if [[ ! -z "${EXECUTORS_CORES}" ]]
            then
                 MESOS_OPTIONS="${MESOS_OPTIONS} --executor-cores ${EXECUTORS_CORES}"
            fi

            EXECUTORS_MEMORY=`awk '{ if( $1 == "spark.executor.memory:" ){ print $2 } }' ${CONF_FILE}`
            if [[ ! -z "${EXECUTORS_MEMORY}" ]]
            then
                 MESOS_OPTIONS="${MESOS_OPTIONS} --executor-memory ${EXECUTORS_MEMORY}"
            fi

            EXECUTORS_INSTANCES=`awk '{ if( $1 == "spark.executor.instances:" ){ print $2 } }' ${CONF_FILE}`
            if [[ ! -z "${EXECUTORS_INSTANCES}" ]]
            then
                 MESOS_OPTIONS="${MESOS_OPTIONS} --num-executors ${EXECUTORS_INSTANCES}"
            fi


            TOTAL_EXECUTOR_CORES=`awk '{ if( $1 == "spark.cores.max:" ){ print $2 } }' ${CONF_FILE}`
            if [[ ! -z "${TOTAL_EXECUTOR_CORES}" ]]
            then
                 MESOS_OPTIONS="${MESOS_OPTIONS} --total-executor-cores ${TOTAL_EXECUTOR_CORES}"
            fi

            MESOS_NATIVE_JAVA_LIBRARY=`awk '{ if( $1 == "java.library.path:" ){ print $2 } }' ${CONF_FILE}`

            #--deploy-mode cluster \
            #--supervise \
            #--executor-memory 20G \
            # --total-executor-cores 100 \

            export MESOS_NATIVE_JAVA_LIBRARY="${MESOS_NATIVE_JAVA_LIBRARY}"
            ${SPARK_HOME}/bin/spark-submit ${VERBOSE_OPTIONS} ${MESOS_OPTIONS} \
            --driver-library-path ${OPENCV_NATIVE_LIB_PATH} \
            --class ${app_mainclass} \
            --jars ${app_classpath} ${engine_jar} \
            -conf ${CONF_FILE}
            ;;

          spark-cluster)
            SPARK_CLUSTER_OPTIONS="--master ${SPARK_MASTER} --deploy-mode cluster --files ${CONF_FILE}#logisland-configuration.yml,${CONF_DIR}/../monitoring/jmx_prometheus_javaagent-0.10.jar#jmx_prometheus_javaagent-0.10.jar,${CONF_DIR}/../monitoring/spark-prometheus.yml#spark-prometheus.yml,${CONF_DIR}/../monitoring/metrics.properties#metrics.properties,${CONF_DIR}/log4j.properties#log4j.properties --conf spark.metrics.namespace=\"${APP_NAME}\" --conf spark.ui.showConsoleProgress=false"

            if [[ ! -z "${SPARK_APP_NAME}" ]]
            then
              SPARK_CLUSTER_OPTIONS="${SPARK_CLUSTER_OPTIONS} --name ${SPARK_APP_NAME}"
            else
              SPARK_APP_NAME=`awk '{ if( $1 == "spark.app.name:" ){ print $2 } }' ${CONF_FILE}`
              if [[ ! -z "${SPARK_APP_NAME}" ]]
              then
                SPARK_CLUSTER_OPTIONS="${SPARK_CLUSTER_OPTIONS} --name ${SPARK_APP_NAME}"
              fi
            fi

            DRIVER_CORES=`awk '{ if( $1 == "spark.driver.cores:" ){ print $2 } }' ${CONF_FILE}`
            if [[ ! -z "${DRIVER_CORES}" ]]
            then
             SPARK_CLUSTER_OPTIONS="${SPARK_CLUSTER_OPTIONS} --driver-cores ${DRIVER_CORES}"
            fi

            DRIVER_MEMORY=`awk '{ if( $1 == "spark.driver.memory:" ){ print $2 } }' ${CONF_FILE}`
            if [[ ! -z "${DRIVER_MEMORY}" ]]
            then
             SPARK_CLUSTER_OPTIONS="${SPARK_CLUSTER_OPTIONS} --driver-memory ${DRIVER_MEMORY}"
            fi

            EXECUTORS_CORES=`awk '{ if( $1 == "spark.executor.cores:" ){ print $2 } }' ${CONF_FILE}`
            if [[ ! -z "${EXECUTORS_CORES}" ]]
            then
                 SPARK_CLUSTER_OPTIONS="${SPARK_CLUSTER_OPTIONS} --executor-cores ${EXECUTORS_CORES}"
            fi

            EXECUTORS_MEMORY=`awk '{ if( $1 == "spark.executor.memory:" ){ print $2 } }' ${CONF_FILE}`
            if [[ ! -z "${EXECUTORS_MEMORY}" ]]
            then
                 SPARK_CLUSTER_OPTIONS="${SPARK_CLUSTER_OPTIONS} --executor-memory ${EXECUTORS_MEMORY}"
            fi

            EXECUTORS_INSTANCES=`awk '{ if( $1 == "spark.executor.instances:" ){ print $2 } }' ${CONF_FILE}`
            if [[ ! -z "${EXECUTORS_INSTANCES}" ]]
            then
                 SPARK_CLUSTER_OPTIONS="${SPARK_CLUSTER_OPTIONS} --num-executors ${EXECUTORS_INSTANCES}"
            fi

            SPARK_TASK_MAX_FAILURES=`awk '{ if( $1 == "spark.task.maxFailures:" ){ print $2 } }' ${CONF_FILE}`
            if [[ ! -z "${SPARK_TASK_MAX_FAILURES}" ]]
            then
                 SPARK_CLUSTER_OPTIONS="${SPARK_CLUSTER_OPTIONS} --conf spark.task.maxFailures=${SPARK_TASK_MAX_FAILURES}"
            fi

            PROPERTIES_FILE_PATH=`awk '{ if( $1 == "spark.properties.file.path:" ){ print $2 } }' ${CONF_FILE}`
            if [[ ! -z "${PROPERTIES_FILE_PATH}" ]]
            then
                SPARK_CLUSTER_OPTIONS="${SPARK_CLUSTER_OPTIONS} --properties-file ${PROPERTIES_FILE_PATH}"
            fi

            SPARK_MONITORING_DRIVER_PORT=`awk '{ if( $1 == "spark.monitoring.driver.port:" ){ print $2 } }' ${CONF_FILE}`
            if [[ -z "${SPARK_MONITORING_DRIVER_PORT}" ]]
            then
                 EXTRA_DRIVER_JAVA_OPTIONS='spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j.properties'
                 EXTRA_PROCESSOR_JAVA_OPTIONS='spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j.properties'
            else
                 EXTRA_DRIVER_JAVA_OPTIONS='spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j.properties -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=0 -Dcom.sun.management.jmxremote.rmi.port=0 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -javaagent:./jmx_prometheus_javaagent-0.10.jar='${SPARK_MONITORING_DRIVER_PORT}':./spark-prometheus.yml'
                 EXTRA_PROCESSOR_JAVA_OPTIONS='spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j.properties -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=0 -Dcom.sun.management.jmxremote.rmi.port=0 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -javaagent:./jmx_prometheus_javaagent-0.10.jar='${SPARK_MONITORING_DRIVER_PORT}':./spark-prometheus.yml'
            fi

            CONF_FILE="logisland-configuration.yml"

            engine_jar=(basename $engine_jar)

            ${SPARK_HOME}/bin/spark-submit ${VERBOSE_OPTIONS} ${SPARK_CLUSTER_OPTIONS} \
            --conf "${EXTRA_DRIVER_JAVA_OPTIONS}" \
            --conf "${EXTRA_PROCESSOR_JAVA_OPTIONS}" \
            --class ${app_mainclass} \
            --jars ${SPARK_STANDALONE_DIR}/* ${SPARK_STANDALONE_DIR}/${engine_jar} \
            -conf ${CONF_FILE}
            ;;

          spark-client)
            SPARK_CLUSTER_OPTIONS="--master ${SPARK_MASTER} --deploy-mode client --conf spark.metrics.namespace=\"${APP_NAME}\""

            if [[ ! -z "${SPARK_APP_NAME}" ]]
            then
              SPARK_CLUSTER_OPTIONS="${SPARK_CLUSTER_OPTIONS} --name ${SPARK_APP_NAME}"
            else
              SPARK_APP_NAME=`awk '{ if( $1 == "spark.app.name:" ){ print $2 } }' ${CONF_FILE}`
              if [[ ! -z "${SPARK_APP_NAME}" ]]
              then
                SPARK_CLUSTER_OPTIONS="${SPARK_CLUSTER_OPTIONS} --name ${SPARK_APP_NAME}"
              fi
            fi

            DRIVER_CORES=`awk '{ if( $1 == "spark.driver.cores:" ){ print $2 } }' ${CONF_FILE}`
            if [[ ! -z "${DRIVER_CORES}" ]]
            then
             SPARK_CLUSTER_OPTIONS="${SPARK_CLUSTER_OPTIONS} --driver-cores ${DRIVER_CORES}"
            fi

            DRIVER_MEMORY=`awk '{ if( $1 == "spark.driver.memory:" ){ print $2 } }' ${CONF_FILE}`
            if [[ ! -z "${DRIVER_MEMORY}" ]]
            then
             SPARK_CLUSTER_OPTIONS="${SPARK_CLUSTER_OPTIONS} --driver-memory ${DRIVER_MEMORY}"
            fi

            PROPERTIES_FILE_PATH=`awk '{ if( $1 == "spark.properties.file.path:" ){ print $2 } }' ${CONF_FILE}`
            if [[ ! -z "${PROPERTIES_FILE_PATH}" ]]
            then
                 SPARK_CLUSTER_OPTIONS="${SPARK_CLUSTER_OPTIONS} --properties-file ${PROPERTIES_FILE_PATH}"
            fi

            EXECUTORS_INSTANCES=`awk '{ if( $1 == "spark.executor.instances:" ){ print $2 } }' ${CONF_FILE}`
            if [[ ! -z "${EXECUTORS_INSTANCES}" ]]
            then
                 SPARK_CLUSTER_OPTIONS="${SPARK_CLUSTER_OPTIONS} --num-executors ${EXECUTORS_INSTANCES}"
            fi

            EXTRA_DRIVER_JAVA_OPTIONS='spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j.properties'
            EXTRA_PROCESSOR_JAVA_OPTIONS='spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j.properties'

            engine_jar=`basename $engine_jar`

            spark_standalone_classpath=`initSparkStandaloneClassPath ${SPARK_STANDALONE_DIR}`

            ${SPARK_HOME}/bin/spark-submit ${VERBOSE_OPTIONS} ${SPARK_CLUSTER_OPTIONS} \
            --conf spark.metrics.conf="${CONF_DIR}/../monitoring/metrics.properties"  \
            --conf "${EXTRA_DRIVER_JAVA_OPTIONS}" \
            --conf "${EXTRA_PROCESSOR_JAVA_OPTIONS}" \
            --class ${app_mainclass} \
            --jars ${spark_standalone_classpath} ${SPARK_STANDALONE_DIR}/${engine_jar} \
            -conf ${CONF_FILE}
            ;;

          *)
            echo "Unsupported run mode: ${MODE}"
            ;;

          esac
    fi
}

main $@
