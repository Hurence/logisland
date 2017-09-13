#!/bin/bash

#. $(dirname $0)/launcher.sh

case "$(uname -s)" in

   Darwin)
     echo "I've detected that you're running Mac OS X, using greadlink instead of readlink"
     lib_dir="$(greadlink -f "$(dirname $0)/../lib")"
     CONF_DIR="$(greadlink -f "$(dirname $0)/../conf")"
     ;;

   *)
     lib_dir="$(readlink -f "$(dirname $0)/../lib")"
     CONF_DIR="$(readlink -f "$(dirname $0)/../conf")"
     ;;
esac



app_classpath=""
for entry in "$lib_dir"/*
do
  if [ -z "$app_classpath" ]
  then
    app_classpath="$lib$entry"
  else
    app_classpath="$lib$entry,$app_classpath"
  fi
done



app_mainclass="com.hurence.logisland.runner.StreamProcessingRunner"


MODE="default"
VERBOSE_OPTIONS=""
YARN_CLUSTER_OPTIONS=""

usage() {
  echo "Usage:"
  echo
  echo " `basename $0` --conf <yml-configuguration-file> [--yarn-cluster] [--spark-home <spark-home-directory>]"
  echo
  echo "Options:"
  echo
  echo "  --conf <yml-configuguration-file> : provides the configuration file"
  echo "  --spark-home : sets the SPARK_HOME (defaults to \$SPARK_HOME environment variable)"
  echo "  --help : displays help"
}

if [ $# -eq 0 ]
then
  usage
  exit 1
fi

while [ $# -gt 0 ]
do
  KEY="$1"

  case $KEY in
    --conf)
      CONF_FILE="$2"
      shift
      ;;
    --verbose)
      VERBOSE_OPTIONS="--verbose"
      ;;
    --spark-home)
      SPARK_HOME="$2"
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

if [ -z "${SPARK_HOME}" ]
then
  echo "Please provide the --spark-home option or set the SPARK_HOME environment variable"
  usage
  exit 1
fi

if [ ! -f ${SPARK_HOME}/bin/spark-submit ]
then
  echo "Invalid SPARK_HOME provided"
  exit 1
fi

if [ -z "${CONF_FILE}" ]
then
    echo "The configuration file is missing"
    usage
    exit 1
fi

if [ ! -f "${CONF_FILE}" ]
then
  echo "The configuration file ${CONF_FILE} does not exist"
  usage
  exit 1
fi


MODE=`awk '{ if( $1 == "spark.master:" ){ print $2 } }' ${CONF_FILE}`
case ${MODE} in
  "yarn")
    EXTRA_MODE=`awk '{ if( $1 == "spark.yarn.deploy-mode:" ){ print $2 } }' ${CONF_FILE}`
    if [ -z "${EXTRA_MODE}" ]
    then
     echo "The property \"spark.yarn.deploy-mode\" is missing in config file \"${CONF_FILE}\""
     exit 1
    fi

    if [ ! ${EXTRA_MODE} = "cluster" -a ! ${EXTRA_MODE} = "client" ]
    then
      echo "The property \"spark.yarn.deploy-mode\" value \"${EXTRA_MODE}\" is not supported"
      exit 1
    else
      MODE=${MODE}-${EXTRA_MODE}
    fi
    ;;
esac

if [ ! -z "${VERBOSE_OPTIONS}" ]
then
  echo "Starting with mode \"${MODE}\""
fi

case $MODE in
  default)
    app_classpath=`echo ${app_classpath} | sed 's#,/[^,]*/logisland-elasticsearch-plugin-[^,]*.jar,#,#'`
    ;;
  yarn-cluster)
    app_classpath=`echo ${app_classpath} | sed 's#,/[^,]*/logisland-spark_[^,-]*-engine[^,]*.jar,#,#'`
    app_classpath=`echo ${app_classpath} | sed 's#,/[^,]*/guava-[^,]*.jar,#,#'`
    app_classpath=`echo ${app_classpath} | sed 's#,/[^,]*/elasticsearch-[^,]*.jar,#,#'`


    YARN_CLUSTER_OPTIONS="--master yarn --deploy-mode cluster --files ${CONF_FILE}#logisland-configuration.yml,/opt/jmx/jmx_prometheus_javaagent-0.9.jar#jmx_prometheus_javaagent-0.9.jar,/opt/jmx/spark_prometheus.yml#spark_prometheus.yml,${SPARK_HOME}/conf/metrics.properties#metrics.properties,${CONF_DIR}/log4j.properties#log4j.properties  --conf spark.metrics.conf=./metrics.properties --conf \"spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j.properties\" --conf \"spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j.properties\" --conf spark.ui.showConsoleProgress=false"


    if [ ! -z "$YARN_APP_NAME" ]
    then
      YARN_CLUSTER_OPTIONS="${YARN_CLUSTER_OPTIONS} --name ${YARN_APP_NAME}"
    else
      YARN_APP_NAME=`awk '{ if( $1 == "spark.app.name:" ){ print $2 } }' ${CONF_FILE}`
      if [ ! -z "${YARN_APP_NAME}" ]
      then
        YARN_CLUSTER_OPTIONS="${YARN_CLUSTER_OPTIONS} --name ${YARN_APP_NAME}"
      fi
    fi

    SPARK_YARN_QUEUE=`awk '{ if( $1 == "spark.yarn.queue:" ){ print $2 } }' ${CONF_FILE}`
    if [ ! -z "${SPARK_YARN_QUEUE}" ]
    then
 	 YARN_CLUSTER_OPTIONS="${YARN_CLUSTER_OPTIONS} --queue ${SPARK_YARN_QUEUE}"
    fi

    DRIVER_CORES=`awk '{ if( $1 == "spark.driver.cores:" ){ print $2 } }' ${CONF_FILE}`
    if [ ! -z "${DRIVER_CORES}" ]
    then
 	 YARN_CLUSTER_OPTIONS="${YARN_CLUSTER_OPTIONS} --driver-cores ${DRIVER_CORES}" 
    fi

    DRIVER_MEMORY=`awk '{ if( $1 == "spark.driver.memory:" ){ print $2 } }' ${CONF_FILE}`
    if [ ! -z "${DRIVER_MEMORY}" ]
    then
 	 YARN_CLUSTER_OPTIONS="${YARN_CLUSTER_OPTIONS} --driver-memory ${DRIVER_MEMORY}" 
    fi

    EXECUTORS_CORES=`awk '{ if( $1 == "spark.executor.cores:" ){ print $2 } }' ${CONF_FILE}`
    if [ ! -z "${EXECUTORS_CORES}" ]
    then
         YARN_CLUSTER_OPTIONS="${YARN_CLUSTER_OPTIONS} --executor-cores ${EXECUTORS_CORES}" 
    fi

    EXECUTORS_MEMORY=`awk '{ if( $1 == "spark.executor.memory:" ){ print $2 } }' ${CONF_FILE}`
    if [ ! -z "${EXECUTORS_MEMORY}" ]
    then
         YARN_CLUSTER_OPTIONS="${YARN_CLUSTER_OPTIONS} --executor-memory ${EXECUTORS_MEMORY}" 
    fi

    EXECUTORS_INSTANCES=`awk '{ if( $1 == "spark.executor.instances:" ){ print $2 } }' ${CONF_FILE}`
    if [ ! -z "${EXECUTORS_INSTANCES}" ]
    then
         YARN_CLUSTER_OPTIONS="${YARN_CLUSTER_OPTIONS} --num-executors ${EXECUTORS_INSTANCES}" 
    fi

    SPARK_YARN_MAX_APP_ATTEMPTS=`awk '{ if( $1 == "spark.yarn.maxAppAttempts:" ){ print $2 } }' ${CONF_FILE}`
    if [ ! -z "${SPARK_YARN_MAX_APP_ATTEMPTS}" ]
    then
         YARN_CLUSTER_OPTIONS="${YARN_CLUSTER_OPTIONS} --conf spark.yarn.maxAppAttempts=${SPARK_YARN_MAX_APP_ATTEMPTS}"
    fi

    SPARK_YARN_AM_ATTEMPT_FAILURES_VALIDITY_INTERVAL=`awk '{ if( $1 == "spark.yarn.am.attemptFailuresValidityInterval:" ){ print $2 } }' ${CONF_FILE}`
    if [ ! -z "${SPARK_YARN_AM_ATTEMPT_FAILURES_VALIDITY_INTERVAL}" ]
    then
         YARN_CLUSTER_OPTIONS="${YARN_CLUSTER_OPTIONS} --conf spark.yarn.am.attemptFailuresValidityInterval=${SPARK_YARN_AM_ATTEMPT_FAILURES_VALIDITY_INTERVAL}"
    fi

    SPARK_YARN_MAX_EXECUTOR_FAILURES=`awk '{ if( $1 == "spark.yarn.max.executor.failures:" ){ print $2 } }' ${CONF_FILE}`
    if [ ! -z "${SPARK_YARN_MAX_EXECUTOR_FAILURES}" ]
    then
         YARN_CLUSTER_OPTIONS="${YARN_CLUSTER_OPTIONS} --conf spark.yarn.max.executor.failures=${SPARK_YARN_MAX_EXECUTOR_FAILURES}"
    fi

    SPARK_YARN_EXECUTOR_FAILURES_VALIDITY_INTERVAL=`awk '{ if( $1 == "spark.yarn.executor.failuresValidityInterval:" ){ print $2 } }' ${CONF_FILE}`
    if [ ! -z "${SPARK_YARN_EXECUTOR_FAILURES_VALIDITY_INTERVAL}" ]
    then
         YARN_CLUSTER_OPTIONS="${YARN_CLUSTER_OPTIONS} --conf spark.yarn.executor.failuresValidityInterval=${SPARK_YARN_EXECUTOR_FAILURES_VALIDITY_INTERVAL}"
    fi

    SPARK_TASK_MAX_FAILURES=`awk '{ if( $1 == "spark.task.maxFailures:" ){ print $2 } }' ${CONF_FILE}`
    if [ ! -z "${SPARK_TASK_MAX_FAILURES}" ]
    then
         YARN_CLUSTER_OPTIONS="${YARN_CLUSTER_OPTIONS} --conf spark.task.maxFailures=${SPARK_TASK_MAX_FAILURES}"
    fi


    PROPERTIES_FILE_PATH=`awk '{ if( $1 == "spark.properties.file.path:" ){ print $2 } }' ${CONF_FILE}`
    if [ ! -z "${PROPERTIES_FILE_PATH}" ]
    then
         YARN_CLUSTER_OPTIONS="${YARN_CLUSTER_OPTIONS} --properties-file ${PROPERTIES_FILE_PATH}"
    fi

    CONF_FILE="logisland-configuration.yml"
    ;;
  yarn-client)

    app_classpath=`echo ${app_classpath} | sed 's#,/[^,]*/logisland-spark_[^,-]*-engine[^,]*.jar,#,#'`
    app_classpath=`echo ${app_classpath} | sed 's#,/[^,]*/guava-[^,]*.jar,#,#'`
    app_classpath=`echo ${app_classpath} | sed 's#,/[^,]*/elasticsearch-[^,]*.jar,#,#'`
    YARN_CLUSTER_OPTIONS="--master yarn --deploy-mode client"

    DRIVER_CORES=`awk '{ if( $1 == "spark.driver.cores:" ){ print $2 } }' ${CONF_FILE}`
    if [ ! -z "${DRIVER_CORES}" ]
    then
 	 YARN_CLUSTER_OPTIONS="${YARN_CLUSTER_OPTIONS} --driver-cores ${DRIVER_CORES}"
    fi

    DRIVER_MEMORY=`awk '{ if( $1 == "spark.driver.memory:" ){ print $2 } }' ${CONF_FILE}`
    if [ ! -z "${DRIVER_MEMORY}" ]
    then
 	 YARN_CLUSTER_OPTIONS="${YARN_CLUSTER_OPTIONS} --driver-memory ${DRIVER_MEMORY}"
    fi

    PROPERTIES_FILE_PATH=`awk '{ if( $1 == "spark.properties.file.path:" ){ print $2 } }' ${CONF_FILE}`
    if [ ! -z "${PROPERTIES_FILE_PATH}" ]
    then
         YARN_CLUSTER_OPTIONS="${YARN_CLUSTER_OPTIONS} --properties-file ${PROPERTIES_FILE_PATH}"
    fi
    ;;
esac


java_cmd="${SPARK_HOME}/bin/spark-submit ${VERBOSE_OPTIONS} ${YARN_CLUSTER_OPTIONS} \
    --class ${app_mainclass} \
    --jars ${app_classpath} \
    ${lib_dir}/logisland-spark*-engine*.jar \
    -conf ${CONF_FILE}"

if [ ! -z "${VERBOSE_OPTIONS}" ]
then
  echo $java_cmd
fi

exec $java_cmd
