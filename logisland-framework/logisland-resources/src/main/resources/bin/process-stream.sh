#!/usr/bin/env bash

source $(dirname $0)/launcher.sh

declare -r app_mainclass="com.hurence.logisland.runner.StreamProcessingRunner"

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
  echo "  --yarn-cluster : flag to inform that the yarn-cluster mode must be used"
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
    --yarn-cluster)
      MODE="yarn-cluster"
      YARN_CLUSTER_OPTIONS="--master yarn --deploy-mode cluster --files ${CONF_FILE}#logisland-configuration.yml"
      ;;
    --yarn-client)
      MODE="yarn-client"
      YARN_CLUSTER_OPTIONS="--master yarn --deploy-mode client"
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

if [ -z "${CONF_FILE}" ]
then
    echo "The configuration file is missing"
    usage 
    exit 1
fi

if [ ! -f ${CONF_FILE} ]
then
  echo "The configuration file ${CONF_FILE} does not exist"
  usage
  exit 1
fi

case $MODE in
  default)
    app_classpath=`echo ${app_classpath} | sed 's#,/[^,]*/logisland-elasticsearch-shaded-[^,]*.jar,#,#'`
    ;;
  yarn-cluster)
    app_classpath=`echo ${app_classpath} | sed 's#,/[^,]*/logisland-spark-engine-[^,]*.jar,#,#'`
    app_classpath=`echo ${app_classpath} | sed 's#,/[^,]*/guava-[^,]*.jar,#,#'`
    app_classpath=`echo ${app_classpath} | sed 's#,/[^,]*/elasticsearch-[^,]*.jar,#,#'`
    CONF_FILE="logisland-configuration.yml"
    ;;
  yarn-client)
    app_classpath=`echo ${app_classpath} | sed 's#,/[^,]*/logisland-spark-engine-[^,]*.jar,#,#'`
    app_classpath=`echo ${app_classpath} | sed 's#,/[^,]*/guava-[^,]*.jar,#,#'`
    app_classpath=`echo ${app_classpath} | sed 's#,/[^,]*/elasticsearch-[^,]*.jar,#,#'`
    ;;
esac

declare java_cmd="${SPARK_HOME}/bin/spark-submit ${VERBOSE_OPTIONS} ${YARN_CLUSTER_OPTIONS} \
    --class ${app_mainclass} \
    --jars ${app_classpath} \
    ${lib_dir}/logisland-spark-engine*.jar \
    -conf ${CONF_FILE}" 

if [ ! -z "${VERBOSE_OPTIONS}" ]
then
  echo $java_cmd
fi

exec $java_cmd
