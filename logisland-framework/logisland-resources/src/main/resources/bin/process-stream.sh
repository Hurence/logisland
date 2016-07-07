#!/usr/bin/env bash

source $(dirname $0)/launcher.sh



#--input-topics cisco-log --output-topics cisco-event --log-parser com.hurence.logisland.plugin.cisco.CiscoLogParser
declare -r app_mainclass="com.hurence.logisland.runner.StreamProcessingRunner"
#declare java_cmd="$SPARK_HOME/bin/spark-submit --class ${app_mainclass} --master local[2] --jars ${app_classpath} ${lib_dir}/com.hurence.log-island-*.jar  ${@}"

declare java_cmd="$SPARK_HOME/bin/spark-submit \
    --class ${app_mainclass} \
    --jars ${app_classpath} ${lib_dir}/logisland*.jar \
    ${@}"




echo $java_cmd
exec $java_cmd