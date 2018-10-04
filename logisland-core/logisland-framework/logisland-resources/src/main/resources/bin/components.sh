#!/bin/sh

LOGISLAND_HOME=$(dirname $(dirname "$0"))

# Find the java binary
if [ -n "${JAVA_HOME}" ]; then
  RUNNER="${JAVA_HOME}/bin/java"
else
  if [ "$(command -v java)" ]; then
    RUNNER="java"
  else
    echo "JAVA_HOME is not set" >&2
    exit 1
  fi
fi


for i in $(ls "${LOGISLAND_HOME}"/lib/*.jar); do
    CLASSPATH=$CLASSPATH:$i
done

CLASSPATH=`echo $CLASSPATH | cut -c2-`



${RUNNER} -cp ${CLASSPATH} com.hurence.logisland.plugin.PluginManager "$@"
