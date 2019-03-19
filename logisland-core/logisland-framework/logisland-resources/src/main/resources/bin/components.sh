#!/usr/bin/env bash

CURRENT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
LOGISLAND_HOME=$(dirname ${CURRENT_DIR})

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
