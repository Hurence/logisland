#!/usr/bin/env bash

CURRENT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
LOGISLAND_HOME=$(dirname ${CURRENT_DIR})


echo "CURRENT_DIR: ${CURRENT_DIR}"
echo "LOGISLAND_HOME: ${LOGISLAND_HOME}"
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

echo "RUNNER: ${RUNNER}"


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

java_cp=""
initJavaCpOptRecursively "${LOGISLAND_HOME}/lib"

#echo "java_cp: ${java_cp}"

${RUNNER} -cp ${java_cp} com.hurence.logisland.plugin.PluginManager "$@"
