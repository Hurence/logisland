#!/bin/bash

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
          #echo "add jar ${name}"
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

main() {

        app_classpath=""
        initSparkJarsOptRecursively "${1}"
        echo $app_classpath
}

main $@
