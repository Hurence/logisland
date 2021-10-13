#!/usr/bin/env bash

TMP="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
declare -r current_dir="$TMP"

assembly1=$1
assembly2=$2


#
# dif between content of directories
#
cd "${assembly1}"
content_assembly1=$(find logisland-*-full-bin/ > "${current_dir}/assembly.txt")

cd "${current_dir}"
cd "${assembly2}"
content_assembly2=$(find logisland-*-full-bin/ > "${current_dir}/assembly2.txt")

cd "${current_dir}"
echo "diff between files in assembly"
diff assembly.txt assembly2.txt

#
# dif between individual jars
#


