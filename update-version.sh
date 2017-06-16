#!/usr/bin/env bash

# update_version - A script to update the logisland version




## declare an array variable
declare -a extension=(".rst" "pom.xml" ".html" ".yml" ".txt" ".md" "SparkJobLauncher.java" "StreamProcessingRunner.java")


function usage
{
    echo "usage: update_version -o old_version -n new_version -d"
}

old_version=
new_version=
dry_run=false

while [ "$1" != "" ]; do
    case $1 in
        -o | --old_version )    shift
                                old_version=$1
                                ;;
        -n | --new_version )    shift
                                new_version=$1
                                ;;
        -d | --dry_run )        dry_run=true
                                ;;
        -h | --help )           usage
                                exit
                                ;;
        * )                     usage
                                exit 1
    esac
    shift
done



SED_REPLACE="s/$old_version/$new_version/g"

## now loop through the above array
if [ "$dry_run" = true ]; then
     grep -r -n -i --exclude-dir=\*{.idea,.git} --exclude="*.iml"  "$old_version" .
else

    for i in `grep -r -n -i -l --exclude-dir=\*{.idea,.git} --exclude=*.iml  "$old_version" .` ; do
        echo  $i;
        sed -i '' "$SED_REPLACE" $i
     done
fi
