#!/bin/bash

export SPARK_HOME=$TOOLS/spark-1.6.2-bin-hadoop2.6/
logisland-assembly/target/logisland-0.10.1-bin-hdp2.4/logisland-0.10.1/bin/logisland.sh \
    --conf logisland-framework/logisland-resources/src/main/resources/conf/$1
