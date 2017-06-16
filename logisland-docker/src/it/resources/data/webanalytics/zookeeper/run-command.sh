#!/usr/bin/env bash

# A script that executes one or more command(s) provided as argument to zookeeper in non-interactive mode.
# Note that this is useless starting from zookeeper 3.4.7 that offers that functionality out-of-the-box.

LOGISLAND_LIB="/usr/local/logisland/lib"
cmd="${@}"
echo $cmd
java -cp ${LOGISLAND_LIB}/zookeeper-3.4.6.jar:${LOGISLAND_LIB}/slf4j-api-1.7.12.jar:${LOGISLAND_LIB}/log4j-api-2.7.jar:${LOGISLAND_LIB}/log4j-core-2.7.jar:${LOGISLAND_LIB}/log4j-over-slf4j-1.7.12.jar:${LOGISLAND_LIB}/netty-3.7.0.Final.jar:${LOGISLAND_LIB}/jline-0.9.94.jar \
     org.apache.zookeeper.ZooKeeperMain <<EOF
${cmd}
quit
EOF
