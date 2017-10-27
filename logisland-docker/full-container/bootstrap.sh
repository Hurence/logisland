#!/bin/bash

:

rm /tmp/*.pid &>/dev/null

/etc/init.d/nginx start
service sshd start

echo "Starting kafka"
cd $KAFKA_HOME
#echo "host.name=sandbox" >> config/server.properties
nohup bin/zookeeper-server-start.sh config/zookeeper.properties > zookeeper.log 2>&1 &
JMX_PORT=10101 nohup bin/kafka-server-start.sh config/server.properties > kafka.log 2>&1 &

if [[ -n "${LOGISLAND_TUTORIALS}" ]]
then
    IFS=',' read -r -a LOGISLAND_TUTORIAL <<< "${LOGISLAND_TUTORIALS}"

    for TUTORIAL in "${LOGISLAND_TUTORIAL[@]}"
    do
        echo "Registering logisland tutorial '${TUTORIAL}'";

        if [[ "${TUTORIAL:0:1}" == "/" ]]
        then
          # Absolute path
          LOGISLAND_CONF="${TUTORIAL}.yml"
        else
          # Relative path
          LOGISLAND_CONF="$LOGISLAND_HOME/conf/${TUTORIAL}.yml"
        fi

        if [[ ! -f "${LOGISLAND_CONF}" ]]
        then
            echo "Logisland tutorial not found ${LOGISLAND_CONF}"
        else
            echo "Starting logisland tutorial:"
            echo "$LOGISLAND_HOME/bin/logisland.sh --conf ${LOGISLAND_CONF} &"
            $LOGISLAND_HOME/bin/logisland.sh --conf ${LOGISLAND_CONF} &
            sleep 20
        fi
    done
fi



echo "logisland is installed in /usr/local/logisland  enjoy!"
cd $LOGISLAND_HOME


CMD=${1:-"exit 0"}
if [[ "$CMD" == "-d" ]];
then
	service sshd stop
	/usr/sbin/sshd -D -d
else
	/bin/bash -c "$*"
fi