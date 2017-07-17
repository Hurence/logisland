#!/bin/bash

:

rm /tmp/*.pid &>/dev/null

# altering the core-site configuration
#sed s/HOSTNAME/$HOSTNAME/ /usr/local/hadoop/etc/hadoop/core-site.xml.template > /usr/local/hadoop/etc/hadoop/core-site.xml


/etc/init.d/nginx start
service sshd start

echo "Starting kafka"
cd $KAFKA_HOME
#echo "host.name=sandbox" >> config/server.properties
nohup bin/zookeeper-server-start.sh config/zookeeper.properties > zookeeper.log 2>&1 &
JMX_PORT=10101 nohup bin/kafka-server-start.sh config/server.properties > kafka.log 2>&1 &

#echo "starting kafka-manager"
#cd $KAFKA_MGR_HOME
#nohup bin/kafka-manager > kafka-manager.log 2>&1 &

echo "Starting kibana"
cd $KIBANA_HOME
nohup bin/kibana > kibana.log 2>&1 &


echo "Starting grafana"
sudo service grafana-server start
grafana-server --config=/usr/local/etc/grafana/grafana.ini --homepath /usr/local/share/grafana cfg:default.paths.logs=/usr/local/var/log/grafana cfg:default.paths.data=/usr/local/var/lib/grafana cfg:default.paths.plugins=/usr/local/var/lib/grafana/plugins



#echo "you could start nifi with the following command"
#echo "cd $NIFI_HOME; bin/nifi.sh start"
#cd $NIFI_HOME
#sudo bin/nifi.sh start



#echo "create a kafka topic for logisland"
#sleep 5
#$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic logisland


echo "to start go to /usr/local/logisland"

#nohup bin/logindexer > log/logindexer.log 2>&1 &


echo -n "Starting elasticsearch"
runuser -l elastic -c '/usr/local/elasticsearch/bin/elasticsearch -d'

attempt=0
while [ $attempt -le 30 ]
do
    attempt=$(( $attempt + 1 ))
    echo -n "."
    if curl -s http://sandbox:9200/ > /dev/null
    then
      echo "OK"
      break
    fi
    sleep 1
done

sleep 5

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