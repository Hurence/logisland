Prerequisites
=============

There are two main ways to launch a logisland job :

- within Docker containers
- within an Hadoop distribution (Cloudera, Hortonworks, ...)


1. Trough a Docker container (testing way)
------------------------------------------
Logisland is packaged as a Docker container that you can build yourself or pull from Docker Hub.

To facilitate integration testing and to easily run tutorials, you can create a `docker-compose.yml` file with the following content, or directly download it from `a gist <https://gist.githubusercontent.com/oalam/706e719baf6bb6df46acdc4cd96ac72f/raw/08c014f3e7116f23a5edae30f82422dd297e8263/docker-compose.yml>`_

.. code-block:: yaml

    # Zookeeper container 172.17.0.1
    zookeeper:
      image: hurence/zookeeper
      hostname: zookeeper
      container_name: zookeeper
      ports:
        - "2181:2181"

    # Kafka container
    kafka:
      image: hurence/kafka
      hostname: kafka
      container_name: kafka
      links:
        - zookeeper
      ports:
        - "9092:9092"
      environment:
        KAFKA_ADVERTISED_PORT: 9092
        KAFKA_ADVERTISED_HOST_NAME: sandbox
        KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
        KAFKA_JMX_PORT: 7071

    # ES container
    elasticsearch:
      environment:
        - ES_JAVA_OPT="-Xms1G -Xmx1G"
        - cluster.name=es-logisland
        - http.host=0.0.0.0
        - transport.host=0.0.0.0
        - xpack.security.enabled=false
      hostname: elasticsearch
      container_name: elasticsearch
      image: 'docker.elastic.co/elasticsearch/elasticsearch:5.4.0'
      ports:
        - '9200:9200'
        - '9300:9300'

    # Kibana container
    kibana:
      environment:
        - 'ELASTICSEARCH_URL=http://elasticsearch:9200'
      image: 'docker.elastic.co/kibana/kibana:5.4.0'
      container_name: kibana
      links:
        - elasticsearch
      ports:
        - '5601:5601'

    # Logisland container : does nothing but launching
    logisland:
      image: hurence/logisland
      command: tail -f bin/logisland.sh
      #command: bin/logisland.sh --conf /conf/index-apache-logs.yml
      links:
        - zookeeper
        - kafka
        - elasticsearch
      ports:
        - "4050:4050"
      volumes:
        - ./conf/logisland:/conf
        - ./data/logisland:/data
      container_name: logisland
      extra_hosts:
        - "sandbox:172.17.0.1"

Once you have this file you can run a `docker-compose` command to launch all the needed services (zookeeper, kafka, es, kibana and logisland)

.. code-block:: sh

    docker-compose up

.. note::

    you should add an entry for **sandbox** (with the container ip) in your ``/etc/hosts`` as it will be easier to access to all web services in logisland running container.


Any logisland script can now be launched by running a `logisland.sh` script within the logisland docker container like in the example below where we launch `index-apache-logs.yml` job :

.. code-block:: sh

    docker exec -i -t logisland bin/logisland.sh --conf conf/index-apache-logs.yml



2. Through an Hadoop cluster (production way)
---------------------------------------------

Now you have played with the tool, you're ready to deploy your jobs into a real distributed cluster.
From an edge node of your cluster :

- download and extract the `latest release <https://github.com/Hurence/logisland/releases>`_ of logisland
- export `SPARK_HOME` and `HADOOP_CONF_DIR` environment variables
- run `logisland.sh` launcher script with your job conf file.


.. code-block:: sh

    cd /opt
    sudo wget https://github.com/Hurence/logisland/releases/download/v0.10.3/logisland-0.10.3-bin-hdp2.5.tar.gz

    export SPARK_HOME=/opt/spark-2.1.0-bin-hadoop2.7/
    export HADOOP_CONF_DIR=$SPARK_HOME/conf

    sudo /opt/logisland-0.10.3/bin/logisland.sh --conf /home/hurence/tom/logisland-conf/v0.10.0/future-factory.yml

