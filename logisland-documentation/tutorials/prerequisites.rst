Prerequisites
=============

There are two main ways to launch a logisland job :

- within Docker containers
- within an Hadoop distribution (Cloudera, Hortonworks, ...)

1. Trough a Docker container (testing way)
------------------------------------------
Logisland is packaged as a Docker container that you can build yourself or pull from Docker Hub.

To facilitate integration testing and to easily run tutorials, you can use `docker-compose` with the followings :
* `docker-compose.yml <https://raw.githubusercontent.com/Hurence/logisland/master/logisland-framework/logisland-resources/src/main/resources/conf/docker-compose.yml>`_.

Once you have these file you can run a `docker-compose` command to launch all the needed services (zookeeper, kafka, es, kibana, redis and logisland).
(You can remove the services that you do not need depending on tutorial).

Elasticsearch on docker needs a special tweak as described `here <https://www.elastic.co/guide/en/elasticsearch/reference/current/docker.html#docker-cli-run-prod-mode>`_

.. code-block:: sh

    # set vm.max_map_count kernel setting for elasticsearch
    sudo sysctl -w vm.max_map_count=262144

    #
    cd /tmp
    wget https://raw.githubusercontent.com/Hurence/logisland/master/logisland-framework/logisland-resources/src/main/resources/conf/docker-compose.yml
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
    sudo wget https://github.com/Hurence/logisland/releases/download/v1.0.0-RC1/logisland-1.0.0-RC1-bin-hdp2.5.tar.gz

    export SPARK_HOME=/opt/spark-2.1.0-bin-hadoop2.7/
    export HADOOP_CONF_DIR=$SPARK_HOME/conf

    sudo /opt/logisland-1.0.0-RC1/bin/logisland.sh --conf /home/hurence/tom/logisland-conf/v0.10.0/future-factory.yml

