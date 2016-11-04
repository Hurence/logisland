How to deploy Log-Island ?
====

Basicaly you have 3 options

1. Single node Docker container
----
The easy way : you start a small Docker container with all you need inside (Elasticsearch, Kibana, Kafka, Spark, LogIsland + some usefull tools)

`Docker <https://www.docker.com>`_ is becoming an unavoidable tool to isolate a complex service component. It's easy to manage, deploy and maintain. That's why you can start right away to play with LogIsland through the Docker image provided from `Docker HUB <https://hub.docker.com/r/hurence/logisland/>`_

.. code-block:: sh

    # Get the LogIsland image
    docker pull hurence/logisland
    
    # Run the container
    docker run \
          -it \
          -p 80:80 \
          -p 9200-9300:9200-9300 \
          -p 5601:5601 \
          -p 2181:2181 \
          -p 9092:9092 \
          -p 9000:9000 \
          -p 4050-4060:4050-4060 \
          --name logisland \
          -h sandbox \
          hurence/logisland:latest bash
    
    # Connect a shell to your LogIsland container
    docker exec -ti logisland bash
    

2. Production mode in an Hadoop cluster
----
When it comes to scale, you'll need a cluster. **logisland** is just a framework that facilitates running sparks jobs over Kafka topics so if you already have a cluster you just have to get the latest logisland binaries and unzip them to a edge node of your hadoop cluster.

For now Log-Island is fully compatible with HDP 2.4 but it should work well on any cluster running Kafka and Spark.
Get the latest release and build the package.

You can download the `latest release build <https://github.com/Hurence/logisland/releases/download/v0.9.5/logisland-0.9.5-bin.tar.gz>`_

.. code-block:: bash

    git clone git@github.com:Hurence/logisland.git
    cd logisland-0.9.5
    mvn clean install -DskipTests

This will produce a ``logisland-assembly/target/logisland-0.9.5-bin.tar.gz`` file that you can untar into any folder of your choice in a edge node of your cluster.



Please read this excellent article on spark long running job setup : `http://mkuthan.github.io/blog/2016/09/30/spark-streaming-on-yarn/ <http://mkuthan.github.io/blog/2016/09/30/spark-streaming-on-yarn/>`_

3. Elastic scale with Mesos and Docker
----


.. note:: Coming next