LogIsland Bro docker files
==========================

These files allow to build a Docker image containing Bro necessary bits to run
the `Indexing Bro events <https://github.com/Hurence/logisland/blob/master/logisland-docs/tutorials/indexing-bro-events.rst>`_ tutorial.

TODO TODO TODO  modify rest of this page...

.. code-block:: sh

    docker build --rm -t hurence/bro:0.9.8 .   
    docker run -it --name bro hurence/bro:0.9.8
 
- Spark 1.6.2
- Elasticsearch 2.3.3
- Kibana 4.5.1
- Kafka 0.9.0.1
- Logisland 0.9.8


This repository contains a Docker file to build a Docker image with Apache Spark, HBase, Flume & Zeppelin. 
This Docker image depends on [centos 6.7](https://github.com/CentOS/CentOS-Dockerfiles) image.

Getting the docker image from repository
----------------------------------------

Pull the image from Docker Repository

.. code-block:: sh

    docker pull hurence/logisland


Build your own
--------------

Building the image

.. code-block:: sh

    # build logisland (from the root directory of the workspace)
    mvn clean install
    cp logisland-assembly/target/logisland-0.9.8-bin.tar.gz logisland-docker
    # go into the docker directory and build the docker image
    cd logisland-docker
    docker build --rm -t hurence/logisland:0.9.8 .

Running the image
-----------------

* if using boot2docker make sure your VM has more than 2GB memory
* in your /etc/hosts file add $(boot2docker ip) as host 'sandbox' to make it easier to access your sandbox UI
* open yarn UI ports when running container

.. code-block:: sh

    docker run \
        -it \
        -p 80:80 \
        -p 9200-9300:9200-9300 \
        -p 5601:5601 \
        -p 2181:2181 \
        -p 9092:9092 \
        -p 9000:9000 \
        -p 8080-8082:8080-8082 \
        -p 3000:3000 \
        -p 4050-4060:4050-4060 \
        --name logisland \
        -h sandbox \
        hurence/logisland:0.9.8 bash

or

.. code-block::

    docker run -d -h sandbox hurence/logisland:0.9.8 -d

if you want to mount a directory from your host, add the following option :

.. code-block::

    -v ~/projects/logisland/docker/mount/:/usr/local/logisland

