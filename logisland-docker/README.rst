LogIsland docker files
===========

Small standalone Hadoop distribution for development and testing purpose :

- Spark 1.6.2
- Elasticsearch 2.3.3
- Kibana 4.5.1
- Kafka 0.9.0.1
- Logisland 0.9.5


This repository contains a Docker file to build a Docker image with Apache Spark, HBase, Flume & Zeppelin. 
This Docker image depends on [centos 6.7](https://github.com/CentOS/CentOS-Dockerfiles) image.

Getting the docker image
------------------------

Pull the image from Docker Repository::

    docker pull hurence/logisland

Building the image::

    # build logisland
    maven clean install
    cp logisland-assembly/target/logisland-0.9.5-bin.tar.gz logisland-docker

The archive is generated under dist directory, 
you have to copy this file into your Dockerfile directory you can now issue :: 

    docker build --rm -t hurence/logisland:0.9.5 .


Running the image
-----------------

* if using boot2docker make sure your VM has more than 2GB memory
* in your /etc/hosts file add $(boot2docker ip) as host 'sandbox' to make it easier to access your sandbox UI
* open yarn UI ports when running container

.. code-block::

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
        hurence/logisland:latest bash

or

.. code-block::

    docker run -d -h sandbox hurence/logisland:latest -d

if you want to mount a directory from your host, add the following option :

.. code-block::

    -v ~/projects/logisland/docker/mount/:/usr/local/logisland

