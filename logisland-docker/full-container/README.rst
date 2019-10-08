LogIsland docker files
======================

Small standalone Hadoop distribution for development and testing purpose :

- Spark 1.6.2
- Elasticsearch 2.3.3
- Kibana 4.5.1
- Kafka 0.9.0.1
- Logisland 1.2.0


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

    # build logisland
    mvn clean package
    cp logisland-assembly/target/logisland-1.2.0-full-bin.tar.gz logisland-docker/full-container

The archive is generated under dist directory, 
you have to copy this file into your Dockerfile directory you can now issue

.. code-block:: sh

    cd logisland-docker/full-container/
    docker build --rm -t hurence/logisland  .
    docker tag hurence/logisland:latest hurence/logisland:1.2.1


Running the image
-----------------

* if using boot2docker make sure your VM has more than 2GB memory
* in your /etc/hosts file add $(boot2docker ip) as host 'sandbox' to make it easier to access your sandbox UI
* open yarn UI ports when running container

.. code-block:: sh

    docker run \
        -it \
        -p 8080-8082:8080-8082 \
        -p 3000:3000 \
        -p 4040-4060:4040-4060 \
        --name logisland \
        -h sandbox \
        hurence/logisland:1.2.0 bash

or

.. code-block::

    docker run -d hurence/logisland:1.2.0 -d

if you want to mount a directory from your host, add the following option :

.. code-block::

    -v ~/projects/logisland/docker/mount/:/usr/local/logisland


Deploy the image to Docker hub
------------------------------

tag the image as latest

.. code-block:: sh

    # verify image build
    docker images
    docker tag <IMAGE_ID> latest


then login and push the latest image

.. code-block:: sh

    docker login
    docker push hurence/logisland
