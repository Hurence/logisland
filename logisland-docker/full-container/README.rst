LogIsland docker files
======================

Small standalone Hadoop distribution for development and testing purpose :

- Spark 1.6.2
- Elasticsearch 2.3.3
- Kibana 4.5.1
- Kafka 0.9.0.1
- Logisland 1.4.0

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
    cp logisland-assembly/target/logisland-1.4.0-full-bin.tar.gz logisland-docker/full-container

Once the full logisland archive is copied in the logisland docker image directory, directory you can now issue:

.. code-block:: sh

    cd logisland-docker/full-container/
    docker build --rm -t hurence/logisland  .
    docker tag hurence/logisland:latest hurence/logisland:1.4.0

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
        hurence/logisland:1.4.0 bash

or

.. code-block::

    docker run -d hurence/logisland:1.4.0 -d

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

Buil Opncv into a docker file alpine
------------------------------------

.. code-block:: sh

    RUN apk add --update --no-cache \
          build-base \
          openblas-dev \
          unzip \
          wget \
          cmake \
          libjpeg  \
          libjpeg-turbo-dev \
          libpng-dev \
          jasper-dev \
          tiff-dev \
          libwebp-dev \
          clang-dev \
          linux-headers \
          python \
          py-pip \
          python-dev \
          apache-ant && \
        pip install numpy

    ENV CC /usr/bin/clang
    ENV CXX /usr/bin/clang++
    ENV OPENCV_VERSION 4.1.1
    ENV  JAVA_HOME /opt/jdk

    RUN cd /opt && \
      wget https://github.com/opencv/opencv/archive/${OPENCV_VERSION}.zip && \
      unzip ${OPENCV_VERSION}.zip && \
      rm -rf ${OPENCV_VERSION}.zip

    RUN mkdir -p /opt/opencv-${OPENCV_VERSION}/build && \
      cd /opt/opencv-${OPENCV_VERSION}/build && \
      cmake \
      -D CMAKE_BUILD_TYPE=RELEASE \
      -D CMAKE_INSTALL_PREFIX=/usr/local \
      -D PYTHON2_EXECUTABLE=/usr/bin/python \
      -D PYTHON_INCLUDE_DIR=/usr/include/python2.7  \
      -D WITH_FFMPEG=NO \
      -D WITH_IPP=NO \
      -D WITH_OPENEXR=NO \
      -D WITH_TBB=NO \
      -D BUILD_EXAMPLES=NO \
      -D BUILD_ANDROID_EXAMPLES=NO \
      -D INSTALL_PYTHON_EXAMPLES=NO \
      -D BUILD_DOCS=NO \
      -D BUILD_opencv_python2=NO \
      -D BUILD_opencv_python3=NO \
      -D BUILD_opencv_java=ON \
      -D BUILD_SHARED_LIBS=OFF \
      -D BUILD_EXAMPLES=OFF \
      -D BUILD_TESTS=OFF \
      -D BUILD_PERF_TESTS=OFF \
      .. && \
      make -j8 && \
      make install && \
      rm -rf /opt/opencv-${OPENCV_VERSION}


    mvn install:install-file -Dfile=/usr/local/share/java/opencv4/opencv-411.jar -DgroupId=opencv -DartifactId=opencv -Dversion=4.1.1 -Dpackaging=jar