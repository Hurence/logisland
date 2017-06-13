LogIsland Bro Docker files
==========================

Content
-------

These files allow to build a Docker image containing `Bro <https://www.bro.org/>`_ necessary bits to run
the `Indexing Bro events <https://github.com/Hurence/logisland/blob/master/logisland-documentation/tutorials/indexing-bro-events.rst>`_ tutorial.

Basically, the image is based on the `Ubuntu <https://hub.docker.com/_/ubuntu/>`_ 16.04 image. It also contains:

- Bro
- Bro-Kafka plugin

The `Bro-Kafka <https://github.com/bro/bro-plugins/tree/master/kafka>`_ plugin needs to be compiled with both the `librdkafka <https://github.com/edenhill/librdkafka>`_ library as
well as the `Bro <https://github.com/bro/bro>`_ workspace. That is why
the sources for those 3 components are available in the image under ``/root/sources``

But Bro is already installed with Bro-Kafka plugin under ``/usr/local/bro`` (``$BRO_HOME``).

The Bro-Kafka plugin needs to be configured in the Bro configuration file. This is explained in the tutorial.

Getting the docker image from repository
----------------------------------------

Pull the image from Docker Repository:

.. code-block:: sh

    docker pull hurence/bro
    
.. warning::

   If the Bro Docker image is not yet available on the Docker Hub, please refer to the next section to build your own image.


Build your own
--------------

To build the image, in the current directory type:

.. code-block:: sh

    docker build --rm -t hurence/bro:0.9.8 .

Running the image
-----------------

To run the container:

.. code-block:: sh

    docker run -it --name bro -h bro hurence/bro:0.9.8
