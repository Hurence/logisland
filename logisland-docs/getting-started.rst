

Getting Started
======================


In the following getting started tutorial we'll drive you through the process of Apache log mining with LogIsland platform.

We will start a Docker container hosting all the LogIsland services, launch two streaming processes and send some apache logs
to the system in order to analyze them in a dashboard.

.. note:: So how can I play as fast as possible ?

Start LogIsland as a Docker container
---------
LogIsland is packaged as a Docker container that you can build yourself or pull from Docker Hub.
The docker container is built from a Centos 6.4 image with the following tools enabled

- Kafka
- Spark
- Elasticsearch
- Kibana
- Logstash
- Flume
- Nginx
- LogIsland

Pull the image from Docker Repository (it may take some time)::

    docker pull hurence/logisland:latest

You should be aware that this Docker container is quite eager in RAM and will need at leat 8G of memory to run smoothly.

Now run the container

.. code-block::sh

    # run container
    docker run \
        -it \
        -p 80:80 \
        -p 8080:8080 \
        -p 3000:3000 \
        -p 9200-9300:9200-9300 \
        -p 5601:5601 \
        -p 2181:2181 \
        -p 9092:9092 \
        -p 9000:9000 \
        -p 4050-4060:4050-4060 \
        --name logisland \
        -h sandbox \
        hurence/logisland bash

    # get container ip
    docker inspect logisland

    # or if your are on mac os
    docker-machine ip default

you should add an entry for **sandbox** (with the container ip) in your `/etc/hosts` as it will be easier to access to all web services in logisland running container.




Start playing with logs
----


All we need now is a log parser and an event mapper, both are Java (or Scala) classes compiled into a jar file.

> Connect 2 shells to your logisland container to launch the following streaming jobs.

.. code-block::sh

    docker exec -ti logisland bash
    bin/logisland.sh --conf conf/configuration-template.yml

A ``SplitText`` processor takes a log line as a String and computes a ``Record`` as a sequence of fields.

This stream will process log entries as soon as they will be queued into `logisland_raw` Kafka topics, each log will
be parsed as an event which will be pushed back to Kafka in the ``logisland_events`` topic.



Inject some Apache logs into LogIsland (outside Docker)
----

Now we're going to work on the host machine, outside logisland Docker container.

We could setup a logstash or flume agent to load some apache logs into a kafka topic
but there's a super useful tool in the Kafka ecosystem : [kafkacat](https://github.com/edenhill/kafkacat),
a `generic command line non-JVM Apache Kafka producer and consumer` which can be easily installed.


.. code-block::sh

    #On recent enough Debian systems::
    sudo apt-get install kafkacat

    #And on Mac OS X with homebrew installed::
    brew install kafkacat

If you don't have your own httpd logs available, you can use some freely available log files from
[NASA-HTTP](http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html) web site access:

- [Jul 01 to Jul 31, ASCII format, 20.7 MB gzip compressed](ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz)
- [Aug 04 to Aug 31, ASCII format, 21.8 MB gzip compressed](ftp://ita.ee.lbl.gov/traces/NASA_access_logAug95.gz)

Send logs to LogIsland with kafkacat to ``logisland_raw`` Kafka topic

.. code-block:: sh

    wget ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
    gunzip NASA_access_log_Jul95.gz
    cat NASA_access_log_Jul95 | kafkacat -b sandbox:9092 -t logisland_raw



Use Kibana to inspect the logs
----

Open up your browser and go to `http://sandbox:5601/ <http://sandbox:5601/app/kibana#/discover?_g=(refreshInterval:(display:Off,pause:!f,value:0),time:(from:'1995-05-08T12:14:53.216Z',mode:absolute,to:'1995-11-25T05:30:52.010Z'))&_a=(columns:!(_source),filters:!(),index:'li-*',interval:auto,query:(query_string:(analyze_wildcard:!t,query:usa)),sort:!('@timestamp',desc),vis:(aggs:!((params:(field:host,orderBy:'2',size:20),schema:segment,type:terms),(id:'2',schema:metric,type:count)),type:histogram))&indexPattern=li-*&type=histogram>`_ and you should be able to explore your apache logs.

.. image:: /_static/kibana-explore.png


Monitor your spark jobs and Kafka topics
----

Now go to `http://sandbox:4050/streaming/ <http://sandbox:4050/streaming/>`_ to see how fast Spark can process
your data

.. image:: /_static/streaming-rate.png


Another tool can help you to tweak and monitor your processing `http://sandbox:9000/ <http://sandbox:9000>`_


.. image:: /_static/kafka-mgr.png
