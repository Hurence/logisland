Log Island
==========

[![Build
Status](https://travis-ci.org/Hurence/log-island.svg?branch=master)](https://travis-ci.org/Hurence/log-island)

LogIsland is an event mining platform based on Spark and Kafka to handle a huge amount of log files.

![log-island architecture](http://hurence.github.io/log-island//public/LogIsland-architecture.png)

You can start right now to play with LogIsland through the Docker image, by following the [getting started guide](http://hurence.github.io/log-island/getting-started/)

The [documentation](http://hurence.github.io/log-island/) also explains how to [build]((http://hurence.github.io/log-island/build)) the source code in order to implement your own [plugins](http://hurence.github.io/log-island/plugins/).

Once you know how to run and build your own parsers and processors, you'll want to [deploy](http://hurence.github.io/log-island/deploy/) and scale them.




## Create a new plugin

    mvn archetype:generate -DarchetypeGroupId=com.hurence.logisland -DarchetypeArtifactId=logisland-plugin-archetype -DarchetypeVersion=0.9.4 -DlogislandVersion=0.9.4
    
    
    Define value for property 'groupId': : com.hurence.logisland
    Define value for property 'artifactId': : logisland-sample-plugin
    Define value for property 'version':  1.0-SNAPSHOT: : 0.1
    Define value for property 'artifactBaseName': : sample
    Define value for property 'package':  com.hurence.logisland.sample: :
    [INFO] Using property: logislandVersion = 0.9.4


## Basic Workflow

1. Raw log files are sent to Kafka topics by a NIFI / Logstash / Flume / Collectd (or whatever) agent 
3. Logs in Kafka topic are translated into Events and pushed back to another Kafka topic by a Spark streaming job
3. Events in Kafka topic are sent to Elasticsearch (or Solr or whatever backend) for online analytics (Kibana or Banana) by a Spark streaming job
4. Log topics can also dumped to HDFS (master dataset) for offline analytics
5. Event processor do some time window based analytics on events to build new events



    

## Start a log parser 

A *Log parser* takes a log line as a String and computes an Event as a sequence of fields. 
Let's start a `LogParser` streaming job with a custom `ApacheLogParser`. 
This stream will process log entries as soon as they will be queued into `li-apache-logs` Kafka topics, each log will be parsed as an event which will be pushed back to Kafka in the `li-apache-event` topic.


    $LOGISLAND_HOME/bin/log-parser \
        --kafka-brokers sandbox:9092 \
        --input-topics li-apache-logs \
        --output-topics li-apache-event \
        --max-rate-per-partition 10000 \
        --log-parser com.hurence.logisland.plugin.apache.ApacheLogParser


## Start an event mapper 

An *event mapper* takes an event and serialize it as an Elasticsearch document.
Let's start an `EventIndexer` with a custom mapper.
This stream will process event entries as soon as they will be queued into `li-apache-event` Kafka topics. 
Each event will be sent to Elasticsearch by bulk. 


    $LOGISLAND_HOME/bin/event-indexer \
        --kafka-brokers sandbox:9092 \
        --es-host sandbox \
        --index-name li-apache \
        --input-topics li-apache-event \
        --max-rate-per-partition 10000 \
        --event-mapper com.hurence.logisland.plugin.apache.ApacheEventMapper


## Start an event processor

//TODO 
