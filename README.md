Log Island
==========

[![Build
Status](https://travis-ci.org/Hurence/log-island.svg?branch=master)](https://travis-ci.org/Hurence/log-island)

LogIsland is an event mining platform based on Spark and Kafka to handle a huge amount of log files.

![log-island architecture](http://hurence.github.io/log-island//public/LogIsland-architecture.png)

You can start right now to play with LogIsland through the Docker image, by following the [getting started guide](http://hurence.github.io/log-island/getting-started/)

The [documentation](http://hurence.github.io/log-island/) also explains how to [build]((http://hurence.github.io/log-island/build)) the source code in order to implement your own [plugins](http://hurence.github.io/log-island/plugins/).

Once you know how to run and build your own parsers and processors, you'll want to [deploy](http://hurence.github.io/log-island/deploy/) and scale them.


## Build and deploy
to build from the source just clone and package

    git clone
    mvn package
    
to deploy artifacts (if you're allowed to), follow this guide [release to OSS Sonatype with maven](http://central.sonatype.org/pages/apache-maven.html)

    
    mvn versions:set -DnewVersion=0.9.5-SNAPSHOT
    mvn clean deploy -Psonatype
    
follow the staging procedure in [https://oss.sonatype.org/#stagingRepositories](https://oss.sonatype.org/#stagingRepositories) or read [Sonatype book](http://books.sonatype.com/nexus-book/reference/staging-deployment.html#staging-maven)
    


## Basic Workflow

1. Raw log files are sent to Kafka topics by a NIFI / Logstash / Flume / Collectd (or whatever) agent 
3. Logs in Kafka topic are translated into Events and pushed back to another Kafka topic by a Spark streaming job
3. Events in Kafka topic are sent to Elasticsearch (or Solr or whatever backend) for online analytics (Kibana or Banana) by a Spark streaming job
4. Log topics can also dumped to HDFS (master dataset) for offline analytics
5. Event component do some time window based analytics on events to build new events



    

## Setup a stream processing workflog

A LogIsland stream processing flow is made of a bunch of components. At least one streaming engine and 1 or more stream processors. You set them up by a YAML configuration file. Please note that events are serialized against an Avro schema while transiting through any Kafka topic. Every `spark.streaming.batchDuration` (time window), each processor will handle its bunch of Events to eventually generate some new events to the output topic.
The following `conf/configuration-template.yml` contains a sample of processor definitions.

```YAML

    version: 0.1
    documentation: LogIsland analytics main config file. Put here every engine or component config
    
    components:
      # Main event streaming engine
      - component: com.hurence.logisland.engine.SparkStreamProcessingEngine
        type: engine
        version: 0.1.0
        documentation: Main Logisland job entry point
        configuration:
          spark.master: local[8]
          spark.executorMemory: 4g
          spark.checkpointingDirectory: file:///tmp
          spark.appName: My first stream component
          spark.streaming.batchDuration: 2000
          spark.serializer: org.apache.spark.serializer.KryoSerializer
          spark.streaming.backpressure.enabled: true
          spark.streaming.unpersist: false
          spark.streaming.blockInterval: 350
          spark.streaming.kafka.maxRatePerPartition: 500
          spark.streaming.timeout: 20000
          spark.ui.port: 4050
          kafka.metadata.broker.list: localhost:9092
          kafka.zookeeper.quorum: localhost:2181
    
      # A Debug component that only logs what it reads
      - component: com.hurence.logisland.processor.debug.EventDebuggerProcessor
        type: processor
        version: 0.1.0
        documentation: a processor that trace the processed events
        configuration:
          kafka.input.topics: logisland-mock-in
          kafka.output.topics: none
          kafka.error.topics: none
          avro.input.schema: |
                  {"version":1,"type":"record","namespace":"com.hurence.logisland","name":"Event","fields":[{"name":"_type","type":"string"},{"name":"_id","type":"string"},{"name":"timestamp","type":"long"},{"name":"method","type":"string"},{"name":"ipSource","type":"string"},{"name":"ipTarget","type":"string"},{"name":"urlScheme","type":"string"},{"name":"urlHost","type":"string"},{"name":"urlPort","type":"string"},{"name":"urlPath","type":"string"},{"name":"requestSize","type":"int"},{"name":"responseSize","type":"int"},{"name":"isOutsideOfficeHours","type":"boolean"},{"name":"isHostBlacklisted","type":"boolean"},{"name":"tags","type":{"type":"array","items":"string"}}]}
          avro.output.schema: |
                        {"version":1,"type":"record","namespace":"com.hurence.logisland","name":"Event","fields":[{"name":"_type","type":"string"},{"name":"_id","type":"string"},{"name":"timestamp","type":"long"},{"name":"method","type":"string"},{"name":"ipSource","type":"string"},{"name":"ipTarget","type":"string"},{"name":"urlScheme","type":"string"},{"name":"urlHost","type":"string"},{"name":"urlPort","type":"string"},{"name":"urlPath","type":"string"},{"name":"requestSize","type":"int"},{"name":"responseSize","type":"int"},{"name":"isOutsideOfficeHours","type":"boolean"},{"name":"isHostBlacklisted","type":"boolean"},{"name":"tags","type":{"type":"array","items":"string"}}]}
    
      # Generate random events based on an avro schema
      - component: com.hurence.logisland.processor.randomgenerator.RandomEventGeneratorProcessor
        type: processor
        version: 0.1.0
        documentation: a processor that produces random events
        configuration:
          kafka.input.topics: none
          kafka.output.topics: logisland-mock-in
          kafka.error.topics: logisland-error
          min.events.count: 5
          max.events.count: 100
          avro.input.schema: |
                        {"version":1,"type":"record","namespace":"com.hurence.logisland","name":"Event","fields":[{"name":"_type","type":"string"},{"name":"_id","type":"string"},{"name":"timestamp","type":"long"},{"name":"method","type":"string"},{"name":"ipSource","type":"string"},{"name":"ipTarget","type":"string"},{"name":"urlScheme","type":"string"},{"name":"urlHost","type":"string"},{"name":"urlPort","type":"string"},{"name":"urlPath","type":"string"},{"name":"requestSize","type":"int"},{"name":"responseSize","type":"int"},{"name":"isOutsideOfficeHours","type":"boolean"},{"name":"isHostBlacklisted","type":"boolean"},{"name":"tags","type":{"type":"array","items":"string"}}]}
          
          avro.output.schema: |
                              {"version":1,"type":"record","namespace":"com.hurence.logisland","name":"Event","fields":[{"name":"_type","type":"string"},{"name":"_id","type":"string"},{"name":"timestamp","type":"long"},{"name":"method","type":"string"},{"name":"ipSource","type":"string"},{"name":"ipTarget","type":"string"},{"name":"urlScheme","type":"string"},{"name":"urlHost","type":"string"},{"name":"urlPort","type":"string"},{"name":"urlPath","type":"string"},{"name":"requestSize","type":"int"},{"name":"responseSize","type":"int"},{"name":"isOutsideOfficeHours","type":"boolean"},{"name":"isHostBlacklisted","type":"boolean"},{"name":"tags","type":{"type":"array","items":"string"}}]}
```    



## Start an the stream workflow

One you've edited your configuration file, you can submit it to execution engine with the following cmd :


    bin/process-stream.sh -conf conf/configuration-template.yml


## Create a new plugin
Logisland processors are hosted in some plugins, you can create your own with a maven archetype.


    git clone git@github.com:Hurence/log-island.git
    cd logisland-0.9.4/logisland-plugins
    mvn archetype:generate -DarchetypeGroupId=com.hurence.logisland -DarchetypeArtifactId=logisland-plugin-archetype -DarchetypeVersion=0.9.4 -DlogislandVersion=0.9.4
    
    
    Define value for property 'groupId': : com.hurence.logisland
    Define value for property 'artifactId': : logisland-sample-plugin
    Define value for property 'version':  1.0-SNAPSHOT: : 0.1
    Define value for property 'artifactBaseName': : sample
    Define value for property 'package':  com.hurence.logisland.sample: :
    [INFO] Using property: logislandVersion = 0.9.4

