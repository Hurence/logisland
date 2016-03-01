


LogIsland is an event mining platform based on Spark and Kafka to handle a huge amount of log files.




## Basic Workflow

1. Raw log files are sent to Kafka topics by a Logstash / Flume / Collectd (or whatever) agent 
3. Logs in Kafka topic are translated into Events and pushed back to another Kafka topic by a Spark streaming job
3. Events in Kafka topic are sent to Elasticsearch (or Solr or whatever backend) for online analytics (Kibana or Banana) by a Spark streaming job
4. Log topics can also dumped to HDFS (master dataset) for offline analytics


## Build source code

to build only the source code

    sbt package

to build all dependecies into one single jar

    sbt assemblyPackageDependency

to publish jar to local ivy cache

    sbt publishLocal
    
    
    
## Build a distribution
to build API documentation

    sbt doc
    cp target/scala-2.10/api/ docs/_site/

to build user documentation

    cd docs
    jekyll build

    
## Build Docker image
The build the docker image, build log-island.tgz and kafka-manager tool

    # build a tgz archive with full standalone dependencies
    sbt universal:packageZipTarball 
    cp target/universal/log-island-*.tgz docker/
    
    # build kafka-manager
    git clone https://github.com/yahoo/kafka-manager.git
    cd kafka-manager
    sbt clean dist
    
    # build docker
    docker build --rm -t hurence/log-island:0.9.1 -f docker/Dockerfile .


## Running the image

* if using boot2docker make sure your VM has more than 2GB memory
* in your /etc/hosts file add $(boot2docker ip) as host 'sandbox' to make it easier to access your sandbox UI
* open yarn UI ports when running container

        docker run \
            -it \
            -p 80:80 \
            -p 9200-9300:9200-9300 \
            -p 5601:5601 \
            -p 2181:2181 \
            -p 9092:9092 \
            -p 9000:9000 \
            -p 4050-4060:4050-4060 \
            --name log-island \
            -h sandbox \
            hurence/log-island:0.9.1 bash
    

## Start a log parser 

A `Log` parser takes a log line as a String and computes an Event as a sequence of fields. 
Let's start a `LogParser` streaming job with a custom `ApacheLogParser`. 
This stream will process log entries as soon as they will be queued into `li-apache-logs` Kafka topics, each log will
be parsed as an event which will be pushed back to Kafka in the `li-apache-event` topic.


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
