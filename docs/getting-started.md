---
layout: page
title: Getting Started
permalink: /getting-started/
---

In the following getting started tutorial we'll drive you through the process of Apache log mining with LogIsland platform.

We will start a Docker container hosting all the LogIsland services, launch two streaming process and send some apache logs
to the system in order to analyze them in a dashboard.

> So how can I play as fast as possible ?

### Start LogIsland as a Docker container
LogIsland is packaged as a Docker container that you can build yourself or pull from Docker Hub. 
The docker container is build from a Centos 6.4 image with the following tools enabled

- Spark
- Elasticsearch
- Kibana
- Logstash
- Flume
- Nginx
- LogIsland

Pull the image from Docker Repository (it may take some time)

```
docker pull hurence/log-island:0.9.1
```

> You should be aware that this Docker is quite eager in RAM and will need at leat 8G of memory to run smoothly.

Now run the container 

```
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
    hurence/log-island:2.1.0 bash
```

You can now browse the web ui of each tools :

- elasticsearch [http://sandbox:9200/_plugin/kopf](http://sandbox:9200/_plugin/kopf) 
- kibana [http://sandbox:5601/](http://sandbox:5601/)
- kafka manager [http://sandbox:9000/](http://sandbox:9000/)
- spark manager [http://sandbox:4050/](http://sandbox:4050/)


All we need now is a log parser and an event mapper, both are Java (or Scala) classes compiled into a jar file.

> Open 2 shell windows in your Docker container to launch the following streaming jobs.


### Start a log parser 

A `Log` parser takes a log line as a String and computes an Event as a sequence of fields. 
Let's start a `LogParser` streaming job with a custom `ApacheLogParser`. 
This stream will process log entries as soon as they will be queued into `li-apache-logs` Kafka topics, each log will
be parsed as an event which will be pushed back to Kafka in the `li-apache-event` topic.

```
$LOGISLAND_HOME/bin/log-parser \
    --kafka-brokers sandbox:9092 \
    --input-topics li-apache-logs \
    --output-topics li-apache-event \
    --max-rate-per-partition 10000 \
    --log-parser com.hurence.logisland.plugin.apache.ApacheLogParser
```

### Start an event mapper 

An *event mapper* takes an event and serialize it as an Elasticsearch document.
Let's start an `EventIndexer` with a custom mapper.
This stream will process event entries as soon as they will be queued into `li-apache-event` Kafka topics. 
Each event will be sent to Elasticsearch by bulk. 

```
$LOGISLAND_HOME/bin/event-indexer \
    --kafka-brokers sandbox:9092 \
    --es-host sandbox \
    --index-name li-apache \
    --input-topics li-apache-event \
    --max-rate-per-partition 10000 \
    --event-mapper com.hurence.logisland.plugin.apache.ApacheEventMapper
```


> Please note that those streams will created automatically if they do not exists yet.

### Inject some Apache logs into LogIsland (outside Docker)
We could setup a logstash or flume agent to load some apache logs into a kafka topic 
but there's a super useful tool in the Kafka ecosystem : [kafkacat](https://github.com/edenhill/kafkacat), 
a `generic command line non-JVM Apache Kafka producer and consumer` which can be easily installed.

On recent enough Debian systems:

```
apt-get install kafkacat
```

And on Mac OS X with homebrew installed:

```
brew install kafkacat
```

If you don't have your own httpd logs available, you can use some freely available log files from 
[NASA-HTTP](http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html) web site access:

- [Jul 01 to Jul 31, ASCII format, 20.7 MB gzip compressed](ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz)
- [Aug 04 to Aug 31, ASCII format, 21.8 MB gzip compressed](ftp://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz)

Send logs to LogIsland with kafkacat to `li-apache-logs` Kafka topic

```
unzip NASA_access_log_Jul95.gz
cat NASA_access_log_Jul95 | kafkacat -b sandbox:9092 -t li-apache-logs
```



### Use Kibana to inspect the logs
Open up your browser and go to [http://sandbox:5601/](http://sandbox:5601/app/kibana#/discover?_g=(refreshInterval:(display:Off,pause:!f,value:0),time:(from:'1995-05-08T12:14:53.216Z',mode:absolute,to:'1995-11-25T05:30:52.010Z'))&_a=(columns:!(_source),filters:!(),index:'li-*',interval:auto,query:(query_string:(analyze_wildcard:!t,query:usa)),sort:!('@timestamp',desc),vis:(aggs:!((params:(field:host,orderBy:'2',size:20),schema:segment,type:terms),(id:'2',schema:metric,type:count)),type:histogram))&indexPattern=li-*&type=histogram) and you should be able to explore your apache logs.

![kibana](/public/kibana-explore.png)


### Monitor your spark jobs and Kafka topics
Now go to [http://sandbox:4050/streaming/](http://sandbox:4050/streaming/) to see how fast Spark can process
your data

![streaming-rate](/public/streaming-rate.png)


Another tool can help you to tweak and monitor your processing [http://sandbox:9000/](http://sandbox:9000) 

![kafka-mgr](/public/kafka-mgr.png)