
Frequently Asked Questions.
=====


I already use ELK, why would I need to use LogIsland ?
----
Well, at first one could say that that both stacks are overlapping, 
but the real purpose of the LogIsland framework is the abstraction of scalability of log aggregation.

In fact if you already have an ELK stack you'll likely want to make it scale (without pain) in both volume and features ways. 
LogIsland will be used for this purpose as an EOM (Event Oriented Middleware) based on Kafka & Spark, where you can plug advanced features
with ease.

So you just have to route your logs from the Logstash (or Flume, or Collectd, ...) agents to Kafka topics and launch parsers and processors.


Do I need Hadoop to play with LogIsland ?
----

No, if your goal is simply to aggregate a massive amount of logs in an Elasticsearch cluster, 
and to define complex event processing rules to generate new events you definitely don't need an Hadoop cluster. 

Kafka topics can be used as an high throughput log buffer for sliding-windows event processing. 
But if you need advanced batch analytics, it's really easy to dump your logs into an hadoop cluster to build machine learning models.


How do I make it scale ?
----

LogIsland relies on Spark and Kafka which are both scalable by essence, to scale LogIsland just have to add more kafka brokers and more Spark slaves.
This is the *manual* way, but we've planned in further releases to provide either Docker Swarn support or Mesos Marathon.


What's the difference between Apache NIFI and LogIsland ?
----

Apache NIFI is a scalable ETL very well suited to process incoming data such as logs file, process & enrich them and send them out to any datastore.
You can do that as well with LogIsland but LogIsland is an event oriented framework designed to process huge amount of events in a Complex Event Processing
manner not a Single Event Processing as NIFI does.

Anyway you can use Apache NIFI to process your logs and send them to Kafka in order to be processed by LogIsland


Error : realpath not found
----

If you don't habe the ``realpath`` command on you system you may need to install it::

    brew install coreutils
    
    sudo apt-get install coreutils

