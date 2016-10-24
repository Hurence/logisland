How to extend LogIsland ?
=======

In this new tutorial we will learn how to create a custom log parser and how to run it inside logisland Docker container


Maven setup
----

Create a folder for your ``super-plugin`` project :


.. code-block:: sh

    mkdir -p super-plugin/lib
    mkdir -p src/main/java/com/hurence/logisland

First you need to build logisland and to get the pom and jars availables for your projet

.. code-block:: sh

    git clone https://github.com/Hurence/logisland.git
    cd logisland

.. note::

logisland jar dependencies are released on maven central :

.. code-block:: xml

    <!-- https://mvnrepository.com/artifact/com.hurence.logisland/logisland-api -->
    <dependency>
        <groupId>com.hurence.logisland</groupId>
        <artifactId>logisland-api</artifactId>
        <version>0.9.5</version>
    </dependency>

.. code-block:: sh

    ├── pom.xml
    ├── src
    │   ├── main
    │   │   ├── java
    │   │   │   └── com
    │   │   │       └── hurence
    │   │   │           └── logisland
    │   │   │               └── MyLogParser.java
    │   │   └── resources
    │   └── test
    │       └── java


Edit your `pom.xml` as follows


Write a custom log parser
---

Write your a custom LogParser for your super-plugin in ``/src/main/java/com/hurence/logisland/MyLogParser.java``

Our parser will analyze some Proxy Log String in the following form :

	"Thu Jan 02 08:43:39 CET 2014	GET	10.118.32.164	193.251.214.117	http	webmail.laposte.net	80	/webmail/fr_FR/Images/Images-20130905100226/Images/RightJauge.gif	724	409	false	false"


.. code-block:: java

    package com.hurence.logisland;

    import com.hurence.logisland.event.Event;
    import com.hurence.logisland.log.LogParser;
    import com.hurence.logisland.log.LogParserException;
    
    import java.text.SimpleDateFormat;
    
    /**
     * NetworkFlow(
     * timestamp: Long,
     * method: String,
     * ipSource: String,
     * ipTarget: String,
     * urlScheme: String,
     * urlHost: String,
     * urlPort: String,
     * urlPath: String,
     * requestSize: Int,
     * responseSize: Int,
     * isOutsideOfficeHours: Boolean,
     * isHostBlacklisted: Boolean,
     * tags: String)
     */
    public class ProxyLogParser implements LogParser {
    
        /**
         * take a line of csv and convert it to a NetworkFlow
         *
         * @param s
         * @return
         */
        public Event[] parse(String s) throws LogParserException {
    
    
            Event event = new Event();
    
            try {
                String[] records = s.split("\t");
    
                try {
                    SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy");
                    event.put("timestamp", "long", sdf.parse(records[0]).getTime());
                } catch (Exception e) {
                    event.put("parsing_error", e.getMessage());
                }
    
                event.put("method", "string", records[1]);
                event.put("ipSource", "string", records[2]);
                event.put("ipTarget", "string", records[3]);
                event.put("urlScheme", "string", records[4]);
                event.put("urlHost", "string", records[5]);
                event.put("urlPort", "string", records[6]);
                event.put("urlPath", "string", records[7]);
    
                try {
                    event.put("requestSize", "int", Integer.parseInt(records[8]));
                } catch (Exception e) {
                    event.put("parsing_error", e.getMessage());
                }
                try {
                    event.put("responseSize", "int", Integer.parseInt(records[9]));
                } catch (Exception e) {
                    event.put("parsing_error", e.getMessage());
                }
                try {
                    event.put("isOutsideOfficeHours", "bool", Boolean.parseBoolean(records[10]));
                } catch (Exception e) {
                    event.put("parsing_error", e.getMessage());
                }
                try {
                    event.put("isHostBlacklisted", "bool", Boolean.parseBoolean(records[11]));
                } catch (Exception e) {
                    event.put("parsing_error", e.getMessage());
                }
    
    
                if (records.length == 13) {
                    String tags = records[12].replaceAll("\"", "").replaceAll("\\[", "").replaceAll("\\]", "");
                    event.put("tags", "string", tags);
                }
    
    
            }catch (Exception e) {
                event.put("parsing_error", e.getMessage());
            }
    
            Event[] result = new Event[1];
            result[0] = event;
    
            return result;
        }
    
    }

Test your parser with JUnit
----


which can be tested (not really deeply ...) with a small unit test


Deploy the custom component to Docker container
----

Now you have a fully functionnal plugin and you can build it with maven by running

.. code-block:: sh

	mvn package

It's time to deploy our splendid little plugin to logisland. We'll get the Docker image, run this container by `mounting a host directory into the container` to share the brand new jar we have built.

.. code-block:: sh

   docker pull hurence/logisland:latest
   docker run \
        -it \
        -p 80:80 \
        -p 9200-9300:9200-9300 \
        -p 5601:5601 \
        -p 2181:2181 \
        -p 9092:9092 \
        -p 9000:9000 \
        -p 4050-4060:4050-4060 \
        --name logisland \
        -h sandbox \
        -v $HOME/Documents/workspace/hurence/projects/super-plugin/:/usr/local/logisland/super-plugin  \
        hurence/logisland:latest bash
    
   cd $LOGISLAND_HOME
   cp super-plugin/target/super-plugin-1.0-SNAPSHOT.jar lib/


Start a log parser 
----




A `Log` parser takes a log line as a String and computes an Event as a sequence of fields. 
Let's start a `LogParser` streaming job with a custom `ApacheLogParser`. 
This stream will process log entries as soon as they will be queued into `li-apache-logs` Kafka topics, each log will
be parsed as an event which will be pushed back to Kafka in the `li-apache-event` topic.

.. code-block:: sh

    $LOGISLAND_HOME/bin/log-parser \
        --kafka-brokers sandbox:9092 \
        --input-topics li-proxy-logs \
        --output-topics li-proxy-events \
        --max-rate-per-partition 10000 \
        --log-parser com.hurence.logisland.ProxyLogParser

As in the [getting started guide]({{ site.baseurl }}/getting-started) you can use `kafkacat` tool to inject the following [proxy log file]({{ site.baseurl }}/public/proxy.log)


.. code-block:: sh

    cat proxy.log | kafkacat -P -b sandbox -t li-proxy-logs


In another Docker shell, you should see that some events are going into Kafka (even if they're serialized in Kryo and you can't understand anything)

	/usr/local/kafka/bin/kafka-console-consumer.sh --from-beginning --topic li-proxy-event --zookeeper sandbox:2181






Rebuild your jar, redeploy it to `logisland/lib` dir and launch a mapper job in the Docker container :

Each event will be sent to Elasticsearch by bulk. 

.. code-block:: sh

    $LOGISLAND_HOME/bin/event-indexer \
        --kafka-brokers sandbox:9092 \
        --es-host sandbox \
        --index-name li-apache \
        --input-topics li-apache-event \
        --max-rate-per-partition 10000 \
        --event-mapper com.hurence.logisland.plugin.apache.ProxyEventMapper


Open up your browser and go to [http://sandbox:5601/](http://sandbox:5601/). Enjoy !



checkout the code of this tutorial here [https://github.com/Hurence/logisland-plugin-template.git](https://github.com/Hurence/logisland-plugin-template.git)