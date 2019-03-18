Index Apache logs Enrichment
============================

In the following tutorial we'll drive you through the process of enriching Apache logs with LogIsland platform.

One of the first steps when treating web access logs is to extract information from the User-Agent header string, in order to be able to classify traffic.
The User-Agent string is part of the access logs from the web server (this is the last field in the example below).

Another step is to find the FQDN (full qualified domain name) from an ip address.

.. code:

      127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326 "http://www.example.com/start.html" "Mozilla/4.08 [en] (Win98; I ;Nav)"

That string is packed with information from the visitor, when you know how to interpret it. However, the User-Agent string is not based on any standard, and it is not trivial to extract meaningful information from it.
LogIsland provides a processor, based on the `YAUAA library <http://github.com/nielsbasjes/yauaa>`_, that simplifies that treatement.

LogIsland provides a processor, based on `InetAdress class from JDK 8 <https://docs.oracle.com/javase/8/docs/api/java/net/InetAddress.html>`_, that use reverse Dns to determine FQDN from an IP.

.. note::

    This class find FQDN from ip using IN-ADDR.ARPA (or IP6.ARPA for ipv6). If it finds a domain name, it verifies that it matches back the same address ip in order to prevent against `IP spoofing attack <https://en.wikipedia.org/wiki/IP_address_spoofing>`_.
    If you want to return the ip anyway, you should implement a new plugin using another library as dnsjava for example or open an issue for asking this feature.

We will reuse the Docker container hosting all the LogIsland services from the `previous tutorial <index-apache-logs.html>`__, and add the User-Agent as well as the IpToFqdn processor to the stream


.. note::

    You can download the `latest release <https://github.com/Hurence/logisland/releases>`_ of logisland and the `YAML configuration file <https://github.com/Hurence/logisland/blob/master/logisland-framework/logisland-resources/src/main/resources/conf/user-agent-logs.yml>`_ for this tutorial which can be also found under `$LOGISLAND_HOME/conf` directory.


1. Start LogIsland as a Docker container
----------------------------------------
LogIsland is packaged as a Docker container that you can build yourself or pull from Docker Hub.

You can find the steps to start the Docker image and start the LogIsland server in the `previous tutorial <index-apache-logs.html>`__.
However, you'll start the server with a different configuration file (that already includes the necessary modifications)



Install required components
___________________________

For this tutorial please make sure to already have installed required modules.

If not you can just do it through the components.sh command line:

.. code-block:: sh

    bin/components.sh -i com.hurence.logisland:logisland-processor-elasticsearch:1.0.0-RC2

    bin/components.sh -i com.hurence.logisland:logisland-service-elasticsearch_2_4_0-client:1.0.0-RC2

    bin/components.sh -i com.hurence.logisland:logisland-processor-enrichment:1.0.0-RC2

    bin/components.sh -i com.hurence.logisland:logisland-processor-useragent:1.0.0-RC2





Stream 1 : modify the stream to analyze the User-Agent string
_____________________________________________________________

.. note::

    You can either apply the modifications from this section to the file *conf/index-apache-logs.yml* ot directly use the file *conf/enrich-apache-logs.yml* that already includes them.

The stream needs to be modified to ::

* modify the regex to add the referer and the User-Agent strings for the SplitText processor
* modify the Avro schema to include the new fields returned by the UserAgentProcessor
* include the processing of the User-Agent string after the parsing of the logs
* include the processor IpToFqdn after the ParserUserAgent
* include a cache service to use with IpToFqdn processor

The example below shows how to include all of the fields supported by the processor.

.. note::

    It is possible to remove unwanted fields from both the processor configuration and the Avro schema


.. code-block:: yaml
  controllerServiceConfigurations:

    - controllerService: lru_cache_service
      component: com.hurence.logisland.service.cache.LRUKeyValueCacheService
      type: service
      documentation: cache service implementation using LinkedHashMap (Least Recent Used)
      configuration:
        cache.size: 16384

  streamConfigurations:
    # parsing
    - stream: parsing_stream
      component: com.hurence.logisland.stream.spark.KafkaRecordStreamParallelProcessing
      type: stream
      documentation: a processor that links
      configuration:
        kafka.input.topics: logisland_raw
        kafka.output.topics: logisland_events
        kafka.error.topics: logisland_errors
        kafka.input.topics.serializer: none
        kafka.output.topics.serializer: com.hurence.logisland.serializer.KryoSerializer
        kafka.error.topics.serializer: com.hurence.logisland.serializer.JsonSerializer
        avro.output.schema: >
          {  "version":1,
             "type": "record",
             "name": "com.hurence.logisland.record.apache_log",
             "fields": [
               { "name": "record_errors",   "type": [ {"type": "array", "items": "string"},"null"] },
               { "name": "record_raw_key", "type": ["string","null"] },
               { "name": "record_raw_value", "type": ["string","null"] },
               { "name": "record_id",   "type": ["string"] },
               { "name": "record_time", "type": ["long"] },
               { "name": "record_type", "type": ["string"] },
               { "name": "src_ip",      "type": ["string","null"] },
               { "name": "http_method", "type": ["string","null"] },
               { "name": "bytes_out",   "type": ["long","null"] },
               { "name": "http_query",  "type": ["string","null"] },
               { "name": "http_version","type": ["string","null"] },
               { "name": "http_status", "type": ["string","null"] },
               { "name": "identd",      "type": ["string","null"] },
               { "name": "user",        "type": ["string","null"] } ,
               { "name": "http_user_agent",  "type": ["string","null"] },
               { "name": "http_referer",     "type": ["string","null"] },
               { "name": "DeviceClass",  "type": ["string","null"] },
               { "name": "DeviceName",  "type": ["string","null"] },
               { "name": "DeviceBrand",  "type": ["string","null"] },
               { "name": "DeviceCpu",  "type": ["string","null"] },
               { "name": "DeviceFirmwareVersion",  "type": ["string","null"] },
               { "name": "DeviceVersion",  "type": ["string","null"] },
               { "name": "OperatingSystemClass",  "type": ["string","null"] },
               { "name": "OperatingSystemName",  "type": ["string","null"] },
               { "name": "OperatingSystemVersion",  "type": ["string","null"] },
               { "name": "OperatingSystemNameVersion",  "type": ["string","null"] },
               { "name": "OperatingSystemVersionBuild",  "type": ["string","null"] },
               { "name": "LayoutEngineClass",  "type": ["string","null"] },
               { "name": "LayoutEngineName",  "type": ["string","null"] },
               { "name": "LayoutEngineVersion",  "type": ["string","null"] },
               { "name": "LayoutEngineVersionMajor",  "type": ["string","null"] },
               { "name": "LayoutEngineNameVersion",  "type": ["string","null"] },
               { "name": "LayoutEngineNameVersionMajor",  "type": ["string","null"] },
               { "name": "LayoutEngineBuild",  "type": ["string","null"] },
               { "name": "AgentClass",  "type": ["string","null"] },
               { "name": "AgentName",  "type": ["string","null"] },
               { "name": "AgentVersion",  "type": ["string","null"] },
               { "name": "AgentVersionMajor",  "type": ["string","null"] },
               { "name": "AgentNameVersion",  "type": ["string","null"] },
               { "name": "AgentNameVersionMajor",  "type": ["string","null"] },
               { "name": "AgentBuild",  "type": ["string","null"] },
               { "name": "AgentLanguage",  "type": ["string","null"] },
               { "name": "AgentLanguageCode",  "type": ["string","null"] },
               { "name": "AgentInformationEmail",  "type": ["string","null"] },
               { "name": "AgentInformationUrl",  "type": ["string","null"] },
               { "name": "AgentSecurity",  "type": ["string","null"] },
               { "name": "AgentUuid",  "type": ["string","null"] },
               { "name": "FacebookCarrier",  "type": ["string","null"] },
               { "name": "FacebookDeviceClass",  "type": ["string","null"] },
               { "name": "FacebookDeviceName",  "type": ["string","null"] },
               { "name": "FacebookDeviceVersion",  "type": ["string","null"] },
               { "name": "FacebookFBOP",  "type": ["string","null"] },
               { "name": "FacebookFBSS",  "type": ["string","null"] },
               { "name": "FacebookOperatingSystemName",  "type": ["string","null"] },
               { "name": "FacebookOperatingSystemVersion",  "type": ["string","null"] },
               { "name": "Anonymized",  "type": ["string","null"] },
               { "name": "HackerAttackVector",  "type": ["string","null"] },
               { "name": "HackerToolkit",  "type": ["string","null"] },
               { "name": "KoboAffiliate",  "type": ["string","null"] },
               { "name": "KoboPlatformId",  "type": ["string","null"] },
               { "name": "IECompatibilityVersion",  "type": ["string","null"] },
               { "name": "IECompatibilityVersionMajor",  "type": ["string","null"] },
               { "name": "IECompatibilityNameVersion",  "type": ["string","null"] },
               { "name": "IECompatibilityNameVersionMajor",  "type": ["string","null"] },
               { "name": "Carrier",  "type": ["string","null"] },
               { "name": "GSAInstallationID",  "type": ["string","null"] },
               { "name": "WebviewAppName",  "type": ["string","null"] },
               { "name": "WebviewAppNameVersionMajor",  "type": ["string","null"] },
               { "name": "WebviewAppVersion",  "type": ["string","null"] },
               { "name": "WebviewAppVersionMajor",  "type": ["string","null"]} ]}
        kafka.metadata.broker.list: sandbox:9092
        kafka.zookeeper.quorum: sandbox:2181
        kafka.topic.autoCreate: true
        kafka.topic.default.partitions: 4
        kafka.topic.default.replicationFactor: 1
      processorConfigurations:

        # parse apache logs
        - processor: apache_parser
          component: com.hurence.logisland.processor.SplitText
          type: parser
          documentation: a parser that produce events from an apache log REGEX
          configuration:
            record.type: apache_log
            # Previous regex
            #value.regex: (\S+)\s+(\S+)\s+(\S+)\s+\[([\w:\/]+\s[+\-]\d{4})\]\s+"(\S+)\s+(\S+)\s*(\S*)"\s+(\S+)\s+(\S+)
            #value.fields: src_ip,identd,user,record_time,http_method,http_query,http_version,http_status,bytes_out
            # Updated regex
            value.regex: (\S+)\s+(\S+)\s+(\S+)\s+\[([\w:\/]+\s[+\-]\d{4})\]\s+"(\S+)\s+(\S+)\s*(\S*)"\s+(\S+)\s+(\S+)\s+"(\S+)"\s+"([^\"]+)"
            value.fields: src_ip,identd,user,record_time,http_method,http_query,http_version,http_status,bytes_out,http_referer,http_user_agent

        - processor: user_agent_analyzer
          component: com.hurence.logisland.processor.useragent.ParseUserAgent
          type: processor
          documentation: decompose the user_agent field into meaningful attributes
          configuration:
            useragent.field: http_user_agent
            fields: DeviceClass,DeviceName,DeviceBrand,DeviceCpu,DeviceFirmwareVersion,DeviceVersion,OperatingSystemClass,OperatingSystemName,OperatingSystemVersion,OperatingSystemNameVersion,OperatingSystemVersionBuild,LayoutEngineClass,LayoutEngineName,LayoutEngineVersion,LayoutEngineVersionMajor,LayoutEngineNameVersion,LayoutEngineNameVersionMajor,LayoutEngineBuild,AgentClass,AgentName,AgentVersion,AgentVersionMajor,AgentNameVersion,AgentNameVersionMajor,AgentBuild,AgentLanguage,AgentLanguageCode,AgentInformationEmail,AgentInformationUrl,AgentSecurity,AgentUuid,FacebookCarrier,FacebookDeviceClass,FacebookDeviceName,FacebookDeviceVersion,FacebookFBOP,FacebookFBSS,FacebookOperatingSystemName,FacebookOperatingSystemVersion,Anonymized,HackerAttackVector,HackerToolkit,KoboAffiliate,KoboPlatformId,IECompatibilityVersion,IECompatibilityVersionMajor,IECompatibilityNameVersion,IECompatibilityNameVersionMajor,GSAInstallationID,WebviewAppName,WebviewAppNameVersionMajor,WebviewAppVersion,WebviewAppVersionMajor

        - processor: ipToFqdn
          component: com.hurence.logisland.processor.enrichment.IpToFqdn
          type: processor
          documentation: find full qualified domain name correponding to an ip using reverse Dns.
          configuration:
            ip.address.field: src_ip
            fqdn.field: src_ip
            override.fqdn.field: true
            cache.service: lru_cache_service


Once the configuration file is updated, LogIsland must be restarted with that new configuration file.

.. code-block:: sh

    bin/logisland.sh --conf <new_configuration_file>




2. Inject some Apache logs into the system
------------------------------------------



Now we're going to send some logs to ``logisland_raw`` Kafka topic.

We could setup a logstash or flume agent to load some apache logs into a kafka topic
but there's a super useful tool in the Kafka ecosystem : `kafkacat <https://github.com/edenhill/kafkacat>`_,
a *generic command line non-JVM Apache Kafka producer and consumer* which can be easily installed (and is already present in the docker image).


If you don't have your own httpd logs available, you can use some freely available log files from
`Elastic <https://raw.githubusercontent.com/elastic/examples/master/ElasticStack_apache/apache_logs>`_ web site

Let's send the first 500000 lines of access log to LogIsland with kafkacat to ``logisland_raw`` Kafka topic

.. code-block:: sh

    docker exec -ti logisland bash
    cd /tmp
    wget https://raw.githubusercontent.com/elastic/examples/master/ElasticStack_apache/apache_logs
    head -500000 apache_logs | kafkacat -b sandbox:9092 -t logisland_raw

.. note::

    The process should last around 280 seconds because reverse dns is a costly operation.
    After all data are processed, you can inject the same logs again and it should be very fast to process thanks to the cache that saved all matched ip.

3. Monitor your spark jobs and Kafka topics
-------------------------------------------
Now go to `http://sandbox:4050/streaming/ <http://sandbox:4050/streaming/>`_ to see how fast Spark can process
your data

.. image:: /_static/spark-job-monitoring.png


Another tool can help you to tweak and monitor your processing `http://sandbox:9000/ <http://sandbox:9000>`_

.. image:: /_static/kafka-mgr.png


4. Use Kibana to inspect the logs
---------------------------------

You've completed the enrichment of your logs using the User-Agent processor.
The logs are now loaded into elasticSearch, in the following form :

.. code-block:: sh

    curl -XGET http://localhost:9200/logisland.*/_search?pretty

.. code-block:: json

    {

        "_index": "logisland.2017.03.21",
        "_type": "apache_log",
        "_id": "4ca6a8b5-1a60-421e-9ae9-6c30330e497e",
        "_score": 1.0,
        "_source": {
            "@timestamp": "2015-05-17T10:05:43Z",
            "agentbuild": "Unknown",
            "agentclass": "Browser",
            "agentinformationemail": "Unknown",
            "agentinformationurl": "Unknown",
            "agentlanguage": "Unknown",
            "agentlanguagecode": "Unknown",
            "agentname": "Chrome",
            "agentnameversion": "Chrome 32.0.1700.77",
            "agentnameversionmajor": "Chrome 32",
            "agentsecurity": "Unknown",
            "agentuuid": "Unknown",
            "agentversion": "32.0.1700.77",
            "agentversionmajor": "32",
            "anonymized": "Unknown",
            "devicebrand": "Apple",
            "deviceclass": "Desktop",
            "devicecpu": "Intel",
            "devicefirmwareversion": "Unknown",
            "devicename": "Apple Macintosh",
            "deviceversion": "Unknown",
            "facebookcarrier": "Unknown",
            "facebookdeviceclass": "Unknown",
            "facebookdevicename": "Unknown",
            "facebookdeviceversion": "Unknown",
            "facebookfbop": "Unknown",
            "facebookfbss": "Unknown",
            "facebookoperatingsystemname": "Unknown",
            "facebookoperatingsystemversion": "Unknown",
            "gsainstallationid": "Unknown",
            "hackerattackvector": "Unknown",
            "hackertoolkit": "Unknown",
            "iecompatibilitynameversion": "Unknown",
            "iecompatibilitynameversionmajor": "Unknown",
            "iecompatibilityversion": "Unknown",
            "iecompatibilityversionmajor": "Unknown",
            "koboaffiliate": "Unknown",
            "koboplatformid": "Unknown",
            "layoutenginebuild": "Unknown",
            "layoutengineclass": "Browser",
            "layoutenginename": "Blink",
            "layoutenginenameversion": "Blink 32.0",
            "layoutenginenameversionmajor": "Blink 32",
            "layoutengineversion": "32.0",
            "layoutengineversionmajor": "32",
            "operatingsystemclass": "Desktop",
            "operatingsystemname": "Mac OS X",
            "operatingsystemnameversion": "Mac OS X 10.9.1",
            "operatingsystemversion": "10.9.1",
            "operatingsystemversionbuild": "Unknown",
            "webviewappname": "Unknown",
            "webviewappnameversionmajor": "Unknown",
            "webviewappversion": "Unknown",
            "webviewappversionmajor": "Unknown",
            "bytes_out": 171717,
            "http_method": "GET",
            "http_query": "/presentations/logstash-monitorama-2013/images/kibana-dashboard3.png",
            "http_referer": "http://semicomplete.com/presentations/logstash-monitorama-2013/",
            "http_status": "200",
            "http_user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.77 Safari/537.36",
            "http_version": "HTTP/1.1",
            "identd": "-",
            "record_id": "4ca6a8b5-1a60-421e-9ae9-6c30330e497e",
            "record_raw_value": "83.149.9.216 - - [17/May/2015:10:05:43 +0000] \"GET /presentations/logstash-monitorama-2013/images/kibana-dashboard3.png HTTP/1.1\" 200 171717 \"http://semicomplete.com/presentations/logstash-monitorama-2013/\" \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.77 Safari/537.36\"",
            "record_time": 1431857143000,
            "record_type": "apache_log",
            "src_ip": "83.149.9.216",
            "user": "-"
        }
    }


You can now browse your data in Kibana and build great dashboards





