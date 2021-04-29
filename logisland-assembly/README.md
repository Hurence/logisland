Logisland Assembly
==================

Build assemblies for logisland
------------------------------

Run this command at **logisland ROOT project**, not in this sub module otherwise assemblies will miss jars :

.. code-block:: sh

    mvn clean package -DskipTests
    
We would expect that when running below command in root pom. 
The assemblies are correctly built but unfortunately this not the case.
Results seems to depend on maven command used.
With this command core library dependencies are not included...

.. code-block:: sh

    mvn clean install -Pintegration-tests


Different Type of assemblies
----------------------------

The descriptors are located in src/assembly.

At the moment we build three differents version of assembly for logisland

Bare metal
++++++++++

This is built as :
* logisland-1.1.2-bare-metal-bin
* logisland-1.1.2-bare-metal-bin.tar.gz

This contain only framework dependencies and engines.
There is no services or processors included so you will have to install them manually with bin/component.sh script


Light
+++++

This is built as :
* logisland-1.1.2-light-bin
* logisland-1.1.2-light-bin.tar.gz

This contain only framework dependencies and engines.
There is also a few processors from logisland-processor-commons module.
For other services or processors you will have to install them manually with bin/component.sh script

full
++++

This is built as :
* logisland-1.1.2-full-bin
* logisland-1.1.2-full-bin.tar.gz

This contain all built-in dependencies. With this packaging every logisland services and processors are already installed.
You can install third party components with bin/component.sh script if necessary.


Explanation of our assembly
---------------------------

It is composed of three type of jar.
The one in lib/core/*, they are jars shared by all logisland processes.
The one in lib/engine/*, they are different engine available deding on where you are running logisland.
The one in lib/plugins/*, It contains all logisland plugins. They are fat jars that will be run in different independant ClassLaoder.

The fat jar are the target of a special repackaging, that's why they are suffixed with "-repackaged".


Current Architecture of the assembly
------------------------------------

Here the result of current "find ./target/logisland-*-full-bin" :

```
./target/logisland-1.4.0-full-bin
./target/logisland-1.4.0-full-bin/logisland-1.4.0
./target/logisland-1.4.0-full-bin/logisland-1.4.0/NOTICE
./target/logisland-1.4.0-full-bin/logisland-1.4.0/bin
./target/logisland-1.4.0-full-bin/logisland-1.4.0/bin/logisland.sh
./target/logisland-1.4.0-full-bin/logisland-1.4.0/bin/components.sh
./target/logisland-1.4.0-full-bin/logisland-1.4.0/monitoring
./target/logisland-1.4.0-full-bin/logisland-1.4.0/monitoring/jmx_prometheus_javaagent-0.10.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/monitoring/metrics.properties
./target/logisland-1.4.0-full-bin/logisland-1.4.0/monitoring/spark-prometheus.yml
./target/logisland-1.4.0-full-bin/logisland-1.4.0/README.md
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/Financial Sample.xlsx
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/docker-compose-index-apache-logs-es.yml
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/docker-compose-index-apache-logs-solr.yml
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/enrich-apache-logs.yml
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/es-template.json
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/ivy.xml
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/logisland.properties
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/index-bro-events.yml
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/docker-compose.yml
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/outlier-detection.yml
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/match-queries.yml
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/index-blockchain-transactions.yml
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/save-to-hdfs.yml
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/future-factory-indexer.yml
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/connect-avro-standalone.properties
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/index-timeseries-solr.yml
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/job-sample.json
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/index-network-packets.yml
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/index-apache-logs-solr.yml
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/opc-iiot.yml
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/mqtt-to-historian.yml
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/store-to-redis.yml
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/retrieve-data-from-elasticsearch.yml
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/index-apache-logs-plainjava.yml
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/index-excel-spreadsheet.yml
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/send-apache-logs-to-hbase.yml
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/configuration-template.yml
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/connect-avro-distributed.properties
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/python-processing.yml
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/log4j-debug.properties
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/aggregate-events.yml
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/threshold-alerting.yml
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/bootstrap.conf
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/docker-compose-index-apache-logs-mongo.yml
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/index-apache-logs-es.yml
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/index-apache-logs-mongo.yml
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/logback.xml
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/timeseries-lookup.csv
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/index-jms-messages.yml
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/log4j.properties
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/index-netflow-events.yml
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/topic-sample.json
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/timeseries-parsing.yml
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/logisland-kafka-connect.yml
./target/logisland-1.4.0-full-bin/logisland-1.4.0/conf/docker-compose-opc-iiot.yml
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/plugins
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/plugins/logisland-connector-opc-1.4.0-repackaged.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/plugins/logisland-processor-xml-1.4.0-repackaged.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/plugins/logisland-processor-web-analytics-1.4.0-repackaged.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/plugins/logisland-service-solr_8-client-1.4.0-repackaged.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/plugins/logisland-service-hbase_1_1_2-client-1.4.0-repackaged.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/plugins/logisland-service-elasticsearch_7_x-client-1.4.0-repackaged.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/plugins/logisland-service-ip-to-geo-maxmind-1.4.0-repackaged.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/plugins/logisland-service-redis-1.4.0-repackaged.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/plugins/logisland-service-solr_6_6_2-client-1.4.0-repackaged.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/plugins/logisland-service-cassandra-client-1.4.0-repackaged.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/plugins/logisland-processor-hbase-1.4.0-repackaged.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/plugins/logisland-processor-excel-1.4.0-repackaged.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/plugins/logisland-processor-scripting-1.4.0-repackaged.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/plugins/logisland-service-solr_chronix_8-client-1.4.0-repackaged.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/plugins/logisland-service-rest-1.4.0-repackaged.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/plugins/logisland-service-elasticsearch_6_6_2-client-1.4.0-repackaged.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/plugins/logisland-processor-cyber-security-1.4.0-repackaged.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/plugins/logisland-processor-outlier-detection-1.4.0-repackaged.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/plugins/logisland-processor-useragent-1.4.0-repackaged.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/plugins/logisland-processor-rest-1.4.0-repackaged.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/plugins/logisland-service-influxdb-client-1.4.0-repackaged.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/plugins/logisland-processor-elasticsearch-1.4.0-repackaged.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/plugins/logisland-service-elasticsearch_2_4_0-client-1.4.0-repackaged.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/plugins/logisland-connector-spooldir-1.4.0-repackaged.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/plugins/logisland-service-elasticsearch_5_4_0-client-1.2.0.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/plugins/logisland-processor-enrichment-1.4.0-repackaged.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/plugins/logisland-service-mongodb-client-1.4.0-repackaged.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/plugins/logisland-service-inmemory-cache-1.4.0-repackaged.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/plugins/logisland-processor-common-1.4.0-repackaged.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/plugins/logisland-processor-querymatcher-1.4.0-repackaged.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/logisland-scripting-mvel-1.4.0.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/slf4j-api-1.7.16.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/commons-cli-1.2.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/joda-time-2.8.2.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/json-20090211.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/json-simple-1.1.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/cglib-nodep-3.2.10.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/spring-boot-loader-2.0.0.RELEASE.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/commons-collections-3.2.1.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/asm-4.0.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/objenesis-2.6.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/jackson-module-jsonSchema-2.10.3.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/minlog-1.2.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/junit-platform-commons-1.5.2.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/jackson-core-2.10.3.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/logisland-plugin-support-1.4.0.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/commons-digester-1.8.1.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/spring-core-5.0.4.RELEASE.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/commons-io-2.4.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/validation-api-1.1.0.Final.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/log4j-to-slf4j-2.10.0.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/ivy-2.2.0.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/jackson-annotations-2.10.3.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/log4j-api-2.10.0.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/opentest4j-1.2.0.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/commons-logging-1.2.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/mvel2-2.3.1.Final.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/hamcrest-core-1.3.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/logisland-api-1.4.0.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/avro-1.9.2.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/protobuf-java-2.5.0.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/logisland-bootstrap-1.4.0.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/spring-jcl-5.0.4.RELEASE.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/objenesis-1.2.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/apiguardian-api-1.1.0.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/jline-0.9.94.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/netty-3.7.0.Final.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/zookeeper-3.4.6.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/commons-codec-1.10.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/commons-compress-1.19.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/snakeyaml-1.24.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/spring-boot-loader-tools-2.0.0.RELEASE.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/junit-4.12.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/commons-validator-1.6.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/junit-vintage-engine-5.5.2.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/logisland-scripting-base-1.4.0.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/junit-platform-engine-1.5.2.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/commons-beanutils-1.9.2.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/commons-compress-1.14.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/kryo-2.21.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/reflectasm-1.07-shaded.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/jackson-databind-2.10.3.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/scala-logging-api_2.10-2.1.2.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/scala-logging-slf4j_2.10-2.1.2.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/logisland-utils-1.4.0.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/zkclient-0.8.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/jackson-dataformat-yaml-2.10.3.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/cores/commons-lang3-3.8.1.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/engines
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/engines/logisland-engine-spark_1_6-1.4.0.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/engines/logisland-engine-spark_2_4-1.4.0.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/engines/logisland-engine-spark_2_1-1.4.0.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/engines/logisland-engine-vanilla-1.4.0.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/lib/engines/logisland-engine-spark_2_3-1.4.0.jar
./target/logisland-1.4.0-full-bin/logisland-1.4.0/LICENSE
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/architecture.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/intro.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/components.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/user
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/user/dynamic-properties.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/user/expression-language.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/user/index.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/user/components
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/user/components/other-processors.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/user/components/engines
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/user/components/engines/index.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/user/components/engines/engine-spark.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/user/components/engines/engine-vanilla.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/user/components/services.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/user/components/index.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/user/components/common-processors.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/rest-api.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/concepts.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/monitoring.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/plugins.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/api.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/faq.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/kibana-blacklisted-host.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/data-driven-computing.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/nifi-drag-template.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/lambda-logicalArchi.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/solr-query.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/kibana-configure-index-netflow.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/solr-dashboard.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/spark-rdd.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/nifi-template-dialog.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/chronix-record.jpeg
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/kibana-logisland-metrics.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/kibana-logisland-import-dashboard.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/apple-touch-icon-144-precomposed.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/kibana-blockchain-dashboard.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/kafka-design.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/nifi-flow.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/lambda-logCentric.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/spark-architecture.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/kibana-logisland-metrics-packet-stream-pycapa.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/kibana-excel-logs.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/kibana-configure-index-packet.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/kibana-logisland-aggregates-events.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/data-pyramid-mccandless.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/kibana-save-search.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/api.yaml
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/sparkcontext-broadcast-executors.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/kibana-explore.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/kibana-connection-alerts.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/hurence-logo.jpeg
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/kibana-logisland-dashboard.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/kibana-configure-index.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/kibana-match-queries.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/logisland_api_flows.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/activemq-create-queue.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/features.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/nifi_netflow.xml
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/traces.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/favicon.ico
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/spark-job-monitoring.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/netflow_dashboard.json
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/kibana-apache-logs.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/logIsland-architecture.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/logIsland-opc.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/kibana-threshold-alerts.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/logcentric.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/kafka-mgr.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/kibana-logisland-metrics-netflow.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/activemq-send-message.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/data-to-knowldege.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/logisland-workflow.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/spark-streaming.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/es-head.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/kibana-blockchain-records.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/spark-streaming-packet-capture-job.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/kafka-usecase.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/_static/kibana-jms-records.png
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/changes.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/developer
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/developer/developer.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/developer/index.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/README.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/workflow.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/tutorials
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/tutorials/index-apache-logs-solr.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/tutorials/iiot-opc-ua.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/tutorials/integrate-kafka-connect.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/tutorials/indexing-network-packets.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/tutorials/enrich-apache-logs.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/tutorials/outlier-detection.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/tutorials/index-apache-logs-es.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/tutorials/index-apache-logs.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/tutorials/index-excel-spreadsheet.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/tutorials/indexing-netflow-events.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/tutorials/index-jdbc-messages.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/tutorials/kubernetes1.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/tutorials/generate_unique_ids.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/tutorials/index-blockchain-transactions.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/tutorials/threshold-alerting.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/tutorials/index.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/tutorials/index-apache-logs-mongo.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/tutorials/match-queries.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/tutorials/mqtt-to-historian.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/tutorials/aggregate-events.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/tutorials/index-jms-messages.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/tutorials/indexing-bro-events.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/tutorials/prerequisites.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/tutorials/store-to-redis.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/index.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/plugins_old.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/release.rst
./target/logisland-1.4.0-full-bin/logisland-1.4.0/docs/connectors.rst
```

Compare different assemblies
----------------------------

There is a small script that can be used to compare two assembly if you are working on it "compare-assembly.sh"

Build the assembly, move ther "target" folder in "old-target" for exemple. Then rebuild the assembly with your modification.

Then just run :

```bash
bash compare-assembly.sh ./old-target ./target
```

it will show you the diff, each output are in "assembly.txt" and "assembly2.txt".

If you want more detail you can use intelliJ to compare content of two jars.





