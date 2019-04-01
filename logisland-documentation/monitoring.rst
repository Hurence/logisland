
Monitoring Guide
================

This document summarizes information relevant to LogIsland monitoring.


Concepts & architecture
-----------------------

LogIsland monitoring is based on the couple prometheus/grafana. Prometheus is used to store all metrics coming from all monitored services by polling those services at a regular interval.

The setup is split into 2 parts, one is for metrics instrumentation (system, kafka, zookeeper, hbase) on each node of the cluster and the other is for the configuration of the docker monitoring components.


Metrics in prometheus
+++++++++++++++++++++

https://prometheus.io/

Prometheus fundamentally stores all data as time series: streams of timestamped values belonging to the same metric and the same set of labeled dimensions. Besides stored time series, Prometheus may generate temporary derived time series as the result of queries. Every time series is uniquely identified by its metric name and a set of key-value pairs, also known as labels.

The metric name specifies the general feature of a system that is measured (e.g. http_requests_total - the total number of HTTP requests received). It may contain ASCII letters and digits, as well as underscores and colons. It must match the regex [a-zA-Z_:][a-zA-Z0-9_:]*.   Labels enable Prometheus's dimensional data model: any given combination of labels for the same metric name identifies a particular dimensional instantiation of that metric (for example: all HTTP requests that used the method POST to the /api/tracks handler). The query language allows filtering and aggregation based on these dimensions. Changing any label value, including adding or removing a label, will create a new time series.

- https://prometheus.io/docs/querying/basics/
- https://prometheus.io/docs/querying/operators/
- https://prometheus.io/docs/querying/functions/


Dashboarding with Grafana
+++++++++++++++++++++++++

https://grafana.com/grafana

Grafana is an open source, feature rich metrics dashboard and graph editor for Graphite, Elasticsearch, OpenTSDB, Prometheus and InfluxDB. It is used to graph the prometheus metrics.


- http://docs.grafana.org/guides/getting_started/
- http://docs.grafana.org/guides/basic_concepts/


Step 1 : Cluster setup
----------------------

The following commands should be launched on each node of your cluster handling LogIsland infrastructure services.



System metrics with Node Exporter
+++++++++++++++++++++++++++++++++


https://github.com/prometheus/node_exporter

On each hardware node which runs a LogIsland related service (Zookeeper, Kafka, HBase, Yarn) we want to grab system metrics. Prometheus was developed for the purpose of monitoring web services. In order to monitor the metrics of your linux server, you should install a tool called Node Exporter. Node Exporter, as its name suggests, exports lots of metrics (such as disk I/O statistics, CPU load, memory usage, network statistics, and more) in a format Prometheus understands.

Node exporter can be either installed manually or launched as a Docker container :

Manual mode :
#############

.. code-block:: sh

    # download the latest build of Node Exporter
    cd /opt
    wget https://github.com/prometheus/node_exporter/releases/download/1.1.0/node_exporter-1.1.0.linux-amd64.tar.gz -O /tmp/node_exporter-1.1.0.linux-amd64.tar.gz
    sudo tar -xvzf /tmp/node_exporter-1.1.0.linux-amd64.tar.gz

    # Create a soft link to the node_exporter binary in /usr/bin.
    sudo ln -s /opt/node_exporter /usr/bin

    # Use nano or your favorite text editor to create an Upstart configuration file called node_exporter.conf.

    sudo vim /etc/init/node_exporter.conf

This file should contain the link to the node_exporter executable, and also specify when the executable should be started. Accordingly, add the following code:


.. code-block:: sh

    # Run node_exporter
    start on startup

    script
       /usr/bin/node_exporter
    end script


At this point, Node Exporter is available as a service which can be started using the service command:

.. code-block:: sh

    sudo service node_exporter start

Docker mode :
#############

Node exporter can also be launched as a docker container :

    docker run -d -p 9100:9100 -v "/proc:/host/proc" -v "/sys:/host/sys" -v "/:/rootfs" --net="host" prom/node-exporter -collector.procfs /host/proc -collector.sysfs /host/proc -collector.filesystem.ignored-mount-points "^/(sys|proc|dev|host|etc)($|/)"

Display the metrics :
#####################

After Node Exporter starts, use a browser to view its web interface available at `http://your_server_ip:9100/metrics <http://your_server_ip:9100/metrics>`_ You should see a page with some metrics.


Zookeeper instrumentation
+++++++++++++++++++++++++

We will use the jmx_prometheus_javaagent tool to publish zookeeper metrics on a given port ($ZK_JMX_PORT here). Prometheus will then scrap the metrics here.

Install files
#############

- First download the `jmx_prometheus_javaagent-0.10.jar <https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.10/jmx_prometheus_javaagent-0.10.jar>`_ jar file and copy it on every node of the cluster (for example in /opt/jmx/ folder) :
    wget https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.10/jmx_prometheus_javaagent-0.10.jar

- Then copy the file jmx_zookeeper.yml on every zookeeper node in the cluster (for example in /opt/jmx/ folder)

Set appropriate flags
#####################

Zookeeper must be launched with the following flags

    -javaagent:/opt/jmx/jmx_prometheus_javaagent-0.10.jar=$ZK_JMX_PORT:/opt/jmx/jmx_zookeeper.yml -Dcom.sun.management.jmxremote

These flags can be set in two different ways :

- They can be added in the zookeeper file zkServer.sh using the following command (please make sure to backup the original zkServer.sh file before) :

    sudo sed -i 's|-Dcom.sun.management.jmxremote |-javaagent:/opt/jmx/jmx_prometheus_javaagent-0.10.jar=$ZK_JMX_PORT:/opt/jmx/jmx_zookeeper.yml -Dcom.sun.management.jmxremote |g' zkServer.sh

- If you are using Ambari, you can enrich the ZOOMAIN environment variable in "zookeeper-env template" section as below :

    export ZOOMAIN="-javaagent:/opt/jmx/jmx_prometheus_javaagent-0.10.jar=$ZK_JMX_PORT:/opt/jmx/jmx_zookeeper.yml ${ZOOMAIN}"

Restart services and check metrics
##################################

Restart zookeeper services.
The metrics should be available for each node and reached via <node_host_name_or_IP>:$ZK_JMX_PORT/metrics

Kafka instrumentation
+++++++++++++++++++++

We will use the jmx_prometheus_javaagent tool to publish kafka metrics on a given port ($KAFKA_JMX_PORT here). Prometheus will then scrap the metrics here.

Install files
#############

- First download the `jmx_prometheus_javaagent-0.10.jar <https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.10/jmx_prometheus_javaagent-0.10.jar>`_ jar file and copy it on every node of the cluster if not already done in a previous step (for example in /opt/jmx/ folder) :
    wget https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.10/jmx_prometheus_javaagent-0.10.jar

- Then copy the file jmx_kafka.yml on every kafka node in the cluster (for example in /opt/jmx/ folder)

Set appropriate flags
#####################

In Ambari, enrich the KAFKA_OPTS environment variable in "kafka-env template" section as below :

    export KAFKA_OPTS=" -javaagent:/opt/jmx/jmx_prometheus_javaagent-0.10.jar=$KAFKA_JMX_PORT:/opt/jmx/jmx_kafka.yml "


Restart services and check metrics
##################################

Restart kafka services.
The metrics should be available for each node and reached via <node_host_name_or_IP>:$KAFKA_JMX_PORT/metrics


Spark instrumentation
+++++++++++++++++++++

Spark has a configurable metrics system based on the Dropwizard Metrics Library. This allows users to report Spark metrics to a variety of sinks including HTTP, JMX, and CSV files. The metrics system is configured via a configuration file that Spark expects to be present at $SPARK_HOME/conf/metrics.properties.
A custom file location can be specified via the spark.metrics.conf configuration property. By default, the root namespace used for driver or executor metrics is the value of spark.app.id. However, often times, users want to be able to track the metrics across apps for driver and executors, which is hard to do with application ID (i.e. spark.app.id) since it changes with every invocation of the app. For such use cases, a custom namespace can be specified for metrics reporting using spark.metrics.namespace configuration property.
If, say, users wanted to set the metrics namespace to the name of the application, they can set the spark.metrics.namespace property to a value like ${spark.app.name}. This value is then expanded appropriately by Spark and is used as the root namespace of the metrics system. Non driver and executor metrics are never prefixed with spark.app.id, nor does the spark.metrics.namespace property have any such affect on such metrics.

Spark’s metrics are decoupled into different instances corresponding to Spark components. Within each instance, you can configure a set of sinks to which metrics are reported. The following instances are currently supported:

- master: The Spark standalone master process.
- applications: A component within the master which reports on various applications.
- worker: A Spark standalone worker process.
- executor: A Spark executor.
- driver: The Spark driver process (the process in which your SparkContext is created).
- shuffleService: The Spark shuffle service.
- logisland: all the LogIsland processing




ENABLE SPARK METRICS REPORT TO JMX
----------------------------------
Spark has a configurable metrics system. By default, it doesn’t expose its metrics, but only through the web UI, as mentioned above. To enable exposing metrics as JMX MBeans, you should edit `$SPARK_HOME/conf/metrics.properties` file.

Add (or uncomment) the row:

metrics.properties

*.sink.jmx.class=org.apache.spark.metrics.sink.JmxSink



.. code-block:: sh

    *.sink.jmx.class=org.apache.spark.metrics.sink.JmxSink
    master.source.jvm.class=org.apache.spark.metrics.source.JvmSource
    worker.source.jvm.class=org.apache.spark.metrics.source.JvmSource
    driver.source.jvm.class=org.apache.spark.metrics.source.JvmSource
    executor.source.jvm.class=org.apache.spark.metrics.source.JvmSource




Step 2: Monitoring console setup
--------------------------------

The second part deals with the monitoring tools in the docker compose. Theses software shall be installed in an autonomous VM or linux host, able to access the cluster nodes like a edge node.

All the binaries can be found in th `$LOGISLAND_HOME/monitoring` folder. So get the latest release, extract it on your edge node and `install Docker & docker-compose <https://docs.docker.com/compose/install/>`_ on the edge node (the one that will run the docker compose monitoring stack : prometheus/grafana) as well.

Services ports list
+++++++++++++++++++

Here is a list of arbitrary ports for prometheus data scrapping.
there are many web services by host so that can a good idea to carefully note every port number for each of them and to keep the same ports on each host.

- prometheus :             9090
- grafana :                3000
- elasticsearch-exporter : 9108
- burrow :                 7074
- burrow-exporter :        7075
- kafka-broker :           7071
- zookeeper :              7073
- node-exporter :          9100


Elasticsearch exporter
++++++++++++++++++++++

https://github.com/justwatchcom/elasticsearch_exporter

this tool is used to get metrics from elasticsearch nodes through the REST api and to serve them in the prometheus format

make sure to edit the `$LOGISLAND_HOME/monitoring/.env` file with the correct ES_HOST and ES_PORT values.

Burrow
++++++

Burrow is a monitoring companion for Apache Kafka that provides consumer lag checking as a service without the need for specifying thresholds. It monitors committed offsets for all consumers and calculates the status of those consumers on demand. An HTTP endpoint is provided to request status on demand, as well as provide other Kafka cluster information. There are also configurable notifiers that can send status out via email or HTTP calls to another service.

https://github.com/linkedin/Burrow

additionnal configuration can be set in `$LOGISLAND_HOME/monitoring/burrow/conf/burrow.cfg` but you can leave the default


Configure Prometheus
++++++++++++++++++++

edit `$LOGISLAND_HOME/monitoring/prometheus/conf/prometheus.yml` with the following (according to the previous port number list)


.. code-block:: yaml

    global:
     scrape_interval: 10s
     evaluation_interval: 10s
    scrape_configs:
     - job_name: 'kafka'
       static_configs:
        - targets:
          - KAFKA_BROKER1:7071
          - KAFKA_BROKER2:7071
     - job_name: 'elasticsearch'
       static_configs:
        - targets:
          - ELASTICSEARCH_EXPORTER:9108
     - job_name: 'zookeeper'
       static_configs:
        - targets:
          - ZK_NODE1:7072
          - ZK_NODE2:7072
          - ZK_NODE3:7072
     - job_name: 'burrow'
       static_configs:
        - targets:
          - BURROW:7075
     - job_name: 'logisland'
       static_configs:
        - targets:
          - LOGISLAND_APP1:7076
     - job_name: 'system'
        static_configs:
        - targets:
          - LOGISLAND_APP1:9100


Launch Docker console
+++++++++++++++++++++

Start Docker-compose
####################

Launch all the tools tools (prometheus, burrow, es-exporter, grafana) are packaged into a docker composite bundle.

    cd $LOGISLAND_HOME/monitoring
    docker-compose up -d

Display the metrics in Prometheus
#################################

Once all the containers have started, use a browser to view metrics displayed in Prometheus web interface `http://prometheus_host:9090/graph <http://prometheus_host:9090/graph>`_ .


Grafana
+++++++

Run Grafana as a Docker container
#################################

Grafana can be run as a Docker container (admin password needs to be chosen):

docker run -d -p 3000:3000 -e "GF_SECURITY_ADMIN_PASSWORD=admin_password" -v ~/grafana_db:/var/lib/grafana grafana/grafana

Add Prometheus Datasource
#########################

Go to the Grafana `login page <http://grafana_host:3000/?orgId=1>`_ to login with *admin/admin_password* (feel free to change that).

1. Click on add data source named **logisland_prometheus** of type **Prometheus** with url **http://localhost:9090** and **direct** access.
2. Go to "Dashboards > Import" and import all the json dashboards you'll find under `$LOGISLAND_HOME/monitoring/grafana`

Metrics and alerts
------------------

Elasticsearch alerts
++++++++++++++++++++

.. code-block::

    # calculate filesytem used and free percent
    elasticsearch_filesystem_data_used_percent = 100 * (elasticsearch_filesystem_data_size_bytes - elasticsearch_filesystem_data_free_bytes) / elasticsearch_filesystem_data_size_bytes
    elasticsearch_filesystem_data_free_percent = 100 - elasticsearch_filesystem_data_used_percent

    # alert if too few nodes are running
    ALERT ElasticsearchTooFewNodesRunning
      IF elasticsearch_cluster_health_number_of_node < 3
      FOR 5m
      LABELS {severity="critical"}
      ANNOTATIONS {description="There are only {{$value}} < 3 ElasticSearch nodes running", summary="ElasticSearch running on less than 3 nodes"}

    # alert if heap usage is over 90%
    ALERT ElasticsearchHeapTooHigh
      IF elasticsearch_jvm_memory_used_bytes{area="heap"} / elasticsearch_jvm_memory_max_bytes{area="heap"} > 0.9
      FOR 15m
      LABELS {severity="critical"}
      ANNOTATIONS {description="The heap usage is over 90% for 15m", summary="ElasticSearch node {{$labels.node}} heap usage is high"}
