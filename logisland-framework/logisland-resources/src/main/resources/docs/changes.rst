What's new in logisland ?
=========================



v0.13.0
-------

- add support for SOLR
- add support for Chronix timeseries
- review Datastore API
- fix matchquery update field policy issue
- remove elasticsearch 2.3 support


v0.10.0
-------

- add kibana pcap panel cyber-security feature gui #187
- add support for elasticsearch 2.4 feature processor
- add support for elasticsearch 5 feature processor #214
- fix pb in kafkaStreamProcessingEngine (2.1) #244
- allow to set a default profile during build #271
- add ElasticSearch Service feature framework #241
- add multiGet elastic search processor feature processor #255
- fix Pcap telemetry processor issue #180 #224
- Make build work if no profile specified (use the highest hdp one) build #210
- implement Logisland agent #201
- fix travis build randomly fails on travis CI (spark-engine module tests) bug framework #159
- support maven profiles to handle dépendencies (hdp 2.4 & hdp 2.5) #116
- add a RESTful API for components live update agent feature framework #42
- add a logisland agent agent enhancement feature framework #117
- add a Topic metadata view feature gui #101
- add scheduler view feature framework gui #103
- add job configuration view feature gui #94
- add a global logisland.properties agent feature #71
- add a Topic metadata registry feature framework
- integrate BRO files & notification through a BroProcessor feature processor security #93
- add Support for SMTP/Mailer Processor feature processor security #138
- add a Release/deployment documentation #108
- Ensure source files have a licence header
- add HBase service to get and scan records
- add Multiget elasticsearch enricher processor
- add sessionization processor
- improve topic management in web ui gui #222
- Docker images shall be builded automatically framework #200
- fix classpath issue bug framework #247
- add Netflow telemetry Processor cyber-security feature processor #181
- add an "How to contribute page" documentation #183
- fix PutElasticsearch throws UnsupportedOperationException when duplicate document is found bug processor #221
- Feature/maven docker#200  enhancement framework #242
- Feature/partitioner  enhancement framework #238
- add PCAP telemetry Processor cyber-security feature processor #180
- Move Mailer Processor into commons plugins build #196
- Origin/webanalytics  framework processor web-analytics #236
- rename Plugins to Processors in online documentation documentation #173


v0.9.8
------
- add a retry parameter to PutElasticsearch bug enhancement processor #124
- add Timezone managmt to SplitText enhancement processor #126
- add IdempotentId processor enhancement feature processor #127
- migrate to Kafka 0.9 enhancement



v0.9.7
------

- add HDFS burner feature processor #89
- add ExtractJsonPath processor  #90
- check compatibility with HDP 2.5 #112
- sometimes the drivers fails with status SUCCEEDED which prevents YARN to resubmit the job automatically #105
- logisland crashes when starting with wrong offsets #111
- add type checking for SplitText component enhancement #46
- add optional regex to SplitText #106
- add record schema management with ConvertFieldsType processor #75
- add field auto extractor processor : SplitTextWithProperties #49
- add a new RemoveFields processor
- add a NormalizeFields processor #88
- Add notion of asserting the asserted fields in MockRecord


v0.9.6
------

- add a Documentation generator for plugins feature #69
- add SQL aggregator plugin feature #74
- #66 merge elasticsearch-shaded and elasticsearch-plugin enhancement
- #73 add metric aggregator processor feature
- #57 add sampling processor enhancement
- #72 integrate OutlierDetection plugin feature
- #34 integrate QueryMatcherProcessor bug


v0.9.5
------

- generify API from Event to Records
- add docker container for demo
- add topic auto-creation parameters
- add Record validators
- add processor chaining that works globally on an input/output topic and pipe in-memory contexts into sub-processors
- better error handling for SplitText
- testRunner API
- migrate LogParser to LogProcessor Interface
- reporting metrics to know where are exactly the processors on the topics
- add an HDFSBurner Engine
- yarn stability improvements
- more spark parameters handling
- driver failover through Zookeper offset checkpointing
- add raw_content to event if regex matching failed in SplitText
- integration testing with embedded Kafka/Spark
- processor chaining
-
