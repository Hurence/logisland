Log Island Roadmap and future work
====

follow the roadmap through `github issues <https://github.com/Hurence/logisland/issues>`_ too

GUI
----

- manage visualy the streams
- search kafka topics

Engine
----

- Add KafkaStreamEngine
- Add autoscaler component
- move offsets management from Zookeeper to Kafka
- whole integration test framework (file => kafka topic => process stream => es => query)

Components
----

- add EventField mutator based on EL
- add an HDFS bulk loader
- add a generic parser that infers a Regexp from a list (Streaming Deep Learning)




