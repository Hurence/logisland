Log Island Roadmap and future work




## API

- Migrate LogParser to LogProcessor Interface
- move event collection into the ProcessContext
- add a process group component that works globally on an input/output topic and pipe in-memory contexts into sub-processors
- add reporting metrics to know where are exactly the processors on the topics

## GUI

- manage visualy the streams
- search kafka topics

## Engine

- Add KafkaStreamEngine
- Add autoscaler component
- manage zookeeper offsets
- whole integration test framework (file => kafka topic => process stream => es => query)

## Components

- add EventField mutator based on EL
- add an HDFS bulk loader
- add a generic parser that infers a Regexp from a list (Streaming Deep Learning)

## Nifi

- a component ExtractTextFromAttribute
- a FlowFile splitter based on REGEX delimiter


