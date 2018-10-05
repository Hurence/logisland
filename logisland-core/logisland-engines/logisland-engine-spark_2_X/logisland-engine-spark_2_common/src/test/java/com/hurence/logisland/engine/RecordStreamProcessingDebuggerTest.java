/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.engine;

import com.hurence.logisland.component.ComponentFactory;
import com.hurence.logisland.component.ComponentType;
import com.hurence.logisland.config.EngineConfiguration;
import com.hurence.logisland.config.ProcessorConfiguration;
import com.hurence.logisland.config.StreamConfiguration;
import com.hurence.logisland.engine.spark.KafkaStreamProcessingEngine;
import com.hurence.logisland.stream.StreamProperties;
import com.hurence.logisland.stream.spark.KafkaRecordStreamDebugger;
import com.hurence.logisland.stream.spark.KafkaRecordStreamHDFSBurner;
import com.hurence.logisland.stream.spark.KafkaRecordStreamParallelProcessing;
import com.hurence.logisland.stream.spark.KafkaRecordStreamSQLAggregator;
import com.hurence.logisland.util.runner.MockProcessor;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


public class RecordStreamProcessingDebuggerTest {
    private static Logger logger = LoggerFactory.getLogger(RecordStreamProcessingDebuggerTest.class);

    private static final String APACHE_LOG_FIELDS = "src_ip,identd,user,record_time,http_method,http_query,http_version,http_status,bytes_out";
    private static final String APACHE_LOG_REGEX = "(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+\\[([\\w:\\/]+\\s[+\\-]\\d{4})\\]\\s+\"(\\S+)\\s+(\\S+)\\s*(\\S*)\"\\s+(\\S+)\\s+(\\S+)";


    @Test
    @Ignore
    public void remoteTest() {

        logger.info("starting StreamProcessingRunner");

        //  ProcessorConfiguration processorConf = getSplitTextProcessorConfiguration();
        StreamConfiguration chainConf = getSQLStreamConfiguration();
        EngineConfiguration engineConf = getStandardEngineConfiguration();
        engineConf.addPipelineConfigurations(chainConf);
        // chainConf.addProcessorConfiguration(processorConf);


        try {

            // instanciate engine and all the processor from the config
            Optional<EngineContext> engineInstance = ComponentFactory.getEngineContext(engineConf);

            assert engineInstance.isPresent();
            assert engineInstance.get().isValid();

            // start the engine
            EngineContext engineContext = engineInstance.get();
            engineInstance.get().getEngine().start(engineContext);


        } catch (Exception e) {
            logger.error("unable to launch runner : {}", e);
        }


    }

    private EngineConfiguration getStandardEngineConfiguration() {
        Map<String, String> engineProperties = new HashMap<>();
        engineProperties.put(KafkaStreamProcessingEngine.SPARK_APP_NAME().getName(), "testApp");
        engineProperties.put(KafkaStreamProcessingEngine.SPARK_STREAMING_BATCH_DURATION().getName(), "5000");
        engineProperties.put(KafkaStreamProcessingEngine.SPARK_MASTER().getName(), "local[4]");
        engineProperties.put(KafkaStreamProcessingEngine.SPARK_EXECUTOR_CORES().getName(), "4");
        engineProperties.put(KafkaStreamProcessingEngine.SPARK_STREAMING_TIMEOUT().getName(), "-1");

        EngineConfiguration engineConf = new EngineConfiguration();
        engineConf.setComponent(KafkaStreamProcessingEngine.class.getName());
        engineConf.setType(ComponentType.ENGINE.toString());
        engineConf.setConfiguration(engineProperties);
        return engineConf;
    }

    private StreamConfiguration getBurnerStreamConfiguration() {
        Map<String, String> streamProperties = new HashMap<>();
        /*chainProperties.put(AbstractKafkaRecordStream.KAFKA_METADATA_BROKER_LIST().getName(),
                "sd-84190:6667,sd-84191:6667,sd-84192:6667,sd-84196:6667");
        chainProperties.put(AbstractKafkaRecordStream.KAFKA_ZOOKEEPER_QUORUM().getName(),
                "sd-76387:2181,sd-84186:2181,sd-84189:2181");*/
        streamProperties.put(StreamProperties.KAFKA_METADATA_BROKER_LIST().getName(),
                "sandbox:9092");
        streamProperties.put(StreamProperties.KAFKA_ZOOKEEPER_QUORUM().getName(),
                "sandbox:2181");
        streamProperties.put(StreamProperties.INPUT_TOPICS().getName(), "logisland_events");
        streamProperties.put(StreamProperties.OUTPUT_TOPICS().getName(), "none");
        streamProperties.put(StreamProperties.INPUT_SERIALIZER().getName(), StreamProperties.KRYO_SERIALIZER().getValue());
        streamProperties.put(StreamProperties.OUTPUT_SERIALIZER().getName(), StreamProperties.NO_SERIALIZER().getValue());
        streamProperties.put(StreamProperties.KAFKA_TOPIC_DEFAULT_REPLICATION_FACTOR().getName(), "1");
        streamProperties.put(StreamProperties.KAFKA_TOPIC_DEFAULT_PARTITIONS().getName(), "2");

        streamProperties.put(StreamProperties.OUTPUT_FOLDER_PATH().getName(), "data/logisland_events");
        streamProperties.put(StreamProperties.OUTPUT_FORMAT().getName(), "parquet");
        streamProperties.put(StreamProperties.RECORD_TYPE().getName(), "record");

        StreamConfiguration chainConf = new StreamConfiguration();
        chainConf.setComponent(KafkaRecordStreamHDFSBurner.class.getName());
        chainConf.setType(ComponentType.STREAM.toString());
        chainConf.setConfiguration(streamProperties);
        chainConf.setStream("KafkaStream");
        return chainConf;
    }


    private StreamConfiguration getParallelStreamConfiguration() {
        Map<String, String> streamProperties = new HashMap<>();
        streamProperties.put(StreamProperties.KAFKA_METADATA_BROKER_LIST().getName(),
                "sandbox:9092");
        streamProperties.put(StreamProperties.KAFKA_ZOOKEEPER_QUORUM().getName(),
                "sandbox:2181");
        streamProperties.put(StreamProperties.OUTPUT_TOPICS().getName(), "logisland_events");
        streamProperties.put(StreamProperties.INPUT_TOPICS().getName(), "logisland_raw");
        streamProperties.put(StreamProperties.ERROR_TOPICS().getName(), "logisland_errors");
        streamProperties.put(StreamProperties.INPUT_SERIALIZER().getName(),
                StreamProperties.NO_SERIALIZER().getValue());
        streamProperties.put(StreamProperties.OUTPUT_SERIALIZER().getName(),
                StreamProperties.JSON_SERIALIZER().getValue());
        streamProperties.put(StreamProperties.ERROR_SERIALIZER().getName(),
                StreamProperties.JSON_SERIALIZER().getValue());

        streamProperties.put(StreamProperties.AVRO_OUTPUT_SCHEMA().getName(),
                "{  \"version\":1,\n" +
                        "             \"type\": \"record\",\n" +
                        "             \"name\": \"com.hurence.logisland.record.apache_log\",\n" +
                        "             \"fields\": [\n" +
                        "               { \"name\": \"record_raw_value\",   \"type\": [\"string\",\"null\"] },\n" +
                        "               { \"name\": \"record_errors\",   \"type\": [ {\"type\": \"array\", \"items\": \"string\"},\"null\"] },\n" +
                        "               { \"name\": \"record_id\",   \"type\": [\"string\",\"null\"] },\n" +
                        "               { \"name\": \"record_time\", \"type\": [\"long\",\"null\"] },\n" +
                        "               { \"name\": \"record_type\", \"type\": [\"string\",\"null\"] },\n" +
                        "               { \"name\": \"src_ip\",      \"type\": [\"string\",\"null\"] },\n" +
                        "               { \"name\": \"http_method\", \"type\": [\"string\",\"null\"] },\n" +
                        "               { \"name\": \"bytes_out\",   \"type\": [\"long\",\"null\"] },\n" +
                        "               { \"name\": \"http_query\",  \"type\": [\"string\",\"null\"] },\n" +
                        "               { \"name\": \"http_version\",\"type\": [\"string\",\"null\"] },\n" +
                        "               { \"name\": \"http_status\", \"type\": [\"string\",\"null\"] },\n" +
                        "               { \"name\": \"identd\", \"type\": [\"string\",\"null\"] },\n" +
                        "               { \"name\": \"user\",        \"type\": [\"string\",\"null\"] }    ]}");

        streamProperties.put(StreamProperties.KAFKA_TOPIC_DEFAULT_REPLICATION_FACTOR().getName(), "1");
        streamProperties.put(StreamProperties.KAFKA_TOPIC_DEFAULT_PARTITIONS().getName(), "2");


        StreamConfiguration chainConf = new StreamConfiguration();
        chainConf.setComponent(KafkaRecordStreamParallelProcessing.class.getName());
        chainConf.setType(ComponentType.STREAM.toString());
        chainConf.setConfiguration(streamProperties);
        chainConf.setStream("KafkaStream");
        return chainConf;
    }


    private StreamConfiguration getDebuggerStreamConfiguration() {
        Map<String, String> streamProperties = new HashMap<>();
        streamProperties.put(StreamProperties.KAFKA_METADATA_BROKER_LIST().getName(), "sandbox:9092");
        streamProperties.put(StreamProperties.KAFKA_ZOOKEEPER_QUORUM().getName(), "sandbox:2181");
        streamProperties.put(StreamProperties.INPUT_TOPICS().getName(), "logisland_raw");
        streamProperties.put(StreamProperties.OUTPUT_TOPICS().getName(), "logisland_events");
        streamProperties.put(StreamProperties.INPUT_SERIALIZER().getName(), StreamProperties.NO_SERIALIZER().getValue());
        streamProperties.put(StreamProperties.OUTPUT_SERIALIZER().getName(), StreamProperties.JSON_SERIALIZER().getValue());
        streamProperties.put(StreamProperties.KAFKA_TOPIC_DEFAULT_REPLICATION_FACTOR().getName(), "1");
        streamProperties.put(StreamProperties.KAFKA_TOPIC_DEFAULT_PARTITIONS().getName(), "4");


        StreamConfiguration chainConf = new StreamConfiguration();
        chainConf.setComponent(KafkaRecordStreamDebugger.class.getName());
        chainConf.setType(ComponentType.STREAM.toString());
        chainConf.setConfiguration(streamProperties);
        chainConf.setStream("KafkaSQLStream");
        return chainConf;
    }


    private StreamConfiguration getSQLStreamConfiguration() {
        Map<String, String> streamProperties = new HashMap<>();
        streamProperties.put(StreamProperties.OUTPUT_RECORD_TYPE().getName(), "product_metric");
        streamProperties.put(StreamProperties.KAFKA_METADATA_BROKER_LIST().getName(),
                "sd-84190:6667,sd-84191:6667,sd-84192:6667,sd-84186:6667");
        streamProperties.put(StreamProperties.KAFKA_ZOOKEEPER_QUORUM().getName(),
                "sd-76387:2181,sd-84186:2181,sd-84189:2181");
        streamProperties.put(StreamProperties.INPUT_TOPICS().getName(), "ffact_products");
        streamProperties.put(StreamProperties.OUTPUT_TOPICS().getName(), "ffact_metrics");
        streamProperties.put(StreamProperties.INPUT_SERIALIZER().getName(), StreamProperties.JSON_SERIALIZER().getValue());

        streamProperties.put(StreamProperties.OUTPUT_SERIALIZER().getName(), StreamProperties.JSON_SERIALIZER().getValue());
        streamProperties.put(StreamProperties.KAFKA_TOPIC_DEFAULT_REPLICATION_FACTOR().getName(), "1");
        streamProperties.put(StreamProperties.KAFKA_TOPIC_DEFAULT_PARTITIONS().getName(), "1");

        streamProperties.put(StreamProperties.MAX_RESULTS_COUNT().getName(), "10");
        streamProperties.put(StreamProperties.SQL_QUERY().getName(), "SELECT count(*)/first(theoretical_cadence) AS product_trs, count(*) as product_count, factory, line, first(product_type) as product_type, first(theoretical_cadence) as theoretical_cadence, max(record_time) as record_time\n" +
                "          FROM ffact_products\n" +
                "          GROUP BY factory, line\n" +
                "          LIMIT 20");


        streamProperties.put(StreamProperties.AVRO_INPUT_SCHEMA().getName(),
                "{  \"version\": 1,\n" +
                        "             \"type\": \"record\",\n" +
                        "             \"name\": \"com.hurence.logisland.ffact.product\",\n" +
                        "             \"fields\": [\n" +
                        "               { \"name\": \"record_errors\",    \"type\": [ {\"type\": \"array\", \"items\": \"string\"},\"null\"] },\n" +
                        "               { \"name\": \"record_raw_key\",   \"type\": [\"string\",\"null\"] },\n" +
                        "               { \"name\": \"record_raw_value\", \"type\": [\"string\",\"null\"] },\n" +
                        "               { \"name\": \"record_id\",        \"type\": [\"string\"] },\n" +
                        "               { \"name\": \"record_time\",      \"type\": [\"long\"] },\n" +
                        "               { \"name\": \"record_type\",      \"type\": [\"string\"] },\n" +
                        "               { \"name\": \"label\",            \"type\": [\"string\",\"null\"] },\n" +
                        "               { \"name\": \"product_type\",     \"type\": [\"string\",\"null\"] },\n" +
                        "               { \"name\": \"operator\",         \"type\": [\"string\",\"null\"] },\n" +
                        "               { \"name\": \"factory\",          \"type\": [\"string\",\"null\"] },\n" +
                        "               { \"name\": \"latitude\",         \"type\": [\"float\",\"null\"] },\n" +
                        "               { \"name\": \"longitude\",        \"type\": [\"float\",\"null\"] },\n" +
                        "               { \"name\": \"theoretical_cadence\",\"type\": [\"float\",\"null\"] },\n" +
                        "               { \"name\": \"line\",             \"type\": [\"string\",\"null\"] }   \n" +
                        "              ]}");

        StreamConfiguration chainConf = new StreamConfiguration();
        chainConf.setComponent(KafkaRecordStreamSQLAggregator.class.getName());
        chainConf.setType("stream");
        chainConf.setConfiguration(streamProperties);
        chainConf.setStream("KafkaSQLStream");
        return chainConf;
    }

    private ProcessorConfiguration getSplitTextProcessorConfiguration() {
        Map<String, String> processorProperties = new HashMap<>();
        processorProperties.put("value.regex", APACHE_LOG_REGEX);
        processorProperties.put("value.fields", APACHE_LOG_FIELDS);
        processorProperties.put("key.regex", "(\\S*):(\\S*):(\\S*):(\\S*):(\\S*)");
        processorProperties.put("key.field", "search_index,sub_project_code,record_type,host_name,uuid");

        ProcessorConfiguration processorConf = new ProcessorConfiguration();
        processorConf.setComponent("com.hurence.logisland.processor.SplitText");
        processorConf.setType("parser");
        processorConf.setConfiguration(processorProperties);
        processorConf.setProcessor("parser");
        return processorConf;
    }


    private ProcessorConfiguration getMockProcessorConfiguration() {


        ProcessorConfiguration processorConf = new ProcessorConfiguration();
        processorConf.setComponent(MockProcessor.class.getName());
        processorConf.setType(ComponentType.PROCESSOR.toString());
        processorConf.setProcessor("debugger");
        return processorConf;
    }

    private ProcessorConfiguration getDebugProcessorConfiguration() {
        Map<String, String> processorProperties = new HashMap<>();
        processorProperties.put("event.serializer", "json");

        ProcessorConfiguration processorConf = new ProcessorConfiguration();
        processorConf.setComponent("com.hurence.logisland.processor.DebugStream");
        processorConf.setType(ComponentType.PROCESSOR.toString());
        processorConf.setConfiguration(processorProperties);
        processorConf.setProcessor("debugger");
        return processorConf;
    }
}
