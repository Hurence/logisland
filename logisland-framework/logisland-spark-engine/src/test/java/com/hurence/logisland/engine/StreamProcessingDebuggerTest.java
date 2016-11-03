/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.engine;

import com.hurence.logisland.component.ComponentType;
import com.hurence.logisland.config.ComponentFactory;
import com.hurence.logisland.config.EngineConfiguration;
import com.hurence.logisland.config.ProcessorChainConfiguration;
import com.hurence.logisland.config.ProcessorConfiguration;
import com.hurence.logisland.engine.spark.HdfsBurnerEngine;
import com.hurence.logisland.engine.spark.StandardSparkStreamProcessingEngine;
import com.hurence.logisland.processor.RecordDebugger;
import com.hurence.logisland.processor.SplitText;
import com.hurence.logisland.processor.chain.KafkaRecordStream;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class StreamProcessingDebuggerTest {
    private static Logger logger = LoggerFactory.getLogger(StreamProcessingDebuggerTest.class);

    private static final String APACHE_LOG_FIELDS = "src_ip,identd,user,record_time,http_method,http_query,http_version,http_status,bytes_out";
    private static final String APACHE_LOG_REGEX = "(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+\\[([\\w:\\/]+\\s[+\\-]\\d{4})\\]\\s+\"(\\S+)\\s+(\\S+)\\s+(\\S+)\"\\s+(\\S+)\\s+(\\S+)";


    @Test
    @Ignore
    public void remoteTest() {

        logger.info("starting StreamProcessingRunner");

        ProcessorConfiguration processorConf = getDebugProcessorConfiguration();
        ProcessorChainConfiguration chainConf = getProcessorChainConfiguration();
        chainConf.addProcessorConfiguration(processorConf);
        EngineConfiguration engineConf = getStandardEngineConfiguration();
        engineConf.addProcessorChainConfigurations(chainConf);


        try {

            // instanciate engine and all the processor from the config
            Optional<StandardEngineInstance> engineInstance = ComponentFactory.getEngineInstance(engineConf);

            assert engineInstance.isPresent();
            assert engineInstance.get().isValid();

            // start the engine
            StandardEngineContext engineContext = new StandardEngineContext(engineInstance.get());
            engineInstance.get().getEngine().start(engineContext);


        } catch (Exception e) {
            logger.error("unable to launch runner : {}", e);
        }


    }
    private EngineConfiguration getStandardEngineConfiguration() {
        Map<String, String> engineProperties = new HashMap<>();
        engineProperties.put(StandardSparkStreamProcessingEngine.SPARK_APP_NAME().getName(), "testApp");
        engineProperties.put(StandardSparkStreamProcessingEngine.SPARK_STREAMING_BATCH_DURATION().getName(), "5000");
        engineProperties.put(StandardSparkStreamProcessingEngine.SPARK_MASTER().getName(), "local[8]");
        engineProperties.put(StandardSparkStreamProcessingEngine.SPARK_STREAMING_TIMEOUT().getName(), "20000");

        EngineConfiguration engineConf = new EngineConfiguration();
        engineConf.setComponent(StandardSparkStreamProcessingEngine.class.getName());
        engineConf.setType(ComponentType.ENGINE.toString());
        engineConf.setConfiguration(engineProperties);
        return engineConf;
    }
    private EngineConfiguration getHdfsBurnerEngineConfiguration() {
        Map<String, String> engineProperties = new HashMap<>();
        engineProperties.put(HdfsBurnerEngine.SPARK_APP_NAME().getName(), "testApp");
        engineProperties.put(HdfsBurnerEngine.SPARK_STREAMING_BATCH_DURATION().getName(), "5000");
        engineProperties.put(HdfsBurnerEngine.SPARK_MASTER().getName(), "local[8]");
        engineProperties.put(HdfsBurnerEngine.SPARK_STREAMING_TIMEOUT().getName(), "20000");

        engineProperties.put(HdfsBurnerEngine.OUTPUT_FOLDER_PATH().getName(), "out-path");
        engineProperties.put(HdfsBurnerEngine.OUTPUT_FORMAT().getName(), "parquet");


        EngineConfiguration engineConf = new EngineConfiguration();
        engineConf.setComponent(HdfsBurnerEngine.class.getName());
        engineConf.setType(ComponentType.ENGINE.toString());
        engineConf.setConfiguration(engineProperties);
        return engineConf;
    }

    private ProcessorChainConfiguration getProcessorChainConfiguration() {
        Map<String, String> chainProperties = new HashMap<>();
        chainProperties.put(KafkaRecordStream.KAFKA_METADATA_BROKER_LIST.getName(),
                "sd-84190:6667,sd-84191:6667,sd-84192:6667,sd-84196:6667");
        chainProperties.put(KafkaRecordStream.KAFKA_ZOOKEEPER_QUORUM.getName(),
                "sd-76387:2181,sd-84186:2181,sd-84189:2181");
        /*chainProperties.put(KafkaRecordStream.KAFKA_METADATA_BROKER_LIST.getName(),
                "localhost:9092");
        chainProperties.put(KafkaRecordStream.KAFKA_ZOOKEEPER_QUORUM.getName(),
                "localhost:2181");*/
        chainProperties.put(KafkaRecordStream.INPUT_TOPICS.getName(), "logisland_events");
        chainProperties.put(KafkaRecordStream.OUTPUT_TOPICS.getName(), "none");
        chainProperties.put(KafkaRecordStream.INPUT_SERIALIZER.getName(), KafkaRecordStream.KRYO_SERIALIZER.getValue());
        chainProperties.put(KafkaRecordStream.OUTPUT_SERIALIZER.getName(), KafkaRecordStream.NO_SERIALIZER.getValue());
        chainProperties.put(KafkaRecordStream.KAFKA_TOPIC_DEFAULT_REPLICATION_FACTOR.getName(), "1");
        chainProperties.put(KafkaRecordStream.KAFKA_TOPIC_DEFAULT_PARTITIONS.getName(), "2");

        ProcessorChainConfiguration chainConf = new ProcessorChainConfiguration();
        chainConf.setComponent(KafkaRecordStream.class.getName());
        chainConf.setType(ComponentType.PROCESSOR_CHAIN.toString());
        chainConf.setConfiguration(chainProperties);
        chainConf.setProcessorChain("KafkaStream");
        return chainConf;
    }

    private ProcessorConfiguration getSplitTextProcessorConfiguration() {
        Map<String, String> processorProperties = new HashMap<>();
        processorProperties.put(SplitText.VALUE_REGEX.getName(), APACHE_LOG_REGEX);
        processorProperties.put(SplitText.VALUE_FIELDS.getName(), APACHE_LOG_FIELDS);
        processorProperties.put(SplitText.KEY_REGEX.getName(), "(\\S*):(\\S*):(\\S*):(\\S*):(\\S*)");
        processorProperties.put(SplitText.KEY_FIELDS.getName(), "search_index,sub_project_code,record_type,host_name,uuid");

        ProcessorConfiguration processorConf = new ProcessorConfiguration();
        processorConf.setComponent(SplitText.class.getName());
        processorConf.setType(ComponentType.PARSER.toString());
        processorConf.setConfiguration(processorProperties);
        processorConf.setProcessor("parser");
        return processorConf;
    }

    private ProcessorConfiguration getDebugProcessorConfiguration() {
        Map<String, String> processorProperties = new HashMap<>();
        processorProperties.put(RecordDebugger.SERIALIZER.getName(), RecordDebugger.JSON.getValue());

        ProcessorConfiguration processorConf = new ProcessorConfiguration();
        processorConf.setComponent(RecordDebugger.class.getName());
        processorConf.setType(ComponentType.PROCESSOR.toString());
        processorConf.setConfiguration(processorProperties);
        processorConf.setProcessor("debugguer");
        return processorConf;
    }
}
