/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.engine;

import com.hurence.logisland.component.ComponentType;
import com.hurence.logisland.config.*;
import com.hurence.logisland.engine.spark.SparkStreamProcessingEngine;
import com.hurence.logisland.processor.SplitText;
import com.hurence.logisland.processor.chain.KafkaRecordStream;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class StreamProcessingDebuggerTest {
    private static Logger logger = LoggerFactory.getLogger(StreamProcessingDebuggerTest.class);

    private static final String APACHE_LOG_FIELDS = "src_ip,identd,user,record_time,http_method,http_query,http_version,http_status,bytes_out";
    private static final String APACHE_LOG_REGEX = "(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+\\[([\\w:/]+\\s[+\\-]\\d{4})\\]\\s+\"(\\S+)\\s+(\\S+)\\s+(\\S+)\"\\s+(\\S+)\\s+(\\S+)";


    @Test
    public void remoteTest(){

        logger.info("starting StreamProcessingRunner");

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
        
        

        Map<String, String> chainProperties = new HashMap<>();
        chainProperties.put(KafkaRecordStream.KAFKA_METADATA_BROKER_LIST.getName(), "sd-79372:6667,sd-84190:6667,sd-84191:6667,sd-84192:6667,sd-84196:6667");
        chainProperties.put(KafkaRecordStream.KAFKA_ZOOKEEPER_QUORUM.getName(), "sd-76387:2181,sd-84186:2181,sd-84189:2181");
        chainProperties.put(KafkaRecordStream.INPUT_TOPICS.getName(), "appl_prod_oad_falcon_usr-backend.localhost_access,appl_prod_oad_falcon_usr-gateway.localhost_access");
        chainProperties.put(KafkaRecordStream.OUTPUT_TOPICS.getName(), "logisland_events");
        chainProperties.put(KafkaRecordStream.INPUT_SERIALIZER.getName(), KafkaRecordStream.NO_SERIALIZER.getValue());
        chainProperties.put(KafkaRecordStream.OUTPUT_SERIALIZER.getName(), KafkaRecordStream.KRYO_SERIALIZER.getValue());

        ProcessorChainConfiguration chainConf = new ProcessorChainConfiguration();
        chainConf.setComponent(KafkaRecordStream.class.getName());
        chainConf.setType(ComponentType.PROCESSOR_CHAIN.toString());
        chainConf.setConfiguration(chainProperties);
        chainConf.setProcessorChain("KafkaStream");
        chainConf.addProcessorConfiguration(processorConf);
        
        
        
        Map<String, String> engineProperties = new HashMap<>();
        engineProperties.put(SparkStreamProcessingEngine.SPARK_APP_NAME().getName(), "testApp");
        engineProperties.put(SparkStreamProcessingEngine.SPARK_STREAMING_BATCH_DURATION().getName(), "500");
        engineProperties.put(SparkStreamProcessingEngine.SPARK_MASTER().getName(), "local[4]");
        engineProperties.put(SparkStreamProcessingEngine.SPARK_STREAMING_TIMEOUT().getName(), "10000");


        EngineConfiguration engineConf = new EngineConfiguration();
        engineConf.setComponent(SparkStreamProcessingEngine.class.getName());
        engineConf.setType(ComponentType.ENGINE.toString());
        engineConf.setConfiguration(engineProperties);
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
}
