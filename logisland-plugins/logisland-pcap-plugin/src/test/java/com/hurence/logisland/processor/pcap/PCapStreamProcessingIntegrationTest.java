/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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
package com.hurence.logisland.processor.pcap;

import com.hurence.logisland.component.ComponentFactory;
import com.hurence.logisland.component.ComponentType;
import com.hurence.logisland.config.EngineConfiguration;
import com.hurence.logisland.config.ProcessorConfiguration;
import com.hurence.logisland.config.StreamConfiguration;
import com.hurence.logisland.engine.AbstractStreamProcessingIntegrationTest;
import com.hurence.logisland.engine.EngineContext;
import com.hurence.logisland.engine.spark.KafkaStreamProcessingEngine;
import com.hurence.logisland.processor.MockProcessor;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.stream.spark.AbstractKafkaRecordStream;
import com.hurence.logisland.stream.spark.KafkaRecordStreamParallelProcessing;
import com.hurence.logisland.util.file.FileUtil;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Empty Java class for source jar generation (need to publish on OSS sonatype)
 */
public class PCapStreamProcessingIntegrationTest extends AbstractStreamProcessingIntegrationTest {

    private static Logger logger = LoggerFactory.getLogger(PCapStreamProcessingIntegrationTest.class);

    public Optional<EngineContext> getEngineContext() {
        Map<String, String> properties = new HashMap<>();
        properties.put(KafkaStreamProcessingEngine.SPARK_APP_NAME().getName(), "parsePCapEventsDemo");
        properties.put(KafkaStreamProcessingEngine.SPARK_STREAMING_BATCH_DURATION().getName(), "4000");
        properties.put(KafkaStreamProcessingEngine.SPARK_MASTER().getName(), "local[4]");
        properties.put(KafkaStreamProcessingEngine.SPARK_STREAMING_TIMEOUT().getName(), "-1");
        properties.put(KafkaStreamProcessingEngine.SPARK_TASK_MAX_FAILURES().getName(),"8");

        EngineConfiguration conf = new EngineConfiguration();
        conf.setComponent(KafkaStreamProcessingEngine.class.getName());
        conf.setType(ComponentType.ENGINE.toString());
        conf.setConfiguration(properties);
        conf.addProcessorChainConfigurations(createStreamConfig());

        return ComponentFactory.getEngineContext(conf);
    }

    private StreamConfiguration createStreamConfig() {
        Map<String, String> properties = new HashMap<>();
        properties.put(AbstractKafkaRecordStream.KAFKA_METADATA_BROKER_LIST().getName(), BROKERHOST + ":" + BROKERPORT);
        properties.put(AbstractKafkaRecordStream.KAFKA_ZOOKEEPER_QUORUM().getName(), ZKHOST + ":" + zkServer.port());
        properties.put(AbstractKafkaRecordStream.KAFKA_TOPIC_DEFAULT_REPLICATION_FACTOR().getName(), "1");
        properties.put(AbstractKafkaRecordStream.KAFKA_TOPIC_DEFAULT_PARTITIONS().getName(), "1");
        properties.put(AbstractKafkaRecordStream.INPUT_SERIALIZER().getName(), AbstractKafkaRecordStream.BYTESARRAY_SERIALIZER().getValue());
        properties.put(AbstractKafkaRecordStream.OUTPUT_SERIALIZER().getName(), AbstractKafkaRecordStream.KRYO_SERIALIZER().getValue());
        properties.put(AbstractKafkaRecordStream.KAFKA_MANUAL_OFFSET_RESET().getName(), AbstractKafkaRecordStream.LARGEST_OFFSET().getValue());

        properties.put(AbstractKafkaRecordStream.INPUT_TOPICS().getName(), INPUT_TOPIC);
        properties.put(AbstractKafkaRecordStream.OUTPUT_TOPICS().getName(), OUTPUT_TOPIC);

        StreamConfiguration conf = new StreamConfiguration();
        conf.setComponent(KafkaRecordStreamParallelProcessing.class.getName());
        conf.setType(ComponentType.STREAM.toString());
        conf.setConfiguration(properties);
        conf.setStream("KafkaStream");
        conf.addProcessorConfiguration(createProcessorConfiguration());

        return conf;
    }

    private ProcessorConfiguration createProcessorConfiguration() {
        Map<String, String> properties = new HashMap<>();
        properties.put(MockProcessor.FAKE_MESSAGE.getName(), MAGIC_STRING);

        ProcessorConfiguration conf = new ProcessorConfiguration();
        conf.setComponent(MockProcessor.class.getName());
        conf.setType(ComponentType.PROCESSOR.toString());
        conf.setConfiguration(properties);
        conf.setProcessor("ParsePCap");

        return conf;
    }

    @Test
    public void validateIntegration() throws NoSuchFieldException, IllegalAccessException, InterruptedException, IOException {

        final List<Record> records = new ArrayList<>();

        byte[] pcapbytes = FileUtil.loadFileContentAsBytes("pcapTestFiles/1-TCP-packet.pcap");

        try {
            // Wait for Kafka to start correctly :
            Thread.sleep(15000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try {
            // Create Record
            Record standardRecord = new StandardRecord("pcap_event_integration");
            standardRecord.setField(FieldDictionary.RECORD_VALUE, FieldType.BYTES, pcapbytes);

            // Send record to Input Topic :
            sendRecord(INPUT_TOPIC, standardRecord);

        } catch (IOException e) {
            e.printStackTrace();
        }
        catch (Exception e) {
            logger.error("unable to launch runner : {}", e);
        }

        try {
            // Wait for the sent record to be handled correctly
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Read all records from Input Topic :
        records.addAll(readRecords(INPUT_TOPIC));

        final TestRunner testRunner = TestRunners.newTestRunner(new ParsePCap());
        testRunner.assertValid();

        // Enqueue all records in the testRunner :
        records.forEach(record -> {
            testRunner.enqueue(record);
        });
        testRunner.clearQueues();
        // Process records :
        testRunner.run();

        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);
        testRunner.assertOutputErrorCount(0);

        // Get all processed records :
        List<MockRecord> processedRecords = testRunner.getOutputRecords();
        // Send each processed record to Kafka OUTPUT TOPIC :
        processedRecords.forEach(record -> {
            try {
                sendRecord(OUTPUT_TOPIC, record);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

    }
}
