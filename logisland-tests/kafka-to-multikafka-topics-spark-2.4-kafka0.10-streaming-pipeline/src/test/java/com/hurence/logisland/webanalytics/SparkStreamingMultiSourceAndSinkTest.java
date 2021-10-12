/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
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
package com.hurence.logisland.webanalytics;

import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.RecordDictionary;
import com.hurence.logisland.webanalytics.test.util.EventsGenerator;
import com.hurence.logisland.webanalytics.util.KafkaUtils;
import com.hurence.logisland.webanalytics.util.SparkMethods;
import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.test.rule.KafkaEmbedded;

@Ignore
public class SparkStreamingMultiSourceAndSinkTest {

    private static Logger logger = LoggerFactory.getLogger(SparkStreamingMultiSourceAndSinkTest.class);

    final static String topic1 = "topic1";
    final static String topic2 = "topic2";

    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(
            1, true, 2,
            topic1, topic2);

    private static KafkaUtils kafkaUtils = new KafkaUtils(embeddedKafka);

    /**
     *
     */
    @Test
    public void multipleSourceOneRewindOneLastestWithoutGroupByTest() throws Exception {
        logger.info("Starting test");
        final String topicName = topic1;
        final String topicName2 = topic2;
        SparkSession spark = SparkSession
                .builder()
                .master("local[2]")
//                .appName("StructuredSessionizationFromKafka")
                .getOrCreate();
        logger.info("Created SparkSession");
//
//    // Subscribe to 1 topic
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", embeddedKafka.getBrokersAsString())
                .option("subscribe", topicName)
                .option("startingOffsets", "latest")//latest by default for stream
                .load();

        Dataset<Row> df2 = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", embeddedKafka.getBrokersAsString())
                .option("subscribe", topicName2)
                .option("startingOffsets", "earliest")//latest by default for stream
                .load();

        injectDataIntoTopics();
        StructType schema = new StructType(
                new StructField[]{
                        new StructField("key", DataTypes.StringType, true, null),
                        new StructField("value", DataTypes.StringType, true, null),
                }
        );

//        if (context.getPropertyValue(GROUP_BY_FIELDS).isSet) {
//      import readDF.sparkSession.implicits._
//                    readDF
//        .groupByKey(_.getField(groupByField).asString())
//                    .flatMapGroups((key, iterator) => {
//                pipelineMethods.executePipeline(key, iterator)
//            })

        Dataset<Row> dataset = df.union(df2).selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                .mapPartitions((MapPartitionsFunction<Row, Row>) SparkMethods::mappartitionRowIntoKeyValueRow, RowEncoder.apply(schema));

        logger.info("Start streaming");
        // Start running the query that prints the session updates to the console
        StreamingQuery query = dataset
                .writeStream()
                .outputMode("update")
                .format("console")
                .option("truncate", false)
                .start();

        Thread.sleep(5000L);//wait to process earliest for topic 2 only !
        EventsGenerator eventGen = new EventsGenerator("events_after_starting_stream");
        long ts = 0L;
        while (true) {
            logger.info("Adding an event in topic");
            Record event = eventGen.generateEvent(ts, topicName);
            kafkaUtils.addingEventsToTopicPartition(topicName, 0, "fromtopic1", event);
            kafkaUtils.addingEventsToTopicPartition(topicName, 0, "fromtopic2", event);
            logger.info("Waiting 5 sec");
            long sleep = 5000L;
            ts += sleep;
            Thread.sleep(sleep);
        }
    }

    /**
     *
     */
    @Test
    public void multipleSourceOneRewindOneLastestWithGroupByTest() throws Exception {
        logger.info("Starting test");
        final String topicName = topic1;
        final String topicName2 = topic2;
        SparkSession spark = SparkSession
                .builder()
                .master("local[2]")
//                .appName("StructuredSessionizationFromKafka")
                .getOrCreate();
        logger.info("Created SparkSession");
//
//    // Subscribe to 1 topic
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", embeddedKafka.getBrokersAsString())
                .option("subscribe", topicName)
                .option("startingOffsets", "latest")//latest by default for stream
                .load();

        Dataset<Row> df2 = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", embeddedKafka.getBrokersAsString())
                .option("subscribe", topicName2)
                .option("startingOffsets", "earliest")//latest by default for stream
                .load();

        injectDataIntoTopics();
        StructType schema = new StructType(
                new StructField[]{
                        new StructField("key", DataTypes.StringType, true, null),
                        new StructField("value", DataTypes.StringType, true, null),
                }
        );

//        if (context.getPropertyValue(GROUP_BY_FIELDS).isSet) {
//      import readDF.sparkSession.implicits._
//                    readDF
//        .groupByKey(_.getField(groupByField).asString())
//                    .flatMapGroups((key, iterator) => {
//                pipelineMethods.executePipeline(key, iterator)
//            })

        Dataset<Row> dataset = df.union(df2).selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
//                .mapPartitions((MapPartitionsFunction<Row, Row>) SparkMethods::mappartitionRowIntoKeyValueRow, RowEncoder.apply(schema));
                .groupByKey((MapFunction<Row, String>) SparkMethods::groupBySession,
                        Encoders.STRING())
                .flatMapGroups((FlatMapGroupsFunction<String, Row, Row>) SparkMethods::doForEAchSessionGroup,
                        RowEncoder.apply(schema));
        logger.info("Start streaming");
        // Start running the query that prints the session updates to the console
        StreamingQuery query = dataset
                .writeStream()
                .outputMode("update")
                .format("console")
                .option("truncate", false)
                .start();

        Thread.sleep(5000L);//wait to process earliest for topic 2 only !
        EventsGenerator eventGen = new EventsGenerator("events_after_starting_stream");
        long ts = 0L;
        while (true) {
            logger.info("Adding an event in topic");
            Record event = eventGen.generateEvent(ts, topicName);
            kafkaUtils.addingEventsToTopicPartition(topicName, 0, "fromtopic1", event);
            kafkaUtils.addingEventsToTopicPartition(topicName, 0, "fromtopic2", event);
            logger.info("Waiting 5 sec");
            long sleep = 5000L;
            ts += sleep;
            Thread.sleep(sleep);
        }
    }

    private void injectDataIntoTopics() throws InterruptedException {
        EventsGenerator eventGen = new EventsGenerator("events_before_running_job");
        long ts = 0L;
        while (ts < 1000) {
            logger.info("Adding an event in topics");
            Record event = eventGen.generateEvent(ts, "outputtopic");
            kafkaUtils.addingEventsToTopicPartition(topic1, 0, "fromtopic1", event);
            kafkaUtils.addingEventsToTopicPartition(topic2, 0, "fromtopic2", event);
            ts += 100;
        }
    }
}