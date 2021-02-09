package com.hurence.logisland.webanalytics;

import com.hurence.logisland.bean.KeyValue;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.webanalytics.test.util.EventsGenerator;
import com.hurence.logisland.webanalytics.util.KafkaUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
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

import java.io.Serializable;

@Ignore
public class SparkStreamingMultiSourceAndSinkTest implements Serializable {

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
    public void multipleSinkTest() throws Exception {
        logger.info("Starting test");
        SparkSession spark = SparkSession
                .builder()
                .master("local[2]")
                .appName("StructuredSessionizationFromKafka")
                .getOrCreate();
        logger.info("Created SparkSession");


//    // Subscribe to 1 topic
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", embeddedKafka.getBrokersAsString())
                .option("subscribe", topic1)
                .option("startingOffsets", "earliest")//latest by default for stream
                //failOnDataLoss => may be false alarm
                //kafkaConsumer.pollTimeoutMs
                //fetchOffset.numRetries
                //fetchOffset.retryIntervalMs
                //maxOffsetsPerTrigger
                .load();

        df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                .groupBy("key")
                .count();

        StructType schema = new StructType(
                new StructField[]{
                        new StructField("key", DataTypes.StringType, true, null),
                        new StructField("counter", DataTypes.LongType, true, null)
                }
        );
        Dataset<Row> dataset = df.map(new MapFunction<Row, Row>() {
                   @Override
                   public Row call(Row row) throws Exception {
                       KeyValue kv = new KeyValue(row.getString(0), row.getLong(1));
                       logger.info("processing key {} with count {}", kv.key, kv.count);
                       return row;
                   }
               }, RowEncoder.apply(schema));

        logger.info("Start streaming");
        // Start running the query that prints the session updates to the console
        StreamingQuery query = dataset
                .writeStream()
                .outputMode("update")
                .format("console")
                .option("truncate", false)
                .start();

        StreamingQuery query2 = dataset
                .writeStream()
                .outputMode("update")
                .format("console")
                .option("truncate", false)
                .start();

        EventsGenerator eventGen = new EventsGenerator("divolte_1");
        long ts = 0L;
        while (true) {
            logger.info("Adding an event in topic");
            Record event = eventGen.generateEvent(ts, "url");
            kafkaUtils.addingEventsToTopicPartition(topic1, 0, event);
            kafkaUtils.addingEventsToTopicPartition(topic1, 0, "session1", event);
            kafkaUtils.addingEventsToTopicPartition(topic1, 1, "session2", event);
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
    public void multipleSourceTest() throws Exception {
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
                .option("startingOffsets", "earliest")//latest by default for stream
                .load();

        Dataset<Row> df2 = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", embeddedKafka.getBrokersAsString())
                .option("subscribe", topicName2)
                .option("startingOffsets", "earliest")//latest by default for stream
                .load();

        df = df.union(df2).selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                .groupBy("key")
                .count();

        StructType schema = new StructType(
                new StructField[]{
                        new StructField("key", DataTypes.StringType, true, null),
                        new StructField("counter", DataTypes.LongType, true, null)
                }
        );

        Dataset<Row> dataset = df.map(new MapFunction<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                KeyValue kv = new KeyValue(row.getString(0), row.getLong(1));
                logger.info("processing key {} with count {}", kv.key, kv.count);
                return row;
            }
        }, RowEncoder.apply(schema));

        logger.info("Start streaming");
        // Start running the query that prints the session updates to the console
        StreamingQuery query = dataset
                .writeStream()
                .outputMode("update")
                .format("console")
                .option("truncate", false)
                .start();

        EventsGenerator eventGen = new EventsGenerator("divolte_1");
        long ts = 0L;
        while (true) {
            logger.info("Adding an event in topic");
            Record event = eventGen.generateEvent(ts, "url");
            kafkaUtils.addingEventsToTopicPartition(topicName, 0, "fromtopic1", event);
            kafkaUtils.addingEventsToTopicPartition(topicName2, 0, "fromtopic2", event);
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
    public void multipleSourceAndSinkTest() throws Exception {
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
                .option("startingOffsets", "earliest")//latest by default for stream
                .load();

        Dataset<Row> df2 = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", embeddedKafka.getBrokersAsString())
                .option("subscribe", topicName2)
                .option("startingOffsets", "earliest")//latest by default for stream
                .load();

        df = df.union(df2).selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                .groupBy("key")
                .count();

        StructType schema = new StructType(
                new StructField[]{
                        new StructField("key", DataTypes.StringType, true, null),
                        new StructField("counter", DataTypes.LongType, true, null)
                }
        );

        Dataset<Row> dataset = df.map(new MapFunction<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                KeyValue kv = new KeyValue(row.getString(0), row.getLong(1));
                logger.info("processing key {} with count {}", kv.key, kv.count);
                return row;
            }
        }, RowEncoder.apply(schema));

        logger.info("Start streaming");
        // Start running the query that prints the session updates to the console
        StreamingQuery query = dataset
                .writeStream()
                .outputMode("update")
                .format("console")
                .option("truncate", false)
                .start();

        StreamingQuery query2 = dataset
                .writeStream()
                .outputMode("update")
                .format("console")
                .option("truncate", false)
                .start();

        EventsGenerator eventGen = new EventsGenerator("divolte_1");
        long ts = 0L;
        while (true) {
            logger.info("Adding an event in topic");
            Record event = eventGen.generateEvent(ts, "url");
            kafkaUtils.addingEventsToTopicPartition(topicName, 0, "fromtopic1", event);
            kafkaUtils.addingEventsToTopicPartition(topicName2, 0, "fromtopic2", event);
            logger.info("Waiting 5 sec");
            long sleep = 5000L;
            ts += sleep;
            Thread.sleep(sleep);
        }
    }

}