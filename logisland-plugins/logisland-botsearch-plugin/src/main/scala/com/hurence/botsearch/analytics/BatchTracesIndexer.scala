package com.hurence.botsearch.analytics

import java.text.SimpleDateFormat
import java.util.Date

import com.hurence.logisland.botsearch.{HttpFlow, Trace}
import com.hurence.logisland.utils.kafka.KafkaOffsetUtils
import com.hurence.logisland.utils.spark.SparkUtils
import com.typesafe.scalalogging.slf4j.LazyLogging

import kafka.serializer.StringDecoder
import org.apache.commons.cli.{DefaultParser, Options}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SQLContext
//import org.apache.spark.streaming.kafka.{KafkaUtils, OffsetRange}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * A demo program that index a few items
  *
  * https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+SimpleConsumer+Example
  *
  *
  */
object BatchTracesIndexer extends LazyLogging {


    def main(args: Array[String]) {


        //////////////////////////////////////////
        // Commande lien management
        val parser = new DefaultParser()
        val options = new Options()
        options.addOption("o", "output", true, "es, solr, debug")
        options.addOption("w", "time-window", true, "window time for micro batch")
        options.addOption("b", "broker-list", true, "kafka broker list :localhost:9092,anotherhost:9092")
        options.addOption("t", "topic-list", true, "kafka topic list log-island1,log-island2")
        options.addOption("e", "es-host", true, "elasticsearch host : sandbox")
        options.addOption("h", "help", false, "print usage")
        options.addOption("f", "folder-path", true, "parquet folder path")
        options.addOption("p", "parquet", false, "store to parquet ?")
        options.addOption("i", "index", false, "indexation ?")

        // parse the command line arguments
        val line = parser.parse(options, args)
        val output = line.getOptionValue("o", "debug")
        val windowTime = line.getOptionValue("w", "2").toLong
        val brokerList = line.getOptionValue("b", "sandbox:9092")
        val topicList = line.getOptionValue("t", "log-island")
        val esHosts = line.getOptionValue("e", "sandbox")
        val doSaveAsParquet = line.hasOption("p")
        val doIndexation = line.hasOption("i")


        val source = "file://" + line.getOptionValue("f", "/usr/local/log-island/data/out")

        // set up context
        val sc = SparkUtils.initContext(this.getClass.getName)
        val sqlContext = new SQLContext(sc)
        import sqlContext.implicits._

        // Define the Kafka parameters, broker list must be specified
        val kafkaParams = Map("metadata.broker.list" -> brokerList, "group.id" -> "log-island-demo")

        // Define which topics to read from
        val topics = topicList.split(",").toSet

        // get first 100000 messages
        val lastOffest = KafkaOffsetUtils.getLastOffset(
            brokerList,
            "log-island",
            0,
            kafka.api.OffsetRequest.LatestTime)

     /*   val offsetRanges = Array(
            OffsetRange("log-island", 0, 0, lastOffest)
        )
        logger.info(s"last offset for kafka topic is $lastOffest")

        if (lastOffest != 0) {


            ////////////////////////////////////////
            // flows loading

            logger.info("Create the direct stream with the Kafka parameters and topics")
            val kafkaRdd = KafkaUtils.createRDD[String, String, StringDecoder, StringDecoder](
                sc, kafkaParams, offsetRanges)

            logger.info("convert raw lines to networkflow objects")
            val flows = kafkaRdd.map(kv => NetworkFlow.parse(kv._2))


            ////////////////////////////////////////
            // trace computation

            logger.info("compute traces from flows")
            val traces = flows.map(r => (r.ipSource + "_" + r.ipTarget, r))
                .groupByKey()
                .map(t => {
                    val flows = t._2
                    val tokens = t._1.split("_")
                    val trace = new Trace()
                    try {
                        trace.setIpSource(tokens(0))
                        trace.setIpTarget(tokens(1))

                        // we need at least 5 flows to compute one trace
                        if (flows.size > 5) {

                            // set up the flows buffer
                            val buffer = new ArrayBuffer[HttpFlow]()
                            flows.foreach(f => {
                                val flow = new HttpFlow()
                                flow.setDate(new java.util.Date(f.timestamp))
                                flow.setRequestSize(f.requestSize)
                                flow.setResponseSize(f.responseSize)
                                flow.setTags(f.tags.split(",").toList)
                                buffer += flow
                            })

                            // flows need to be sorted on timestamp
                            val sortedFlows = buffer.toList.sortWith(_.getDate().getTime() < _.getDate().getTime())
                            sortedFlows.foreach(f => trace.add(f))

                            // compute trace frequencies and stats
                            trace.compute()
                        }
                    } catch {
                        case ex: Throwable => logger.error(ex.getMessage)
                    }

                    trace
                }).map(trace => (trace.getIpSource + "_" + trace.getIpTarget, new NetworkTrace(
                trace.getIpSource,
                trace.getIpTarget,
                trace.getAvgUploadedBytes.toFloat,
                trace.getAvgDownloadedBytes.toFloat,
                trace.getAvgTimeBetweenTwoFLows.toFloat,
                trace.getMostSignificantFrequency.toFloat,
                trace.getFlows.size(),
                trace.getTags.toArray.mkString(","))))



            ////////////////////////////////////////
            // traces clustering

            logger.info("convert traces into a Dense vector")
            val tracesVector = traces.map(t => (t._1,
                Vectors.dense(Array[Double](
                    t._2.avgUploadedBytes,
                    t._2.avgDownloadedBytes,
                    t._2.avgTimeBetweenTwoFLows,
                    t._2.mostSignificantFrequency))))
                .cache()

            logger.info("scale the trace to get mean = 0 and std = 1")
            val scaler = new StandardScaler(withMean = true, withStd = true)
                .fit(tracesVector.map(_._2))
            val scaledTraces = tracesVector.map(x => (x._1, scaler.transform(x._2)))


            // TODO add an automated job which compute best parameters
            // Cluster the data into two classes using KMeans
            val numClusters = 8
            val numIterations = 20
            logger.info(s"Cluster the data into two classes using KMeans k:$numClusters, numIterations:$numIterations")
            val clusters = KMeans.train(scaledTraces.map(_._2), numClusters, numIterations)

            // Evaluate clustering by computing Within Set Sum of Squared Errors
            val WSSSE = clusters.computeCost(scaledTraces.map(_._2))
            logger.info(s"Within Set Sum of Squared Errors = $WSSSE")


            logger.info(s"assign traces to clusters")
            val centroids = scaledTraces.map(t => (t._1, clusters.predict(t._2))).toDF("id", "centroid")

            logger.info(s"save traces to parquet")
            val tmp = traces.map(r => (r._1, r._2.ipSource, r._2.ipTarget,
                r._2.avgUploadedBytes,
                r._2.avgDownloadedBytes,
                r._2.avgTimeBetweenTwoFLows,
                r._2.mostSignificantFrequency,
                r._2.flowsCount,
                r._2.tags)).toDF("id",
                "ipSource",
                "ipTarget",
                "avgUploadedBytes",
                "avgDownloadedBytes",
                "avgTimeBetweenTwoFLows",
                "mostSignificantFrequency",
                "flowsCount",
                "tags")
                .join(centroids, "id")
                .select("ipSource",
                    "ipTarget",
                    "avgUploadedBytes",
                    "avgDownloadedBytes",
                    "avgTimeBetweenTwoFLows",
                    "mostSignificantFrequency",
                    "flowsCount",
                    "tags",
                    "centroid")
                .filter("flowsCount != 0")
                .repartition(8)

            tmp.printSchema()
            tmp.show()


            ////////////////////////////////////////
            // traces indexation
            val dateSuffix = new SimpleDateFormat("yyyy.MM.dd").format(new Date())

            if (doIndexation) {
                logger.info("create es index")
                //    EventConverter.createIndex(esHosts, s"log-island-$dateSuffix")

                logger.info("launch traces indexation to es")
                //     EventConverter.index(tmp.toDF.rdd, esHosts, s"log-island-$dateSuffix", "trace")
            }

            if (doSaveAsParquet) {
                logger.info("save traces to parquet")
                tmp.write.save(s"$source/$dateSuffix/traces.parquet")
            }
        }*/

        sc.stop()
    }
}
