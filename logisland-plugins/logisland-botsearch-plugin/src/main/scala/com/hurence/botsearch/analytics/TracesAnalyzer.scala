package com.hurence.botsearch.analytics

import com.hurence.logisland.botsearch.{HttpFlow, Trace}
import com.hurence.logisland.utils.spark.SparkUtils
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SQLContext

import scala.collection.mutable.ArrayBuffer


/**
  * This Spark application load enriched csv flows from HDFS
  * and compute trace analytics among them
  *
  * {{{
  *   # build scala assembly
  *   sbt assembly
  *
  *   # submit scala job
  *   spark-submit
  *      --class com.hurence.logisland.job.TracesAnalyzer
  *      target/scala-2.10/log-island-assembly.jar
  *      -f /user/log-island/data/cisco

  * }}}
  *
  */
object TracesAnalyzer extends LazyLogging {


    /**
      * Main Spark job entry
      *
      * @param args
      */
    def main(args: Array[String]) {

        //////////////////////////////////////////
        // Commande line management
        /* val parser = new GnuParser()
            val options = new Options()
            options.addOption("f", "input-path", true, "folder containing parquet files for input processing")
            options.addOption("h", "help", false, "print usage")
            val line = parser.parse(options, args)
            val source = line.getOptionValue("f", "serai/2014.03.26")
            */

        ////////////////////////////////////////
        // init spark context
        val sc = SparkUtils.initContext(this.getClass.getName)
        val sqlContext = new SQLContext(sc)
        SparkUtils.registerUdfs(sqlContext)
        import sqlContext.implicits._
        // try {


        ////////////////////////////////////////
        logger.info("load flows from files")
        val source = "/user/log-island/data/cisco"
        val flows = sc.textFile(s"$source/flows").map(line => NetworkFlow.parse(line))






        ////////////////////////////////////////
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
            }).map(flow => (flow.getIpSource + "_" + flow.getIpTarget, new NetworkTrace(
            flow.getIpSource,
            flow.getIpTarget,
            flow.getAvgUploadedBytes.toFloat,
            flow.getAvgDownloadedBytes.toFloat,
            flow.getAvgTimeBetweenTwoFLows.toFloat,
            flow.getMostSignificantFrequency.toFloat,
            flow.getFlows.size(),
            "")))

        logger.info(s"save flows to parquet")
        flows.toDF().write.save(s"$source/flows.parquet")

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
            r._2.flowsCount)).toDF("id",
            "ip_source",
            "ip_target",
            "avg_uploaded_bytes",
            "avg_downloaded_bytes",
            "avg_time_between_two_fLows",
            "most_significant_frequency",
            "flows_count")
            .join(centroids, "id")
            .select("ip_source",
                "ip_target",
                "avg_uploaded_bytes",
                "avg_downloaded_bytes",
                "avg_time_between_two_fLows",
                "most_significant_frequency",
                "flows_count",
                "centroid")

        tmp.printSchema()
        tmp.show()
        tmp.write.save(s"$source/traces.parquet")

        sc.stop()
    }

}