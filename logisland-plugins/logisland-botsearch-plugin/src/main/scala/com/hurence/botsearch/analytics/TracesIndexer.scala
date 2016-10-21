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
package com.hurence.botsearch.analytics

import com.hurence.botsearch.trace.NetworkTraceLogParser
import com.hurence.logisland.util.spark.SparkUtils
import org.apache.commons.cli.{GnuParser, Options}
import org.apache.log4j.Logger
import org.apache.spark.sql.SQLContext


/**
  * This jobs takes kafka topics full of logs
  * and index them into an ES index
  *
  *
  * @see https://databricks.com/blog/2015/03/30/improvements-to-kafka-integration-of-spark-streaming.html
  *
  */
object TracesIndexer {

    private val logger = Logger.getLogger(this.getClass.getName)


    def main(args: Array[String]) {


        //////////////////////////////////////////
        // Commande lien management
        val parser = new GnuParser()
        val options = new Options()
        options.addOption("o", "output", true, "es, solr, debug")
        options.addOption("t", "time-window", true, "window time for micro batch")
        options.addOption("h", "help", false, "print usage")

        // parse the command line arguments
        val line = parser.parse(options, args)
        val output = line.getOptionValue("o", "debug")
        val windowTime = line.getOptionValue("t", "2").toLong


        // set up context
        val sc = SparkUtils.initContext(this.getClass.getName)
        val sqlContext = new SQLContext(sc)


        val source = "/user/logisland/data/cisco"
        val traces = sqlContext.read.load(s"$source/traces.parquet").rdd.repartition(4)



        // get the logs, convert them to NetworkFlows and index them to SOLR
        output match {
            case "es" => {
                val esHosts = "localhost"
                val esIndex = "logisland"

                val parser = new NetworkTraceLogParser()
           //     traces.map(trace => parser.parse(null, "", trace.toString()))
                //EventIndexer.indexEvents(traces, esHosts, esIndex, "traces")
            }
            case "debug" => {
                traces.take(10).foreach(println)
            }
        }


    }

}
