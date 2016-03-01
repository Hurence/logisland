/*
 Copyright 2016 Hurence

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package com.hurence.logisland.integration

import java.util
import java.util.UUID
import java.util.concurrent.TimeUnit

import com.hurence.logisland.event.{EventIndexer, Event}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.elasticsearch.action.bulk.{BulkProcessor, BulkRequest, BulkResponse}
import org.elasticsearch.action.index.IndexRequest.OpType
import org.elasticsearch.action.index.IndexRequestBuilder
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.unit.{ByteSizeUnit, ByteSizeValue, TimeValue}
import collection.JavaConversions._

/**
  * This jobs takes kafka topics full of logs and index them into an ES index
  *
  * @see https://databricks.com/blog/2015/03/30/improvements-to-kafka-integration-of-spark-streaming.html
  *
  */
class ElasticsearchEventIndexer(esHosts: String, esIndex: String) extends EventIndexer with LazyLogging with Serializable {

    var esClient:TransportClient = null



    /**
      * take an rdd of RDD[(String,Event)
      * where _1 is the event type
      *
      * @param events
      * @param bulkSize
      */
    override def bulkLoad(events: util.Collection[Event], bulkSize: Int = 10000) = {
        val start = System.currentTimeMillis()
        logger.info("start indexing events")

        // on startup
        if(esClient == null)
            esClient = ElasticsearchUtils.createTransportClient(esHosts)

        var numItemProcessed = 0L

        val bulkProcessor = BulkProcessor.builder(
            esClient,
            new BulkProcessor.Listener() {
                def beforeBulk(executionId: Long, request: BulkRequest) = {}

                def afterBulk(executionId: Long, request: BulkRequest, response: BulkResponse) = {

                    logger.info(response.buildFailureMessage())
                    logger.info(s"done bulk request in ${response.getTookInMillis} ms with failure = ${response.hasFailures}")
                }

                def afterBulk(executionId: Long, request: BulkRequest, failure: Throwable) = {
                    logger.error(s"something went wrong while bulk loading events to es : ${failure.getMessage}")
                }
            })
            .setBulkActions(bulkSize)
            .setBulkSize(new ByteSizeValue(10, ByteSizeUnit.MB))
            .setFlushInterval(TimeValue.timeValueSeconds(5))
            .setConcurrentRequests(4)
            .build()

        events.foreach(event => {
            numItemProcessed += 1

            // Setting ES document id to document's id itself if any (to prevent duplications in ES)
            var idString = UUID.randomUUID.toString
            if (event.getId != "none") {
                idString = event.getId
            }

            val document = ElasticsearchEventConverter.convert(event)
            val result: IndexRequestBuilder = esClient.prepareIndex(esIndex, event.getType, idString).setSource(document).setOpType(OpType.CREATE)
            bulkProcessor.add(result.request())
        })

        logger.debug(s"waiting for remaining items to be flushed")
        bulkProcessor.awaitClose(10, TimeUnit.MINUTES)
        logger.debug(s"idexed $numItemProcessed records on this partition to es in ${System.currentTimeMillis() - start}")

        logger.info("shutting down es client")
        // on shutdown
        esClient.close()
    }

}
