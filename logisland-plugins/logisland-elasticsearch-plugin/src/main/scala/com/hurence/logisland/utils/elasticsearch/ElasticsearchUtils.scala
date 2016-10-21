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

package com.hurence.logisland.util.elasticsearch

import java.io.{BufferedReader, IOException, InputStream, InputStreamReader}
import java.net.InetAddress
import java.nio.charset.Charset
import java.util.concurrent.TimeUnit
import java.util.{Objects, UUID}

import com.google.common.base.Charsets
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse
import org.elasticsearch.action.bulk.{BulkProcessor, BulkRequest, BulkResponse}
import org.elasticsearch.action.index.IndexRequest.OpType
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.io.Streams
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.unit.{ByteSizeUnit, ByteSizeValue, TimeValue}
import org.elasticsearch.index.query.QueryBuilders


/**
  * Generic class to access elasticsearch documents
  *
  */
object ElasticsearchUtils extends LazyLogging {


    private val ELASTICSEARCH_RESOURCES_FOLDER: String = "/elasticsearch/"
    val EMPTY_STRING: String = ""

    /**
      * instanciate a client instance to access an elasticsearch index from a json config file
      *
      * @param index
      * @param esConfigName
      * @throws java.io.IOException
      * @return
      */
    @throws(classOf[IOException])
    def getClientInstance(index: String, esConfigName: String): Client = {
        val settingsStream: InputStream = this.getClass.getResourceAsStream(ELASTICSEARCH_RESOURCES_FOLDER + index + ".conf")
        if (settingsStream != null) {
            val esConfig: Settings = Settings.builder.loadFromSource(Streams.copyToString(new InputStreamReader(settingsStream, Charsets.UTF_8))).build
            return getClientInstance(esConfig, esConfigName)
        }
        else {
            logger.error("error reading config " + index)
        }
        return getClientInstance(Settings.builder.build, esConfigName)
    }

    private def getClientInstance(settings: Settings, esConfigName: String): Client = {
        Objects.requireNonNull(settings)

        val realEsConfigName = if (esConfigName == null || esConfigName.isEmpty) {
            "es"
        } else esConfigName

        val hostname = settings.get(realEsConfigName + ".node.host", "localhost")
        val port = settings.getAsInt(realEsConfigName + ".node.port", 9300)
        val clusterName = settings.get(realEsConfigName + ".cluster.name", "elasticsearch")

        val instance: Client = try {
            TransportClient.builder
                .settings(Settings.settingsBuilder.put("cluster.name", clusterName)).build
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostname), port))
        }
        catch {
            case t: Throwable => {
                logger.error("Could not setup elasticsearch client", t)
                null
            }
        }
        return instance
    }

    /**
      * Create an ES index from a json file
      *
      * @param client
      * @param index
      */
    def createIndex(client: Client, index: String) {
        Objects.requireNonNull(client)
        Objects.requireNonNull(index)
        logger.info("create index {} - begin", index)
        val response: IndicesExistsResponse = client.admin.indices.prepareExists(index).get
        if (!response.isExists) {
            logger.info("creating index {}", index)
            val settingsStream: InputStream = response.getClass.getResourceAsStream(ELASTICSEARCH_RESOURCES_FOLDER + "index-" + index + ".json")
            val createIndexRequestBuilder: CreateIndexRequestBuilder = client.admin.indices.prepareCreate(index)
            if (settingsStream != null) {
                try {
                    val source: String = readString(settingsStream, Charset.forName("UTF-8"))
                    createIndexRequestBuilder.setSource(source).get
                    refreshIndex(client, index)
                }
                catch {
                    case e: Exception => {
                        logger.info("error creating index " + index, e)
                    }
                }
            }
        }
        else {
            logger.info("index {} already exists", index)
        }
    }

    /**
      * refresh an ES index
      *
      * @param client
      * @param indices
      */
    def refreshIndex(client: Client, indices: String*) {
        Objects.requireNonNull(client)
        if (indices.length > 0) {
            client.admin.indices.prepareRefresh(indices:_*).get
        }
    }

    /**
      * create an ES template from a json file
      *
      * @param client
      * @param name
      */
    def createTemplate(client: Client, name: String) {
        Objects.requireNonNull(client)
        Objects.requireNonNull(name)
        logger.info("create template - " + name + " begin")
        val response: GetIndexTemplatesResponse = client.admin.indices.prepareGetTemplates(name).get
        if (response.getIndexTemplates == null || response.getIndexTemplates.isEmpty) {
            logger.info("creating template - " + name)
            val settingsStream: InputStream = response.getClass.getResourceAsStream(ELASTICSEARCH_RESOURCES_FOLDER + "template-" + name + ".json")
            if (settingsStream != null) {
                try {
                    val source: String = readString(settingsStream, Charset.forName("UTF-8"))
                    client.admin.indices.preparePutTemplate(name).setSource(source).get
                    refreshIndex(client, name)
                }
                catch {
                    case e: Exception => {
                        logger.warn("error creating template - " + name, e)
                    }
                }
            }
        }
        else {
            logger.info("template {} already exists" + name)
        }
    }

    /**
      * creates an ES type from a json file
      *
      * @param client
      * @param index
      * @param `type`
      */
    def createType(client: Client, index: String, `type`: String) {
        Objects.requireNonNull(client)
        Objects.requireNonNull(index)
        Objects.requireNonNull(`type`)
        val response: TypesExistsResponse = client.admin.indices.prepareTypesExists(index).setTypes(`type`).get
        if (!response.isExists) {
            logger.info("create type " + index + " - " + `type` + " - begin")
            val settingsStream: InputStream = response.getClass.getResourceAsStream(ELASTICSEARCH_RESOURCES_FOLDER + "type-" + index + "-" + `type` + ".json")
            if (settingsStream != null) {
                try {
                    val source: String = readString(settingsStream, Charset.forName("UTF-8"))
                    client.admin.indices.preparePutMapping(index).setType(`type`).setSource(source).execute.actionGet
                    refreshIndex(client, index)
                    logger.info("created type " + index + " - " + `type` + " - begin")
                }
                catch {
                    case e: Exception => {
                        logger.info("error type " + index + " - " + `type` + " - begin", e)
                    }
                }
            }
        }
        else {
            logger.info("already exist type " + index + " - " + `type` + " - begin")
        }
    }

    /**
      * read a string from an InputStream
      *
      * @param is
      * @param charset
      * @return
      */
    private def readString(is: InputStream, charset: Charset): String = {
        try {
            val r: BufferedReader = new BufferedReader(new InputStreamReader(is, charset))
            try {
                var str: String = null
                val sb: StringBuilder = new StringBuilder(8192)
                while ( {
                    str = r.readLine;
                    str
                } != null) {
                    sb.append(str)
                }
                return sb.toString
            }
            catch {
                case ioe: IOException => {
                    ioe.printStackTrace()
                }
            } finally {
                if (r != null) r.close()
            }
        }
        return null
    }

    /**
      * Search for some documents
      *
      * @param esClient
      * @param indexName
      * @param from
      * @param size
      * @return
      */
    def searchAll(esClient: Client,
                  indexName: String,
                  query: String,
                  from: Integer,
                  size: Integer): String = {

        esClient.prepareSearch(indexName)
            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
            .setQuery(QueryBuilders.termQuery("_all", query))
            .setFrom(from).setSize(size).setExplain(true)
            .execute()
            .actionGet()
            .toString
    }


    /**
      * Retrieves a document by its id
      *
      * @param esClient
      * @param indexName
      * @param docType
      * @param docId
      * @return
      */
    def getDocument(esClient: Client,
                    indexName: String,
                    docType: String,
                    docId: String): String = {

        esClient.prepareGet(indexName, docType, docId)
            .get()
            .getSourceAsString
    }


    /**
      * Index a single document
      *
      * @param esClient
      * @param indexName
      * @param documentType
      * @param documentId
      * @param document
      */
    def indexDocument(esClient: Client,
                      indexName: String,
                      documentType: String,
                      documentId: String,
                      document: String) = {
        val response = esClient.prepareIndex(indexName, documentType, documentId)
            .setSource(document)
            .get()

        if(!response.isCreated)
            logger.error(response.toString)
    }


    /**
      * Bulk loads a bunch of documents
      *
      *
      * @param esClient
      * @param indexName
      * @param documentType
      * @param documents
      * @param bulkSize
      */
    def bulkLoadDocuments(esClient: Client,
                          indexName: String,
                          documentType: String,
                          documents: List[String],
                          bulkSize: Int = 10000) = {
        val start = System.currentTimeMillis()
        logger.info("start indexing events")


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

        documents.foreach(document => {
            numItemProcessed += 1

            // Setting ES document id to document's id itself if any (to prevent duplications in ES)
            val idString = UUID.randomUUID.toString
            val result = esClient.prepareIndex(indexName, documentType, idString)
                .setSource(document)
                .setOpType(OpType.CREATE)
            bulkProcessor.add(result.request())
        })

        logger.debug(s"waiting for remaining items to be flushed")
        bulkProcessor.awaitClose(10, TimeUnit.MINUTES)
        logger.debug(s"indexed $numItemProcessed records on this partition to es in ${System.currentTimeMillis() - start}")

        logger.info("shutting down es client")
        // on shutdown
        esClient.close()
    }

}
