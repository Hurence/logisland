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

import java.net.InetAddress
import java.text.SimpleDateFormat
import java.util.Date

import com.hurence.logisland.event.EventMapper
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.xcontent.XContentFactory._

/**
  * Created by tom on 12/01/16.
  */
object ElasticsearchUtils extends LazyLogging with Serializable {

    val CLUSTER_NAME = "log-island"
    val INDEX_PREFIX = "log-island"

    def createTransportClient(esHosts: String): TransportClient = {
        val settings = Settings.settingsBuilder()
            .put("cluster.name", CLUSTER_NAME)
            .build()

        val hosts = esHosts.split(",")
        val client = TransportClient.builder().build()
        hosts.foreach(host => client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), 9300)))
        logger.debug(s"creating es client for host $esHosts")

        client
    }


    /**
      * create an elasticsearch index
      *
      * @param esHosts
      * @return the index name
      */
    def createIndex(esHosts: String, indexPrefix: String, eventMapper: EventMapper): String = {

        val dateSuffix = new SimpleDateFormat("yyyy.MM.dd").format(new Date())
        val esIndexName = s"$indexPrefix-$dateSuffix"

        val client = createTransportClient(esHosts)
        // create a new index only if not already exists
        val res = client.admin().indices().prepareExists(esIndexName).execute().actionGet()
        if (!res.isExists()) {
            //val delIdx = client.admin().indices().prepareDelete(esIndexName)
            //delIdx.execute().actionGet()

            val createIndexRequestBuilder = client.admin().indices().prepareCreate(esIndexName)

            // MAPPING GOES HERE
            createIndexRequestBuilder.addMapping("_default_",
                jsonBuilder().startObject()
                    .startObject("_default_")
                    .startObject("_source")
                    .field("enabled", "true")
                    .endObject()
                    .endObject()
                    .endObject())

            createIndexRequestBuilder.addMapping(eventMapper.getDocumentType, eventMapper.getMapping)

            // MAPPING DONE
            createIndexRequestBuilder
                .setSettings(Settings.settingsBuilder().put("number_of_shards", 3).put("number_of_replicas", 0))
                .execute().actionGet()
        }

        client.close()
        esIndexName
    }

}
