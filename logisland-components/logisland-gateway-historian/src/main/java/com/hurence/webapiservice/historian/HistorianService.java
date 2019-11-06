package com.hurence.webapiservice.historian;

import com.hurence.webapiservice.historian.impl.SolrHistorianServiceImpl;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.apache.solr.client.solrj.SolrClient;


/**
 * This interface describes the Transactions Manager Service. Note that all methods has same name of corresponding operation id
 */
@ProxyGen
@VertxGen
public interface HistorianService {


    @GenIgnore
    static HistorianService create(Vertx vertx, SolrClient client, String collection, Handler<AsyncResult<HistorianService>> readyHandler) {
        return new SolrHistorianServiceImpl(vertx, client, collection, readyHandler);
    }

    @GenIgnore
    static com.hurence.webapiservice.historian.reactivex.HistorianService createProxy(Vertx vertx, String address) {
        return new com.hurence.webapiservice.historian.reactivex.HistorianService(
                new HistorianServiceVertxEBProxy(vertx, address)
        );
    }

    /**
     * @param params        as a json object
     *                      <pre>
     *                      {
     *                          {@value HistorianFields#FROM_REQUEST_FIELD} : "content of chunks as an array",
     *                          {@value HistorianFields#TO_REQUEST_FIELD} : "total chunk matching query",
     *                          {@value HistorianFields#FIELDS_TO_FETCH_AS_LIST_REQUEST_FIELD} : ["field1", "field2"...],
     *                          {@value HistorianFields#TAGS} : "total chunk matching query",
     *                          {@value HistorianFields#METRIC_NAMES_AS_LIST_REQUEST_FIELD} : "content of chunks as an array",
     *                      }
     *                      </pre>
     *                      explanation :
     *                      if {@value HistorianFields#FROM_REQUEST_FIELD} not specified will search from 0
     *                      if {@value HistorianFields#TO_REQUEST_FIELD} not specified will search to Max.Long
     *                      use {@value HistorianFields#FIELDS_TO_FETCH_AS_LIST_REQUEST_FIELD} if you want to retrieve some of the precalculated aggs. If not specified retrieve all.
     *                      use {@value HistorianFields#TAGS} to search for specific timeseries having one of those tags
     *                      use {@value HistorianFields#METRIC_NAMES_AS_LIST_REQUEST_FIELD} to search a specific timeseries name
     * @param resultHandler return chunks of timeseries as an array of
     *                      <pre>
     *                      {
     *                          {@value HistorianFields#RESPONSE_CHUNKS} : "content of chunks as an array",
     *                          {@value HistorianFields#RESPONSE_TOTAL_FOUND} : "total chunk matching query"
     *                      }
     *                      </pre>
     * @return himself
     */
    @Fluent
    HistorianService getTimeSeriesChunk(JsonObject params, Handler<AsyncResult<JsonObject>> resultHandler);

    /**
     * @param params        as a json object, it is ignored at the moment TODO
     * @param resultHandler return chunks of timeseries as an array of
     *                      <pre>
     *                      {
     *                          {@value HistorianFields#RESPONSE_METRICS} : "all metric name matching the query",
     *                          {@value HistorianFields#RESPONSE_TOTAL_FOUND} : "total chunk matching query"
     *                      }
     *                      </pre>
     * @return himself
     */
    @Fluent
    HistorianService getMetricsName(JsonObject params, Handler<AsyncResult<JsonObject>> resultHandler);

}
