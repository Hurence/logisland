package com.hurence.webapiservice.historian;

import com.hurence.logisland.record.FieldDictionary;
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

    public static String CHUNKS = "chunks";
    public static String METRICS = "metrics";
    public static String TOTAL_FOUND = "total_hit";
    public static String FROM = "from";
    public static String TO = "to";
    public static String FIELDS_TO_FETCH = "fields";
    public static String TAGS = "tags";
    public static String NAMES = "names";
    public static String MAX_TOTAL_CHUNKS_TO_RETRIEVE = "total_max_chunks";

    //Response fields
    public static String METRIC_NAME = "name";
    public static String CHUNK_ID = "id";
    public static String CHUNK_VERSION = "_version_";
    public static String CHUNK_VALUE = FieldDictionary.CHUNK_VALUE;
    public static String CHUNK_MAX = FieldDictionary.CHUNK_MAX;
    public static String CHUNK_MIN = FieldDictionary.CHUNK_MIN;
    public static String CHUNK_START = FieldDictionary.CHUNK_START;
    public static String CHUNK_END = FieldDictionary.CHUNK_END;
    public static String CHUNK_AVG = FieldDictionary.CHUNK_AVG;
    public static String CHUNK_SIZE = FieldDictionary.CHUNK_SIZE;
    public static String CHUNK_SUM = FieldDictionary.CHUNK_SUM;
    public static String CHUNK_SAX = FieldDictionary.CHUNK_SAX;
    public static String CHUNK_WINDOW_MS = FieldDictionary.CHUNK_WINDOW_MS;
    public static String CHUNK_TREND = FieldDictionary.CHUNK_TREND;
    public static String CHUNK_SIZE_BYTES = FieldDictionary.CHUNK_SIZE_BYTES;


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
     *                          {@value FROM} : "content of chunks as an array",
     *                          {@value TO} : "total chunk matching query",
     *                          {@value FIELDS_TO_FETCH} : ["field1", "field2"...],
     *                          {@value TAGS} : "total chunk matching query",
     *                          {@value NAMES} : "content of chunks as an array",
     *                      }
     *                      </pre>
     *                      explanation :
     *                      if {@value FROM} not specified will search from 0
     *                      if {@value TO} not specified will search to Max.Long
     *                      use {@value FIELDS_TO_FETCH} if you want to retrieve some of the precalculated aggs. If not specified retrieve all.
     *                      use {@value TAGS} to search for specific timeseries having one of those tags
     *                      use {@value NAMES} to search a specific timeseries name
     * @param resultHandler return chunks of timeseries as an array of
     *                      <pre>
     *                      {
     *                          {@value CHUNKS} : "content of chunks as an array",
     *                          {@value TOTAL_FOUND} : "total chunk matching query"
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
     *                          {@value METRICS} : "all metric name matching the query",
     *                          {@value TOTAL_FOUND} : "total chunk matching query"
     *                      }
     *                      </pre>
     * @return himself
     */
    @Fluent
    HistorianService getMetricsName(JsonObject params, Handler<AsyncResult<JsonObject>> resultHandler);

}
