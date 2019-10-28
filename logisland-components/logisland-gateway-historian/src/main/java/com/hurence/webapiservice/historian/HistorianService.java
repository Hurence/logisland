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
 *
 */
@ProxyGen
@VertxGen
public interface HistorianService {

  public static String CHUNKS = "chunks";
  public static String TOTAL_FOUND = "total_hit";
  public static String FROM = "from";
  public static String TO = "to";
  public static String FIELDS_TO_FETCH = "fields";
  public static String TAGS = "tags";
  public static String NAMES = "names";

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
   *
   * @param params as a json object
   * <pre>
   * {
   *     {@value FROM} : "content of chunks as an array",
   *     {@value TO} : "total chunk matching query",
   *     {@value FIELDS_TO_FETCH} : ["field1", "field2"...],
   *     {@value TAGS} : "total chunk matching query",
   *     {@value NAMES} : "content of chunks as an array",
   * }
   * </pre>
   * explanation :
   *    if {@value FROM} not specified will search from 0
   *    if {@value TO} not specified will search to Max.Long
   *    use {@value FIELDS_TO_FETCH} if you want to retrieve some of the precalculated aggs. If not specified retrieve all.
   *    use {@value TAGS} to search for specific timeseries having one of those tags
   *    use {@value NAMES} to search a specific timeseries name
   *
   * @param resultHandler
   * @return uncompressed timeseries as an array of
   * <pre>
   * {
   *     {@value CHUNKS} : "content of chunks as an array",
   *     {@value TOTAL_FOUND} : "total chunk matching query"
   * }
   * DOCS contains at minimum chunk_value, chunk_start
   * </pre>
   */
  @Fluent
  HistorianService getTimeSeriesChunk(JsonObject params, Handler<AsyncResult<JsonObject>> resultHandler);

}
