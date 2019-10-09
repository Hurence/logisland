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


/**
 * This interface describes the Transactions Manager Service. Note that all methods has same name of corresponding operation id
 *
 */
@ProxyGen
@VertxGen
public interface HistorianService {

  @GenIgnore
  static HistorianService create(Handler<AsyncResult<HistorianService>> readyHandler) {
    return new SolrHistorianServiceImpl(readyHandler);
  }

  @GenIgnore
  static com.hurence.webapiservice.historian.reactivex.HistorianService createProxy(Vertx vertx, String address) {
    return new com.hurence.webapiservice.historian.reactivex.HistorianService(
            new HistorianServiceVertxEBProxy(vertx, address)
    );
  }

  /**
   *
   * @param params
   * @param resultHandler
   * @return
   */
  @Fluent
  HistorianService getTimeSeries(JsonObject params, Handler<AsyncResult<JsonObject>> resultHandler);

}
