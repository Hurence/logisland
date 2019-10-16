package com.hurence.webapiservice.timeseries;

import com.hurence.webapiservice.timeseries.impl.TimeseriesServiceImpl;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;


/**
 * This interface describes the Transactions Manager Service. Note that all methods has same name of corresponding operation id
 *
 */
@ProxyGen
@VertxGen
public interface TimeseriesService {

  public static String DEFAULT_ADRESS = "timeseries";

  @GenIgnore
  static TimeseriesService create(Vertx vertx, Handler<AsyncResult<TimeseriesService>> readyHandler) {
    return new TimeseriesServiceImpl(vertx, readyHandler);
  }

  @GenIgnore
  static com.hurence.webapiservice.timeseries.reactivex.TimeseriesService createProxy(Vertx vertx, String address) {
    return new com.hurence.webapiservice.timeseries.reactivex.TimeseriesService(
            new TimeseriesServiceVertxEBProxy(vertx, address)
    );
  }

  /**
   *
   * @param params
   * @param resultHandler
   * @return
   */
  @Fluent
  TimeseriesService unCompressTimeSeries(JsonObject params, Handler<AsyncResult<JsonArray>> resultHandler);

}
