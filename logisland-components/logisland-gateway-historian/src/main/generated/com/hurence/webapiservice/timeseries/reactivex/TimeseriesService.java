/*
 * Copyright 2014 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.hurence.webapiservice.timeseries.reactivex;

import java.util.Map;
import io.reactivex.Observable;
import io.reactivex.Flowable;
import io.reactivex.Single;
import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

/**
 * This interface describes the Transactions Manager Service. Note that all methods has same name of corresponding operation id
 *
 *
 * <p/>
 * NOTE: This class has been automatically generated from the {@link com.hurence.webapiservice.timeseries.TimeseriesService original} non RX-ified interface using Vert.x codegen.
 */

@io.vertx.lang.rx.RxGen(com.hurence.webapiservice.timeseries.TimeseriesService.class)
public class TimeseriesService {

  @Override
  public String toString() {
    return delegate.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TimeseriesService that = (TimeseriesService) o;
    return delegate.equals(that.delegate);
  }
  
  @Override
  public int hashCode() {
    return delegate.hashCode();
  }

  public static final io.vertx.lang.rx.TypeArg<TimeseriesService> __TYPE_ARG = new io.vertx.lang.rx.TypeArg<>(    obj -> new TimeseriesService((com.hurence.webapiservice.timeseries.TimeseriesService) obj),
    TimeseriesService::getDelegate
  );

  private final com.hurence.webapiservice.timeseries.TimeseriesService delegate;
  
  public TimeseriesService(com.hurence.webapiservice.timeseries.TimeseriesService delegate) {
    this.delegate = delegate;
  }

  public com.hurence.webapiservice.timeseries.TimeseriesService getDelegate() {
    return delegate;
  }

  /**
   * @param params as a json object <pre> {  : "a byte array encoded as base64 to uncompress to timeseries",  : "a long value"  : "a long value" (optional) } </pre>
   * @param resultHandler 
   * @return uncompressed timeseries as an array of <pre> {  : "a_timestamp",  : "a double value" } </pre>
   */
  public com.hurence.webapiservice.timeseries.reactivex.TimeseriesService unCompressTimeSeries(JsonObject params, Handler<AsyncResult<JsonArray>> resultHandler) { 
    delegate.unCompressTimeSeries(params, resultHandler);
    return this;
  }

  /**
   * @param params as a json object <pre> {  : "a byte array encoded as base64 to uncompress to timeseries",  : "a long value"  : "a long value" (optional) } </pre>
   * @return uncompressed timeseries as an array of <pre> {  : "a_timestamp",  : "a double value" } </pre>
   */
  public Single<JsonArray> rxUnCompressTimeSeries(JsonObject params) { 
    return io.vertx.reactivex.impl.AsyncResultSingle.toSingle(handler -> {
      unCompressTimeSeries(params, handler);
    });
  }

  public static final String DEFAULT_ADRESS = com.hurence.webapiservice.timeseries.TimeseriesService.DEFAULT_ADRESS;
  public static final String TIMESTAMP = com.hurence.webapiservice.timeseries.TimeseriesService.TIMESTAMP;
  public static final String VALUE = com.hurence.webapiservice.timeseries.TimeseriesService.VALUE;
  public static final String CHUNK = com.hurence.webapiservice.timeseries.TimeseriesService.CHUNK;
  public static final String START = com.hurence.webapiservice.timeseries.TimeseriesService.START;
  public static final String END = com.hurence.webapiservice.timeseries.TimeseriesService.END;

  public static  TimeseriesService newInstance(com.hurence.webapiservice.timeseries.TimeseriesService arg) {
    return arg != null ? new TimeseriesService(arg) : null;
  }
}
