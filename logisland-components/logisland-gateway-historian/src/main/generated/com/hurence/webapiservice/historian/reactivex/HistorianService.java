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

package com.hurence.webapiservice.historian.reactivex;

import java.util.Map;
import io.reactivex.Observable;
import io.reactivex.Flowable;
import io.reactivex.Single;
import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.vertx.core.json.JsonObject;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

/**
 * This interface describes the Transactions Manager Service. Note that all methods has same name of corresponding operation id
 *
 * <p/>
 * NOTE: This class has been automatically generated from the {@link com.hurence.webapiservice.historian.HistorianService original} non RX-ified interface using Vert.x codegen.
 */

@io.vertx.lang.rx.RxGen(com.hurence.webapiservice.historian.HistorianService.class)
public class HistorianService {

  @Override
  public String toString() {
    return delegate.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    HistorianService that = (HistorianService) o;
    return delegate.equals(that.delegate);
  }
  
  @Override
  public int hashCode() {
    return delegate.hashCode();
  }

  public static final io.vertx.lang.rx.TypeArg<HistorianService> __TYPE_ARG = new io.vertx.lang.rx.TypeArg<>(    obj -> new HistorianService((com.hurence.webapiservice.historian.HistorianService) obj),
    HistorianService::getDelegate
  );

  private final com.hurence.webapiservice.historian.HistorianService delegate;
  
  public HistorianService(com.hurence.webapiservice.historian.HistorianService delegate) {
    this.delegate = delegate;
  }

  public com.hurence.webapiservice.historian.HistorianService getDelegate() {
    return delegate;
  }

  public com.hurence.webapiservice.historian.reactivex.HistorianService getTimeSeries(JsonObject myParams, Handler<AsyncResult<JsonObject>> myResult) { 
    delegate.getTimeSeries(myParams, myResult);
    return this;
  }

  public Single<JsonObject> rxGetTimeSeries(JsonObject myParams) { 
    return io.vertx.reactivex.impl.AsyncResultSingle.toSingle(handler -> {
      getTimeSeries(myParams, handler);
    });
  }

  /**
   * @param params as a json object <pre> {  : "content of chunks as an array",  : "total chunk matching query",  : ["field1", "field2"...],  : "total chunk matching query",  : "content of chunks as an array", } </pre> explanation : if  not specified will search from 0 if  not specified will search to Max.Long use  if you want to retrieve some of the precalculated aggs. If not specified retrieve all. use  to search for specific timeseries having one of those tags use  to search a specific timeseries name
   * @param resultHandler return chunks of timeseries as an array of <pre> {  : "content of chunks as an array",  : "total chunk matching query", } </pre>
   * @return himself
   */
  public com.hurence.webapiservice.historian.reactivex.HistorianService getTimeSeriesChunk(JsonObject params, Handler<AsyncResult<JsonObject>> resultHandler) { 
    delegate.getTimeSeriesChunk(params, resultHandler);
    return this;
  }

  /**
   * @param params as a json object <pre> {  : "content of chunks as an array",  : "total chunk matching query",  : ["field1", "field2"...],  : "total chunk matching query",  : "content of chunks as an array", } </pre> explanation : if  not specified will search from 0 if  not specified will search to Max.Long use  if you want to retrieve some of the precalculated aggs. If not specified retrieve all. use  to search for specific timeseries having one of those tags use  to search a specific timeseries name
   * @return himself
   */
  public Single<JsonObject> rxGetTimeSeriesChunk(JsonObject params) { 
    return io.vertx.reactivex.impl.AsyncResultSingle.toSingle(handler -> {
      getTimeSeriesChunk(params, handler);
    });
  }

  /**
   * @param params as a json object <pre> {  : "content of chunks as an array",  : "total chunk matching query",  : ["field1", "field2"...],  : "total chunk matching query",  : "content of chunks as an array", } </pre> explanation : if  not specified will search from 0 if  not specified will search to Max.Long use  if you want to retrieve some of the precalculated aggs. If not specified retrieve all. use  to search for specific timeseries having one of those tags use  to search a specific timeseries name
   * @param resultHandler return chunks of timeseries as an array of <pre> {  : "content of chunks as an array",  : "total chunk matching query", } </pre>
   * @return himself
   */
  public com.hurence.webapiservice.historian.reactivex.HistorianService compactTimeSeriesChunk(JsonObject params, Handler<AsyncResult<JsonObject>> resultHandler) { 
    delegate.compactTimeSeriesChunk(params, resultHandler);
    return this;
  }

  /**
   * @param params as a json object <pre> {  : "content of chunks as an array",  : "total chunk matching query",  : ["field1", "field2"...],  : "total chunk matching query",  : "content of chunks as an array", } </pre> explanation : if  not specified will search from 0 if  not specified will search to Max.Long use  if you want to retrieve some of the precalculated aggs. If not specified retrieve all. use  to search for specific timeseries having one of those tags use  to search a specific timeseries name
   * @return himself
   */
  public Single<JsonObject> rxCompactTimeSeriesChunk(JsonObject params) { 
    return io.vertx.reactivex.impl.AsyncResultSingle.toSingle(handler -> {
      compactTimeSeriesChunk(params, handler);
    });
  }

  /**
   * @param params as a json object, it is ignored at the moment TODO
   * @param resultHandler return chunks of timeseries as an array of <pre> {  : "all metric name matching the query",  : "total chunk matching query" } </pre>
   * @return himself
   */
  public com.hurence.webapiservice.historian.reactivex.HistorianService getMetricsName(JsonObject params, Handler<AsyncResult<JsonObject>> resultHandler) { 
    delegate.getMetricsName(params, resultHandler);
    return this;
  }

  /**
   * @param params as a json object, it is ignored at the moment TODO
   * @return himself
   */
  public Single<JsonObject> rxGetMetricsName(JsonObject params) { 
    return io.vertx.reactivex.impl.AsyncResultSingle.toSingle(handler -> {
      getMetricsName(params, handler);
    });
  }


  public static  HistorianService newInstance(com.hurence.webapiservice.historian.HistorianService arg) {
    return arg != null ? new HistorianService(arg) : null;
  }
}
