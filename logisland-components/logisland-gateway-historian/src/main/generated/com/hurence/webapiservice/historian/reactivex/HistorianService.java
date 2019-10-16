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

  /**
   * @param params 
   * @param resultHandler 
   * @return 
   */
  public com.hurence.webapiservice.historian.reactivex.HistorianService getTimeSeries(JsonObject params, Handler<AsyncResult<JsonObject>> resultHandler) { 
    delegate.getTimeSeries(params, resultHandler);
    return this;
  }

  /**
   * @param params 
   * @return 
   */
  public Single<JsonObject> rxGetTimeSeries(JsonObject params) { 
    return io.vertx.reactivex.impl.AsyncResultSingle.toSingle(handler -> {
      getTimeSeries(params, handler);
    });
  }


  public static  HistorianService newInstance(com.hurence.webapiservice.historian.HistorianService arg) {
    return arg != null ? new HistorianService(arg) : null;
  }
}
