package com.hurence.webapiservice.historian.impl;

import com.hurence.webapiservice.historian.HistorianService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrHistorianServiceImpl implements HistorianService {

  private static Logger logger = LoggerFactory.getLogger(SolrHistorianServiceImpl.class);

  public SolrHistorianServiceImpl(Handler<AsyncResult<HistorianService>> readyHandler) {
    readyHandler.handle(Future.succeededFuture(this));
  }


  @Override
  public HistorianService getTimeSeries(JsonObject params, Handler<AsyncResult<JsonObject>> resultHandler) {
    //TODO
    return this;
  }
}
