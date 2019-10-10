package com.hurence.webapiservice.historian.impl;

import com.hurence.webapiservice.historian.HistorianService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SolrHistorianServiceImpl implements HistorianService {

  private static Logger logger = LoggerFactory.getLogger(SolrHistorianServiceImpl.class);

  private final SolrClient client;

  public SolrHistorianServiceImpl(SolrClient client, Handler<AsyncResult<HistorianService>> readyHandler) {
    this.client = client;
    try {
      client.ping();
    } catch (IOException | SolrServerException ex) {

    }

    readyHandler.handle(Future.succeededFuture(this));
  }


  @Override
  public HistorianService getTimeSeries(JsonObject params, Handler<AsyncResult<JsonObject>> resultHandler) {
//    client.query();
    //TODO
    return this;
  }
}
