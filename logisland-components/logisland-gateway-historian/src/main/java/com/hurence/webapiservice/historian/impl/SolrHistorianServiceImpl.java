package com.hurence.webapiservice.historian.impl;

import com.hurence.logisland.record.Point;
import com.hurence.logisland.timeseries.converter.compaction.BinaryCompactionConverter;
import com.hurence.webapiservice.historian.HistorianService;
import io.vertx.core.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class SolrHistorianServiceImpl implements HistorianService {

  private static Logger logger = LoggerFactory.getLogger(SolrHistorianServiceImpl.class);
//  UNCOMPRESSION
  public static String TIMESTAMP = "timestamp";
  public static String VALUE = "value";
  public static String CHUNK = "chunk";
  public static String START = "start";
  public static String END = "end";
//   SEARCH
  public static String DOCS = "docs";
  public static String TOTAL_FOUND = "total_hit";

  private final SolrClient client;
  private final Vertx vertx;
  private final String collection;
  private final BinaryCompactionConverter compacter;

  public SolrHistorianServiceImpl(Vertx vertx, SolrClient client, String collection, Handler<AsyncResult<HistorianService>> readyHandler) {
    this.client = client;
    this.vertx = vertx;
    this.collection = collection;
    this.compacter = new BinaryCompactionConverter.Builder().build();
    Handler<Promise<Integer>> colPinghandler = p -> {
      try {
        final SolrRequest request = CollectionAdminRequest.collectionStatus(collection);
        final NamedList<Object> rsp = client.request(request);
        final NamedList<Object> responseHeader = (NamedList<Object>) rsp.get("responseHeader");
        int status = (int) responseHeader.get("status");
        p.complete(status);
      } catch (IOException|SolrServerException e) {
        p.fail(e);
      } catch (Exception e) {
        logger.error("unexpected exception");
        p.fail(e);
      }
    };
    Handler<AsyncResult<Integer>> statusHandler = h -> {
      if (h.succeeded()) {
        if (h.result() == 0) {
          readyHandler.handle(Future.succeededFuture(this));
        } else {
          readyHandler.handle(Future.failedFuture(new IllegalArgumentException(
                  String.format("historian collection ping command returned status %d", h.result())
          )));
        }
      } else {
        readyHandler.handle(Future.failedFuture(h.cause()));
      }
    };
    vertx.executeBlocking(colPinghandler, statusHandler);
  }


  @Override
  public HistorianService getTimeSeries(JsonObject params, Handler<AsyncResult<JsonObject>> resultHandler) {
    String queryStr = "*:*";//TODO build correct string depending on params (or query should be given as param ?
    SolrQuery query = new SolrQuery(queryStr);
//    query.addField("chunk_value");//TODO change default ? can select what we want

    Handler<Promise<JsonObject>> getTimeSeriesHandler = p -> {
      try {
        final QueryResponse response = client.query(collection, query);
        final SolrDocumentList documents = response.getResults();
        logger.info("Found " + documents.getNumFound() + " documents");
        JsonArray docs = new JsonArray(documents.stream()
                .map(this::convertDoc)
                .collect(Collectors.toList())
        );
        p.complete(new JsonObject()
                .put(TOTAL_FOUND, documents.getNumFound())
                .put(DOCS, docs)
        );
      } catch (IOException | SolrServerException e) {
        p.fail(e);
      } catch (Exception e) {
        logger.error("unexpected exception");
        p.fail(e);
      }
    };
    vertx.executeBlocking(getTimeSeriesHandler, resultHandler);
    return this;
  }

  private JsonObject convertDoc(SolrDocument doc) {
    final JsonObject json = new JsonObject();
    doc.getFieldNames().forEach(f -> {
          json.put(f, doc.get(f));
    });
    return json;
  }
}
