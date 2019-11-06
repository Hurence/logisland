package com.hurence.webapiservice.historian.impl;

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
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.stream.Collectors;

import static com.hurence.webapiservice.historian.HistorianFields.*;

public class SolrHistorianServiceImpl implements HistorianService {

  private static Logger logger = LoggerFactory.getLogger(SolrHistorianServiceImpl.class);

  private final SolrClient client;
  private final Vertx vertx;
  private final String collection;
  private final BinaryCompactionConverter compacter;

  public SolrHistorianServiceImpl(Vertx vertx, SolrClient client, String collection, Handler<AsyncResult<HistorianService>> readyHandler) {
    this.client = client;
    this.vertx = vertx;
    this.collection = collection;
    this.compacter = new BinaryCompactionConverter.Builder().build();
    Handler<Promise<Integer>> colPinghandler = createPingHandler(6000,  3);
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

  private Handler<Promise<Integer>> createPingHandler(long sleepDurationMilli, int numberOfRetry) {
    return p -> {
      try {
        p.complete(pingSolrServer(6000,  3));
      } catch (IOException e) {
        logger.error("IOException while pinging solr",e);
        p.fail(e);
      } catch (SolrServerException e) {
        logger.error("SolrServerException while pinging solr",e);
        p.fail(e);
      }
    };
  }

  private Integer pingSolrServer(long sleepDurationMilli, int numberOfRetry) throws IOException, SolrServerException {
    try {
      final SolrRequest request = CollectionAdminRequest.collectionStatus(collection);
      final NamedList<Object> rsp = client.request(request);
      final NamedList<Object> responseHeader = (NamedList<Object>) rsp.get("responseHeader");
      int status = (int) responseHeader.get("status");
      return status;
    } catch (IOException | SolrServerException e) {
      throw e;
    } catch (SolrException e) {
      logger.warn("Could not connect so solr");
      if (numberOfRetry == 0)
        throw e;
      logger.info("waiting {} ms before retrying.", sleepDurationMilli);
      try {
        Thread.sleep(sleepDurationMilli);
      } catch (InterruptedException ex) {
        logger.error("InterruptedException exception", e);
        throw e;
      }
      int triesLeft = numberOfRetry - 1;
      logger.info("Retrying to connect to solr, {} {} left.", triesLeft, triesLeft == 1 ? "try" : "tries");
      return pingSolrServer(sleepDurationMilli, triesLeft);
    }
  }

  @Override
  public HistorianService getTimeSeriesChunk(JsonObject params, Handler<AsyncResult<JsonObject>> resultHandler) {
    //    SEARCH
    StringBuilder queryBuilder = new StringBuilder();
    if (params.getLong(TO_REQUEST_FIELD) != null) {
      queryBuilder.append(RESPONSE_CHUNK_START_FIELD).append(":[* TO ").append(params.getLong(TO_REQUEST_FIELD)).append("]");
    }
    if (params.getLong(FROM_REQUEST_FIELD) != null) {
      if (queryBuilder.length() != 0)
        queryBuilder.append(" AND ");
      queryBuilder.append(RESPONSE_CHUNK_END_FIELD).append(":[").append(params.getLong(FROM_REQUEST_FIELD)).append(" TO *]");
    }
    //
    SolrQuery query = new SolrQuery("*:*");
    if (queryBuilder.length() != 0)
      query.setQuery(queryBuilder.toString());
    //    FILTER
    if (params.getJsonArray(TAGS) != null) {
      logger.error("TODO there is tags");//TODO
    }
    if (params.getJsonArray(METRIC_NAMES_AS_LIST_REQUEST_FIELD) != null && !params.getJsonArray(METRIC_NAMES_AS_LIST_REQUEST_FIELD).isEmpty()) {
      if (params.getJsonArray(METRIC_NAMES_AS_LIST_REQUEST_FIELD).size() == 1) {
        query.addFilterQuery(RESPONSE_METRIC_NAME_FIELD + ":" + params.getJsonArray(METRIC_NAMES_AS_LIST_REQUEST_FIELD).getString(0));
      } else {
        String orNames = params.getJsonArray(METRIC_NAMES_AS_LIST_REQUEST_FIELD).stream()
                .map(String.class::cast)
                .collect(Collectors.joining(" OR ", "(", ")"));
        query.addFilterQuery(RESPONSE_METRIC_NAME_FIELD + ":" + orNames);
      }
    }
    //    FIELDS_TO_FETCH
    if (params.getJsonArray(FIELDS_TO_FETCH_AS_LIST_REQUEST_FIELD) != null) {
      JsonArray fields = params.getJsonArray(FIELDS_TO_FETCH_AS_LIST_REQUEST_FIELD);
      fields.stream().forEach(field -> {
        if (field instanceof String) {
          query.addField((String) field);
        } else {
          logger.error("agg {} should be a string but was {} instead", field, field.getClass());
        }
      });
    }
    //    SORT
    query.setSort(RESPONSE_CHUNK_START_FIELD, SolrQuery.ORDER.asc);
    query.setRows(params.getInteger(MAX_TOTAL_CHUNKS_TO_RETRIEVE_REQUEST_FIELD, 1000));
    //  EXECUTE REQUEST
    Handler<Promise<JsonObject>> getTimeSeriesHandler = p -> {
      try {
        final QueryResponse response = client.query(collection, query);
        final SolrDocumentList documents = response.getResults();
        logger.debug("Found " + documents.getNumFound() + " documents");
        JsonArray docs = new JsonArray(documents.stream()
                .map(this::convertDoc)
                .collect(Collectors.toList())
        );
        p.complete(new JsonObject()
                .put(RESPONSE_TOTAL_FOUND, documents.getNumFound())
                .put(RESPONSE_CHUNKS, docs)
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

  @Override
  public HistorianService getMetricsName(JsonObject params, Handler<AsyncResult<JsonObject>> resultHandler) {
    SolrQuery query = new SolrQuery("*:*");
    //TODO search a syntax for metric
    query.setRows(0);//we only need distinct values of metrics
//    query.setFacet(true);
//    query.setFacetSort("index");
//    query.setFacetLimit(0);
    query.addFacetField(RESPONSE_METRIC_NAME_FIELD);
    //  EXECUTE REQUEST
    Handler<Promise<JsonObject>> getMetricsNameHandler = p -> {
      try {
        final QueryResponse response = client.query(collection, query);
        FacetField facetField = response.getFacetField(RESPONSE_METRIC_NAME_FIELD);
        FacetField.Count count = facetField.getValues().get(0);
        count.getCount();
        count.getName();
        count.getAsFilterQuery();
        count.getFacetField();
        logger.debug("Found " + facetField.getValueCount() + " different values");
        JsonArray metrics = new JsonArray(facetField.getValues().stream()
                .map(FacetField.Count::getName)
                .collect(Collectors.toList())
        );
        p.complete(new JsonObject()
                .put(RESPONSE_TOTAL_FOUND, facetField.getValueCount())
                .put(RESPONSE_METRICS, metrics)
        );
      } catch (IOException | SolrServerException e) {
        p.fail(e);
      } catch (Exception e) {
        logger.error("unexpected exception");
        p.fail(e);
      }
    };
    vertx.executeBlocking(getMetricsNameHandler, resultHandler);
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
