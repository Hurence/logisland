package com.hurence.webapiservice.historian.impl;

import com.hurence.logisland.timeseries.sampling.SamplingAlgorithm;
import com.hurence.webapiservice.historian.HistorianService;
import com.hurence.webapiservice.http.compaction.LazyCompactor;
import com.hurence.webapiservice.http.compaction.LazyDocumentLoader;
import com.hurence.webapiservice.http.compaction.SolrFacetService;
import com.hurence.webapiservice.modele.SamplingConf;
import com.hurence.webapiservice.timeseries.MultiTimeSeriesExtracter;
import com.hurence.webapiservice.timeseries.MultiTimeSeriesExtracterImpl;
import com.hurence.webapiservice.timeseries.MultiTimeSeriesExtractorUsingPreAgg;
import com.hurence.webapiservice.timeseries.TimeSeriesExtracterUtil;
import io.vertx.core.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.SolrStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hurence.webapiservice.historian.HistorianFields.*;
import static com.hurence.webapiservice.http.compaction.CompactionHandlerParams.*;
import static com.hurence.webapiservice.http.compaction.CompactionHandlerParams.PAGE_SIZE;
import static java.lang.String.join;
import static org.apache.commons.lang3.StringUtils.isBlank;

public class SolrHistorianServiceImpl implements HistorianService {

    private static Logger LOGGER = LoggerFactory.getLogger(SolrHistorianServiceImpl.class);

    private final Vertx vertx;
    private final SolrHistorianConf solrHistorianConf;

    public SolrHistorianServiceImpl(Vertx vertx, SolrHistorianConf solrHistorianConf,
                                    Handler<AsyncResult<HistorianService>> readyHandler) {
        this.vertx = vertx;
        this.solrHistorianConf = solrHistorianConf;
        LOGGER.debug("SolrHistorianServiceImpl with params:");
        LOGGER.debug("collection : {}", solrHistorianConf.collection);
        LOGGER.debug("streamEndPoint : {}", solrHistorianConf.streamEndPoint);
        LOGGER.debug("limitNumberOfPoint : {}", solrHistorianConf.limitNumberOfPoint);
        LOGGER.debug("limitNumberOfChunks : {}", solrHistorianConf.limitNumberOfChunks);
        Handler<Promise<Integer>> colPinghandler = createPingHandler(solrHistorianConf.sleepDurationBetweenTry, solrHistorianConf.numberOfRetryToConnect);
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
                p.complete(pingSolrServer(6000, 3));
            } catch (IOException e) {
                LOGGER.error("IOException while pinging solr", e);
                p.fail(e);
            } catch (SolrServerException e) {
                LOGGER.error("SolrServerException while pinging solr", e);
                p.fail(e);
            }
        };
    }

    private Integer pingSolrServer(long sleepDurationMilli, int numberOfRetry) throws IOException, SolrServerException {
        try {
            final SolrRequest request = CollectionAdminRequest.collectionStatus(solrHistorianConf.collection);
            final NamedList<Object> rsp = solrHistorianConf.client.request(request);
            final NamedList<Object> responseHeader = (NamedList<Object>) rsp.get("responseHeader");
            int status = (int) responseHeader.get("status");
            return status;
        } catch (IOException | SolrServerException e) {
            throw e;
        } catch (SolrException e) {
            LOGGER.warn("Could not connect so solr");
            if (numberOfRetry == 0)
                throw e;
            LOGGER.info("waiting {} ms before retrying.", sleepDurationMilli);
            try {
                Thread.sleep(sleepDurationMilli);
            } catch (InterruptedException ex) {
                LOGGER.error("InterruptedException exception", e);
                throw e;
            }
            int triesLeft = numberOfRetry - 1;
            LOGGER.info("Retrying to connect to solr, {} {} left.", triesLeft, triesLeft == 1 ? "try" : "tries");
            return pingSolrServer(sleepDurationMilli, triesLeft);
        }
    }

    @Override
    public HistorianService getTimeSeriesChunk(JsonObject params, Handler<AsyncResult<JsonObject>> resultHandler) {
        final SolrQuery query = buildTimeSeriesChunkQuery(params);
        query.setFields();//so we return every fields (this endpoint is currently used only in tests, this is legacy code)
        //  EXECUTE REQUEST
        Handler<Promise<JsonObject>> getTimeSeriesHandler = p -> {
            try {
                final QueryResponse response = solrHistorianConf.client.query(solrHistorianConf.collection, query);
                final SolrDocumentList documents = response.getResults();
                LOGGER.debug("Found " + documents.getNumFound() + " documents");
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
                LOGGER.error("unexpected exception");
                p.fail(e);
            }
        };
        vertx.executeBlocking(getTimeSeriesHandler, resultHandler);
        return this;
    }

    @Override
    public HistorianService compactTimeSeriesChunk(JsonObject params, Handler<AsyncResult<JsonObject>> resultHandler) {
        final SolrQuery query = buildTimeSeriesChunkQuery(params);
        query.setFields();//so we return every fields (this endpoint is currently used only in tests, this is legacy code)
        //  EXECUTE REQUEST
        Handler<Promise<JsonObject>> getTimeSeriesHandler = p -> {
            try {


                final QueryResponse response = solrHistorianConf.client.query(solrHistorianConf.collection, query);
                final SolrDocumentList documents = response.getResults();

                LOGGER.debug("Found " + documents.getNumFound() + " documents");
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
                LOGGER.error("unexpected exception");
                p.fail(e);
            }
        };
        vertx.executeBlocking(getTimeSeriesHandler, resultHandler);
        return this;
    }

    private SolrQuery buildTimeSeriesChunkQuery(JsonObject params) {
        StringBuilder queryBuilder = new StringBuilder();
        if (params.getLong(TO_REQUEST_FIELD) != null) {
            LOGGER.trace("requesting timeseries to {}", params.getLong(TO_REQUEST_FIELD));
            queryBuilder.append(RESPONSE_CHUNK_START_FIELD).append(":[* TO ").append(params.getLong(TO_REQUEST_FIELD)).append("]");
        }
        if (params.getLong(FROM_REQUEST_FIELD) != null) {
            LOGGER.trace("requesting timeseries from {}", params.getLong(FROM_REQUEST_FIELD));
            if (queryBuilder.length() != 0)
                queryBuilder.append(" AND ");
            queryBuilder.append(RESPONSE_CHUNK_END_FIELD).append(":[").append(params.getLong(FROM_REQUEST_FIELD)).append(" TO *]");
        }
        //
        SolrQuery query = new SolrQuery("*:*");
        if (queryBuilder.length() != 0)
            query.setQuery(queryBuilder.toString());
        //    FILTER
        buildSolrFilterFromArray(params.getJsonArray(TAGS_TO_FILTER_ON_REQUEST_FIELD), RESPONSE_TAG_NAME_FIELD)
                .ifPresent(query::addFilterQuery);
        buildSolrFilterFromArray(params.getJsonArray(METRIC_NAMES_AS_LIST_REQUEST_FIELD), RESPONSE_METRIC_NAME_FIELD)
                .ifPresent(query::addFilterQuery);
        //    FIELDS_TO_FETCH
        query.setFields(RESPONSE_CHUNK_START_FIELD,
                RESPONSE_CHUNK_END_FIELD,
                RESPONSE_CHUNK_SIZE_FIELD,
                RESPONSE_METRIC_NAME_FIELD);
        //    SORT
        query.setSort(RESPONSE_CHUNK_START_FIELD, SolrQuery.ORDER.asc);
        query.addSort(RESPONSE_CHUNK_END_FIELD, SolrQuery.ORDER.asc);
        query.setRows(params.getInteger(MAX_TOTAL_CHUNKS_TO_RETRIEVE_REQUEST_FIELD, 50000));
        return query;
    }

    private Optional<String> buildSolrFilterFromArray(JsonArray jsonArray, String responseMetricNameField) {
        if (jsonArray == null || jsonArray.isEmpty())
            return Optional.empty();
        if (jsonArray.size() == 1) {
            return Optional.of(responseMetricNameField + ":" + jsonArray.getString(0));
        } else {
            String orNames = jsonArray.stream()
                    .map(String.class::cast)
                    .collect(Collectors.joining(" OR ", "(", ")"));
            return Optional.of(responseMetricNameField + ":" + orNames);
        }
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
                final QueryResponse response = solrHistorianConf.client.query(solrHistorianConf.collection, query);
                FacetField facetField = response.getFacetField(RESPONSE_METRIC_NAME_FIELD);
                FacetField.Count count = facetField.getValues().get(0);
                count.getCount();
                count.getName();
                count.getAsFilterQuery();
                count.getFacetField();
                LOGGER.debug("Found " + facetField.getValueCount() + " different values");
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
                LOGGER.error("unexpected exception");
                p.fail(e);
            }
        };
        vertx.executeBlocking(getMetricsNameHandler, resultHandler);
        return this;
    }

    /**
     * nombre point < LIMIT_TO_DEFINE ==> Extract points from chunk
     * nombre point >= LIMIT_TO_DEFINE && nombre de chunk < LIMIT_TO_DEFINE ==> Sample points with chunk aggs depending on alg (min, avg)
     * nombre de chunk >= LIMIT_TO_DEFINE ==> Sample points with chunk aggs depending on alg (min, avg),
     * but should using agg on solr side (using key partition, by month, daily ? yearly ?)
     */
    @Override
    public HistorianService getTimeSeries(JsonObject myParams, Handler<AsyncResult<JsonObject>> myResult) {
        final SolrQuery query = buildTimeSeriesChunkQuery(myParams);
        Handler<Promise<JsonObject>> getTimeSeriesHandler = p -> {
            MetricsSizeInfo metricsInfo;
            try {
                metricsInfo = getNumberOfPointsByMetricInRequest(query);
                LOGGER.debug("metrics info to query : {}", metricsInfo);
                if (metricsInfo.isEmpty()) {
                    final MultiTimeSeriesExtracter timeSeriesExtracter = createTimeSerieExtractorSamplingAllPoints(myParams, metricsInfo);
                    p.complete(buildTimeSeriesResponse(timeSeriesExtracter));
                    return;
                }
                final MultiTimeSeriesExtracter timeSeriesExtracter = getMultiTimeSeriesExtracter(myParams, query, metricsInfo);
                requestSolrAndbuildTimeSeries(query, p, timeSeriesExtracter);
            } catch (IOException e) {
                LOGGER.error("unexpected exception", e);
                p.fail(e);
            }
        };
        vertx.executeBlocking(getTimeSeriesHandler, myResult);
        return this;
    }

    public MultiTimeSeriesExtracter getMultiTimeSeriesExtracter(JsonObject myParams, SolrQuery query, MetricsSizeInfo metricsInfo) {
        //TODO make three different group for each metrics, not use a single strategy globally for all metrics.
        final MultiTimeSeriesExtracter timeSeriesExtracter;
        if (metricsInfo.getTotalNumberOfPoints() < solrHistorianConf.limitNumberOfPoint ||
                metricsInfo.getTotalNumberOfPoints() <= getSamplingConf(myParams).getMaxPoint()) {
            LOGGER.debug("QUERY MODE 1: metricsInfo.getTotalNumberOfPoints() < limitNumberOfPoint");
            query.addField(RESPONSE_CHUNK_VALUE_FIELD);
            timeSeriesExtracter = createTimeSerieExtractorSamplingAllPoints(myParams, metricsInfo);
        } else if (metricsInfo.getTotalNumberOfChunks() < solrHistorianConf.limitNumberOfChunks) {
            LOGGER.debug("QUERY MODE 2: metricsInfo.getTotalNumberOfChunks() < limitNumberOfChunks");
            addFieldsThatWillBeNeededBySamplingAlgorithms(myParams, query, metricsInfo);
            timeSeriesExtracter = createTimeSerieExtractorUsingChunks(myParams, metricsInfo);
        } else {
            LOGGER.debug("QUERY MODE 3 : else");
            //TODO Sample points with chunk aggs depending on alg (min, avg),
            // but should using agg on solr side (using key partition, by month, daily ? yearly ?)
            // For the moment we use the stream api without partitionning
            addFieldsThatWillBeNeededBySamplingAlgorithms(myParams, query, metricsInfo);
            timeSeriesExtracter = createTimeSerieExtractorUsingChunks(myParams, metricsInfo);
        }
        return timeSeriesExtracter;
    }

    public void requestSolrAndbuildTimeSeries(SolrQuery query, Promise<JsonObject> p, MultiTimeSeriesExtracter timeSeriesExtracter) {
        try (JsonStream stream = queryStream(query)) {
            JsonObject timeseries = extractTimeSeriesThenBuildResponse(stream, timeSeriesExtracter);
            p.complete(timeseries);
        } catch (Exception e) {
            LOGGER.error("unexpected exception", e);
            p.fail(e);
        }
    }

    public void addFieldsThatWillBeNeededBySamplingAlgorithms(JsonObject myParams, SolrQuery query, MetricsSizeInfo metricsInfo) {
        SamplingConf requestedSamplingConf = getSamplingConf(myParams);
        Set<SamplingAlgorithm> samplingAlgos = determineSamplingAlgoThatWillBeUsed(requestedSamplingConf, metricsInfo);
        addNecessaryFieldToQuery(query, samplingAlgos);
    }

    private void addNecessaryFieldToQuery(SolrQuery query, Set<SamplingAlgorithm> samplingAlgos) {
        samplingAlgos.forEach(algo -> {
            switch (algo) {
                case NONE:
                    query.addField(RESPONSE_CHUNK_VALUE_FIELD);
                    break;
                case FIRST_ITEM:
                    query.addField(RESPONSE_CHUNK_FIRST_VALUE_FIELD);
                    break;
                case AVERAGE:
                    query.addField(RESPONSE_CHUNK_SUM_FIELD);
                    break;
                case MIN:
                    query.addField(RESPONSE_CHUNK_MIN_FIELD);
                    break;
                case MAX:
                    query.addField(RESPONSE_CHUNK_MAX_FIELD);
                    break;
                case MODE_MEDIAN:
                case LTTB:
                case MIN_MAX:
                default:
                    throw new IllegalStateException("algorithm " + algo.name() + " is not yet supported !");
            }
        });
    }

    private Set<SamplingAlgorithm> determineSamplingAlgoThatWillBeUsed(SamplingConf askedSamplingConf, MetricsSizeInfo metricsSizeInfo) {
        if (askedSamplingConf.getAlgo() != SamplingAlgorithm.NONE) {
            Set<SamplingAlgorithm> singletonSet = new HashSet<SamplingAlgorithm>();
            singletonSet.add(askedSamplingConf.getAlgo());
            return singletonSet;
        }
        return metricsSizeInfo.getMetrics().stream()
                .map(metricName -> {
                    MetricSizeInfo metricInfo = metricsSizeInfo.getMetricInfo(metricName);
                    SamplingAlgorithm algo = TimeSeriesExtracterUtil.calculSamplingAlgorithm(askedSamplingConf, metricInfo.totalNumberOfPoints);
                    return algo;
                }).collect(Collectors.toSet());
    }

    //TODO from, to and SamplingConf as parameter. So calcul SampligConf before this method not in MultiTimeSeriesExtractorUsingPreAgg
    private MultiTimeSeriesExtracter createTimeSerieExtractorUsingChunks(JsonObject params, MetricsSizeInfo metricsInfo) {
        long from = params.getLong(FROM_REQUEST_FIELD);
        long to = params.getLong(TO_REQUEST_FIELD);
        SamplingConf requestedSamplingConf = getSamplingConf(params);
        MultiTimeSeriesExtractorUsingPreAgg timeSeriesExtracter = new MultiTimeSeriesExtractorUsingPreAgg(from, to, requestedSamplingConf);
        fillingExtractorWithMetricsSizeInfo(timeSeriesExtracter, metricsInfo);
        return timeSeriesExtracter;
    }

    //TODO from, to and SamplingConf as parameter. So calcul SampligConf before this method not in MultiTimeSeriesExtracterImpl
    private MultiTimeSeriesExtracter createTimeSerieExtractorSamplingAllPoints(JsonObject params, MetricsSizeInfo metricsInfo) {
        long from = params.getLong(FROM_REQUEST_FIELD);
        long to = params.getLong(TO_REQUEST_FIELD);
        SamplingConf requestedSamplingConf = getSamplingConf(params);
        MultiTimeSeriesExtracterImpl timeSeriesExtracter = new MultiTimeSeriesExtracterImpl(from, to, requestedSamplingConf);
        fillingExtractorWithMetricsSizeInfo(timeSeriesExtracter, metricsInfo);
        return timeSeriesExtracter;
    }

    private void fillingExtractorWithMetricsSizeInfo(MultiTimeSeriesExtracterImpl timeSeriesExtracter,
                                                     MetricsSizeInfo metricsInfo) {
        metricsInfo.getMetrics().forEach(metric -> {
            timeSeriesExtracter.setTotalNumberOfPointForMetric(metric, metricsInfo.getMetricInfo(metric).totalNumberOfPoints);
        });
    }

    private SamplingConf getSamplingConf(JsonObject params) {
        SamplingAlgorithm algo = SamplingAlgorithm.valueOf(params.getString(SAMPLING_ALGO_REQUEST_FIELD));
        int bucketSize = params.getInteger(BUCKET_SIZE_REQUEST_FIELD);
        int maxPoint = params.getInteger(MAX_POINT_BY_METRIC_REQUEST_FIELD);
        return new SamplingConf(algo, bucketSize, maxPoint);
    }

    private JsonObject extractTimeSeriesThenBuildResponse(JsonStream stream, MultiTimeSeriesExtracter timeSeriesExtracter) throws IOException {
        stream.open();
        JsonObject chunk = stream.read();
        while (!chunk.containsKey("EOF") || !chunk.getBoolean("EOF")) {
            timeSeriesExtracter.addChunk(chunk);
            chunk = stream.read();
        }
        timeSeriesExtracter.flush();
        LOGGER.debug("read {} chunks in stream", stream.getNumberOfDocRead());
        LOGGER.debug("extractTimeSeries response metric : {}", chunk.encodePrettily());
        return buildTimeSeriesResponse(timeSeriesExtracter);
    }

    private JsonObject extractTimeSeriesThenBuildResponse(List<JsonObject> chunks, MultiTimeSeriesExtracter timeSeriesExtracter) {
        chunks.forEach(timeSeriesExtracter::addChunk);
        timeSeriesExtracter.flush();
        return buildTimeSeriesResponse(timeSeriesExtracter);
    }

    private JsonObject buildTimeSeriesResponse(MultiTimeSeriesExtracter timeSeriesExtracter) {
        return new JsonObject()
                .put(TOTAL_POINTS_RESPONSE_FIELD, timeSeriesExtracter.pointCount())
                .put(TIMESERIES_RESPONSE_FIELD, timeSeriesExtracter.getTimeSeries());
    }

    private JsonStream queryStream(SolrQuery query) {
        StringBuilder exprBuilder = new StringBuilder("search(").append(solrHistorianConf.collection).append(",")
                .append("q=\"").append(query.getQuery()).append("\",");
        if (query.getFilterQueries() != null) {
            for (String filterQuery : query.getFilterQueries()) {
                exprBuilder
                        .append("fq=\"").append(filterQuery).append("\",");
            }
        }
        exprBuilder
                .append("fl=\"").append(query.getFields()).append("\",")
                .append("sort=\"").append(query.getSortField()).append("\",")
                .append("qt=\"/export\")");

        ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
        paramsLoc.set("expr", exprBuilder.toString());
        paramsLoc.set("qt", "/stream");
        LOGGER.debug("queryStream params : {}", paramsLoc);

        TupleStream solrStream = new SolrStream(solrHistorianConf.streamEndPoint, paramsLoc);
        StreamContext context = new StreamContext();
        solrStream.setStreamContext(context);
        return new JsonStreamSolrStreamImpl(solrStream);
    }


//    private MetricSizeInfo getNumberOfPointsInByRequest(SolrQuery query) throws IOException {//TODO better handling of exception
//        String cexpr = String.format("stats(%s,\n" +
//                "q=\"%s\",\n" +
//                "sum(chunk_size), count(*))",
//                collection, query.getQuery());
//        ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
//        paramsLoc.set("expr", cexpr);
//        paramsLoc.set("qt", "/stream");
//        TupleStream solrStream = new SolrStream(streamEndPoint, paramsLoc);
//        StreamContext context = new StreamContext();
//        solrStream.setStreamContext(context);
//        solrStream.open();
//        Tuple tuple = solrStream.read();
//        long numberOfChunk = -1;
//        long numberOfPoints = -1;
//        while (!tuple.EOF) {
//            LOGGER.trace("tuple : {}", tuple.jsonStr());
//            numberOfPoints = tuple.getLong("sum(chunk_size)");
//            numberOfChunk = tuple.getLong("count(*)");
//            tuple = solrStream.read();
//        }
//        LOGGER.debug("metric response : {}", tuple.jsonStr());
//        solrStream.close(); //TODO could be try-with-resources
//        MetricSizeInfo metrics = new MetricSizeInfo();
//        metrics.totalNumberOfChunks = numberOfChunk;
//        metrics.totalNumberOfPoints = numberOfPoints;
//        return metrics;
//    }

    private MetricsSizeInfo getNumberOfPointsByMetricInRequest(SolrQuery query) throws IOException {//TODO better handling of exception
//        String cexpr = "rollup(search(historian, q=\"*:*\", fl=\"chunk_size, name\", qt=\"/export\", sort=\"name asc\"),\n" +
//                "\t\t\t\t over=\"name\", sum(chunk_size))";
        StringBuilder exprBuilder = new StringBuilder("rollup(search(").append(solrHistorianConf.collection)
                .append(",q=\"").append(query.getQuery()).append("\"");
        if (query.getFilterQueries() != null) {
            for (String filterQuery : query.getFilterQueries()) {
                exprBuilder
                        .append(",fq=\"").append(filterQuery).append("\"");
            }
        }
        exprBuilder.append(",fl=\"").append(RESPONSE_CHUNK_SIZE_FIELD).append(", ")
                .append(RESPONSE_METRIC_NAME_FIELD).append("\"")
                .append(",qt=\"/export\", sort=\"").append(RESPONSE_METRIC_NAME_FIELD).append(" asc\")")
                .append(",over=\"name\", sum(chunk_size), count(*))");
        LOGGER.trace("expression is : {}", exprBuilder.toString());
        ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
        paramsLoc.set("expr", exprBuilder.toString());
        paramsLoc.set("qt", "/stream");
        TupleStream solrStream = new SolrStream(solrHistorianConf.streamEndPoint, paramsLoc);
        StreamContext context = new StreamContext();
        solrStream.setStreamContext(context);
        solrStream.open();
        Tuple tuple = solrStream.read();
        MetricsSizeInfoImpl metricsInfo = new MetricsSizeInfoImpl();
        while (!tuple.EOF) {
            LOGGER.trace("tuple : {}", tuple.jsonStr());
            MetricSizeInfo metric = new MetricSizeInfo();
            metric.metricName = tuple.getString("name");
            metric.totalNumberOfChunks = tuple.getLong("count(*)");
            metric.totalNumberOfPoints = tuple.getLong("sum(chunk_size)");
            metricsInfo.setMetricInfo(metric);
            tuple = solrStream.read();
        }
        LOGGER.debug("metric response : {}", tuple.jsonStr());
        solrStream.close(); //TODO could be try-with-resources
        return metricsInfo;
    }

    private JsonObject convertDoc(SolrDocument doc) {
        final JsonObject json = new JsonObject();
        doc.getFieldNames().forEach(f -> {
            json.put(f, doc.get(f));
        });
        return json;
    }
}
