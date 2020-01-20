package com.hurence.webapiservice.http.compaction;


import com.hurence.logisland.timeseries.sampling.SamplingAlgorithm;
import com.hurence.webapiservice.historian.reactivex.HistorianService;
import com.hurence.webapiservice.historian.util.HistorianResponseHelper;
import com.hurence.webapiservice.http.GetTimeSerieRequestParser;
import com.hurence.webapiservice.http.grafana.GrafanaApi;
import com.hurence.webapiservice.http.grafana.QueryRequestParser;
import com.hurence.webapiservice.http.grafana.modele.QueryRequestParam;
import com.hurence.webapiservice.modele.SamplingConf;
import com.hurence.webapiservice.timeseries.*;
import io.reactivex.Single;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.MultiMap;
import io.vertx.reactivex.ext.web.RoutingContext;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RTimerTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Principal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.hurence.webapiservice.historian.HistorianFields.*;
import static com.hurence.webapiservice.http.Codes.BAD_REQUEST;
import static com.hurence.webapiservice.http.Codes.NOT_FOUND;

public class CompactionApiImpl implements CompactionApi {

    private static final Logger LOGGER = LoggerFactory.getLogger(CompactionApiImpl.class);
    private HistorianService service;

    public final static String ALGO_TAG_KEY = "Algo";
    public final static String BUCKET_SIZE_TAG_KEY = "Bucket size";
    public final static String FILTER_TAG_KEY = "Tag";
    private static final GetTimeSerieRequestParser getTimeSerieParser = new GetTimeSerieRequestParser();

    private static TimeSeriesModeler timeserieModeler = new GrafanaTimeSeriesModeler();
    private static ChronixCompactionHandler compactionHandler = new ChronixCompactionHandler();

    public CompactionApiImpl(HistorianService service) {
        this.service = service;
    }

    @Override
    public void root(RoutingContext context) {
        context.response()
                .setStatusCode(200)
                .end("Historian compaction api is Working fine");
    }

    @Override
    public void search(RoutingContext context) {
        //TODO parse request body to filter query of metrics ?
        final JsonObject getMetricsParam = new JsonObject();
        service.rxGetMetricsName(getMetricsParam)
                .doOnError(ex -> {
                    LOGGER.error("Unexpected error : ", ex);
                    context.response().setStatusCode(500);
                    context.response().putHeader("Content-Type", "application/json");
                    context.response().end(ex.getMessage());
                })
                .doOnSuccess(metricResponse -> {
                    JsonArray metricNames = metricResponse.getJsonArray(RESPONSE_METRICS);
                    context.response().setStatusCode(200);
                    context.response().putHeader("Content-Type", "application/json");
                    context.response().end(metricNames.encode());
                }).subscribe();
    }

    @Override
    public void compact(RoutingContext context) {
        //TODO parse request body to filter query of metrics ?
        final JsonObject getMetricsParam = new JsonObject();

        //  Single<JsonObject> chunks = service.rxGetTimeSeriesChunk(getMetricsParam);

        service.rxGetTimeSeriesChunk(getMetricsParam)
                .doOnError(ex -> {
                    LOGGER.error("Unexpected error : ", ex);
                    context.response().setStatusCode(500);
                    context.response().putHeader("Content-Type", "application/json");
                    context.response().end(ex.getMessage());
                })
                .doOnSuccess(metricResponse -> {
                    JsonArray metricNames = metricResponse.getJsonArray(RESPONSE_METRICS);
                    context.response().setStatusCode(200);
                    context.response().putHeader("Content-Type", "application/json");
                    context.response().end(metricNames.encode());
                }).subscribe();
    }

    @Override
    public void query(RoutingContext context) {
        final TimeSeriesRequest request;
        try {
           /* MultiMap map = context.queryParams();
            request = getTimeSerieParser.parseRequest(map);*/

            JsonObject requestBody = context.getBodyAsJson();
            request = new QueryRequestParser().parseRequest(requestBody);
        } catch (Exception ex) {
            LOGGER.error("error parsing request", ex);
            context.response().setStatusCode(BAD_REQUEST);
            context.response().setStatusMessage(ex.getMessage());
            context.response().putHeader("Content-Type", "application/json");
            context.response().end();
            return;
        }

        final JsonObject getTimeSeriesChunkParams = buildHistorianRequest(request);


        service
                .rxCompactTimeSeriesChunk(getTimeSeriesChunkParams)
                .map(chunkResponse -> {
                    List<JsonObject> chunks = HistorianResponseHelper.extractChunks(chunkResponse);

                    Map<String, List<JsonObject>> chunksByName = chunks.stream().collect(
                            Collectors.groupingBy(chunk ->  chunk.getString(RESPONSE_METRIC_NAME_FIELD))
                    );
                    return TimeSeriesModeler.buildTimeSeries(request, chunksByName, timeserieModeler);
                })
                .doOnError(ex -> {
                    LOGGER.error("Unexpected error : ", ex);
                    context.response().setStatusCode(500);
                    context.response().putHeader("Content-Type", "application/json");
                    context.response().end(ex.getMessage());
                })
                .doOnSuccess(timeseries -> {
                  /*  JsonObject response = new JsonObject();
                    response
                            .put("query", getTimeSeriesChunkParams)
                            .put("total_timeseries", timeseries.getList().size())
                            .put("timeseries", timeseries);*/
                    JsonArray response = new JsonArray();
                    response.addAll(timeseries);
                            context.response().setStatusCode(200);
                    context.response().putHeader("Content-Type", "application/json");
                    context.response().end(response.encode());
                }).subscribe();
    }

    @Override
    public void annotations(RoutingContext context) {

    }

    @Override
    public void tagKeys(RoutingContext context) {

    }

    @Override
    public void tagValues(RoutingContext context) {

    }


    private JsonObject buildHistorianRequest(TimeSeriesRequest request) {
        JsonArray fieldsToFetch = new JsonArray()
                .add(RESPONSE_CHUNK_VALUE_FIELD)
                .add(RESPONSE_CHUNK_START_FIELD)
                .add(RESPONSE_CHUNK_END_FIELD)
                .add(RESPONSE_CHUNK_SIZE_FIELD)
                .add(RESPONSE_METRIC_NAME_FIELD);
        SamplingConf samplingConf = request.getSamplingConf();
        return new JsonObject()
                .put(FROM_REQUEST_FIELD, request.getFrom())
                .put(TO_REQUEST_FIELD, request.getTo())
                .put(FIELDS_TO_FETCH_AS_LIST_REQUEST_FIELD, fieldsToFetch)
                .put(METRIC_NAMES_AS_LIST_REQUEST_FIELD, request.getMetricNames())
                .put(TAGS_TO_FILTER_ON_REQUEST_FIELD, request.getTags())
                .put(SAMPLING_ALGO_REQUEST_FIELD, samplingConf.getAlgo())
                .put(BUCKET_SIZE_REQUEST_FIELD, samplingConf.getBucketSize())
                .put(MAX_POINT_BY_METRIC_REQUEST_FIELD, samplingConf.getMaxPoint());
    }


    private String parseTagValuesRequest(RoutingContext context) throws IllegalArgumentException {
        JsonObject body = context.getBodyAsJson();
        try {
            return body.getString("key");
        } catch (Exception ex) {
            throw new IllegalArgumentException(String.format("body request does not contain a key 'key'. " +
                            "Request is expected to be the following format : %s \n\n but was %s",
                    "{ \"key\":\"Algo\"}", body.encodePrettily()));
        }
    }

    private JsonArray getTagValuesOfAlgo() {
        return new JsonArray()
                .add(new JsonObject().put("text", SamplingAlgorithm.NONE))
                .add(new JsonObject().put("text", SamplingAlgorithm.AVERAGE))
                .add(new JsonObject().put("text", SamplingAlgorithm.FIRST_ITEM))
                .add(new JsonObject().put("text", SamplingAlgorithm.MIN))
                .add(new JsonObject().put("text", SamplingAlgorithm.MAX));
    }
}