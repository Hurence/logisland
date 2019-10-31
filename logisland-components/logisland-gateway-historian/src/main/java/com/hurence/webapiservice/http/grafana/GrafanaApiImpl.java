package com.hurence.webapiservice.http.grafana;


import com.hurence.webapiservice.historian.reactivex.HistorianService;
import com.hurence.webapiservice.historian.util.HistorianResponseHelper;
import com.hurence.webapiservice.timeseries.GrafanaTimeSeriesModeler;
import com.hurence.webapiservice.timeseries.TimeSeriesRequest;
import com.hurence.webapiservice.timeseries.TimeSeriesModeler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.hurence.webapiservice.http.Codes.BAD_REQUEST;

public class GrafanaApiImpl implements GrafanaApi {

    private static final Logger LOGGER = LoggerFactory.getLogger(GrafanaApiImpl.class);
    private HistorianService service;
    private static final QueryRequestParser queryRequestParser = new QueryRequestParser();
    private TimeSeriesModeler timeserieToolBox = new GrafanaTimeSeriesModeler();

    public GrafanaApiImpl(HistorianService service) {
        this.service = service;
    }

    @Override
    public void root(RoutingContext context) {
        context.response()
                .setStatusCode(200)
                .end("Historian grafana api is Working fine");
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
                    JsonArray metricNames = metricResponse.getJsonArray(HistorianService.METRICS);
                    context.response().setStatusCode(200);
                    context.response().putHeader("Content-Type", "application/json");
                    context.response().end(metricNames.encode());
                }).subscribe();
    }

    @Override
    public void query(RoutingContext context) {
        final TimeSeriesRequest request;
        try {
            JsonObject requestBody = context.getBodyAsJson();
            request = queryRequestParser.parseRequest(requestBody);
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
                .rxGetTimeSeriesChunk(getTimeSeriesChunkParams)
                .map(chunkResponse -> {
                    List<JsonObject> chunks = HistorianResponseHelper.extractChunks(chunkResponse);
                    Map<String, List<JsonObject>> chunksByName = chunks.stream().collect(
                            Collectors.groupingBy(chunk ->  chunk.getString(HistorianService.METRIC_NAME))
                    );
                    return TimeSeriesModeler.buildTimeSeries(request, chunksByName, timeserieToolBox);
                })
                .doOnError(ex -> {
                    LOGGER.error("Unexpected error : ", ex);
                    context.response().setStatusCode(500);
                    context.response().putHeader("Content-Type", "application/json");
                    context.response().end(ex.getMessage());
                })
                .doOnSuccess(timeseries -> {
                    context.response().setStatusCode(200);
                    context.response().putHeader("Content-Type", "application/json");
                    context.response().end(timeseries.encode());
                }).subscribe();
    }



    private JsonObject buildHistorianRequest(TimeSeriesRequest request) {
        JsonArray fieldsToFetch = new JsonArray()
                .add(HistorianService.CHUNK_VALUE)
                .add(HistorianService.CHUNK_START)
                .add(HistorianService.CHUNK_END)
                .add(HistorianService.CHUNK_SIZE)
                .add(HistorianService.METRIC_NAME);
        List<String> metricsToRetrieve = request.getNames();
        return new JsonObject()
                .put(HistorianService.FROM, request.getFrom())
                .put(HistorianService.TO, request.getTo())
                .put(HistorianService.MAX_TOTAL_CHUNKS_TO_RETRIEVE, 1000)
                .put(HistorianService.FIELDS_TO_FETCH, fieldsToFetch)
                .put(HistorianService.NAMES, metricsToRetrieve);
    }

    @Override
    public void annotations(RoutingContext context) {
        throw new UnsupportedOperationException("Not implemented yet");//TODO
    }

    @Override
    public void tagKeys(RoutingContext context) {
        throw new UnsupportedOperationException("Not implemented yet");//TODO
    }

    @Override
    public void tagValues(RoutingContext context) {
        throw new UnsupportedOperationException("Not implemented yet");//TODO
    }
}
