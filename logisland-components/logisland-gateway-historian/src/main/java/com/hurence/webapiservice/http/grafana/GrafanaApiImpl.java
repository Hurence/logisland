package com.hurence.webapiservice.http.grafana;


import com.hurence.webapiservice.historian.reactivex.HistorianService;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrafanaApiImpl implements GrafanaApi {

    private static final Logger LOGGER = LoggerFactory.getLogger(GrafanaApiImpl.class);
    private HistorianService service;

    public GrafanaApiImpl(HistorianService service) {
        this.service = service;
    }

    @Override
    public void root(RoutingContext context) {
        context.response()
                .setStatusCode(200)
                .end();
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

        throw new UnsupportedOperationException("Not implented yet");//TODO
    }

    @Override
    public void annotations(RoutingContext context) {
        throw new UnsupportedOperationException("Not implented yet");//TODO
    }

    @Override
    public void tagKeys(RoutingContext context) {
        throw new UnsupportedOperationException("Not implented yet");//TODO
    }

    @Override
    public void tagValues(RoutingContext context) {
        throw new UnsupportedOperationException("Not implented yet");//TODO
    }
}
