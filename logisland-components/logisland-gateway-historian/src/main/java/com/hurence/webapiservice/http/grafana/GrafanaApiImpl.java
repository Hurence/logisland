package com.hurence.webapiservice.http.grafana;


import com.hurence.webapiservice.historian.reactivex.HistorianService;
import io.vertx.reactivex.ext.web.RoutingContext;

public class GrafanaApiImpl implements GrafanaApi {

    private HistorianService service;

    public GrafanaApiImpl(HistorianService service) {
        this.service = service;
    }

    @Override
    public void rootHandler(RoutingContext context) {
        context.response()
                .setStatusCode(200)
                .end();
    }

    @Override
    public void searchHandler(RoutingContext context) {
        //TODO at the moment we just return all different metrics

        throw new UnsupportedOperationException("Not implented yet");//TODO
    }

    @Override
    public void queryHandler(RoutingContext context) {
        throw new UnsupportedOperationException("Not implented yet");//TODO
    }

    @Override
    public void annotationsHandler(RoutingContext context) {
        throw new UnsupportedOperationException("Not implented yet");//TODO
    }

    @Override
    public void tagKeysHandler(RoutingContext context) {
        throw new UnsupportedOperationException("Not implented yet");//TODO
    }

    @Override
    public void tagValuesHandler(RoutingContext context) {
        throw new UnsupportedOperationException("Not implented yet");//TODO
    }
}
