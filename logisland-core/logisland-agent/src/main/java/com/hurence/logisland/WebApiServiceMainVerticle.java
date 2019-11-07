package com.hurence.logisland;


import io.vertx.core.DeploymentOptions;
import io.vertx.core.Promise;
import io.vertx.rxjava.core.AbstractVerticle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Single;

public class WebApiServiceMainVerticle extends AbstractVerticle {

    private static Logger LOGGER = LoggerFactory.getLogger(WebApiServiceMainVerticle.class);

    private static final String CONFIG_HTTP_SERVER_ROOT = "server";
    private static final String CONFIG_HISTORIAN_ROOT = "historian";
    private static final String CONFIG_INSTANCE_NUMBER_WEB = "web.verticles.instance.number";
    private static final String CONFIG_INSTANCE_NUMBER_HISTORIAN = " historian.verticles.instance.number";

    @Override
    public void start(Promise<Void> promise) throws Exception {
        vertx.getOrCreateContext();
        LOGGER.debug("deploying {} verticle with config : {}", WebApiServiceMainVerticle.class.getSimpleName(), config().encodePrettily());

        vertx
                .createHttpServer()
                .requestHandler(r -> {
                    r.response().end("<h1>Hello from my first " +
                            "Vert.x 3 application</h1>");
                })
                .listen(8080, result -> {
                    if (result.succeeded()) {
                        promise.complete();
                    } else {
                        promise.fail(result.cause());
                    }
                });
    }


}