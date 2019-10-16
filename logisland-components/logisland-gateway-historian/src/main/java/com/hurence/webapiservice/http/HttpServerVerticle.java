package com.hurence.webapiservice.http;

import com.hurence.webapiservice.historian.reactivex.HistorianService;
import io.vertx.core.Promise;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.http.HttpServer;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.RoutingContext;
import io.vertx.reactivex.ext.web.handler.BodyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HttpServerVerticle extends AbstractVerticle {

  public static final String CONFIG_HTTP_SERVER_PORT = "port";
  public static final String CONFIG_HTTP_SERVER_HOSTNAME = "host";
  public static final String CONFIG_HISTORIAN_ADDRESS = "historian.address";

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpServerVerticle.class);

  private HistorianService historianService;

  @Override
  public void start(Promise<Void> promise) throws Exception {

    String docServiceAdr = config().getString(CONFIG_HISTORIAN_ADDRESS, "historian");
    historianService = com.hurence.webapiservice.historian.HistorianService.createProxy(vertx.getDelegate(), docServiceAdr);

    HttpServer server = vertx.createHttpServer();

    Router router = Router.router(vertx);

//    router.route().handler(CookieHandler.create());
    router.route().handler(BodyHandler.create());
//    router.route().handler(SessionHandler.create(LocalSessionStore.create(vertx)));

    router.get("/timeseries").handler(this::getTimeSeries);
//    router.get("/doc/similarTo/:id").handler(this::getSimilarDoc);

    int portNumber = config().getInteger(CONFIG_HTTP_SERVER_PORT, 8080);
    String host = config().getString(CONFIG_HTTP_SERVER_HOSTNAME, "localhost");
    server
      .requestHandler(router)
      .listen(portNumber, host, ar -> {
        if (ar.succeeded()) {
          LOGGER.info("HTTP server running at {}:{}", host, portNumber);
          promise.complete();
        } else {
          LOGGER.error("Could not start a HTTP server at {}:{}", host, portNumber, ar.cause());
          promise.fail(ar.cause());
        }
      });
  }

  private void getTimeSeries(RoutingContext context)  {
    //TODO
  }
}