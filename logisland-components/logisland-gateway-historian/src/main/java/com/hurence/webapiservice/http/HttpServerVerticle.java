package com.hurence.webapiservice.http;

import com.hurence.webapiservice.historian.reactivex.HistorianService;
import com.hurence.webapiservice.historian.util.HistorianResponseHelper;
import com.hurence.webapiservice.http.compaction.CompactionApiImpl;
import com.hurence.webapiservice.http.grafana.GrafanaApiImpl;
import com.hurence.webapiservice.timeseries.LogislandTimeSeriesModeler;
import com.hurence.webapiservice.timeseries.TimeSeriesModeler;
import com.hurence.webapiservice.timeseries.TimeSeriesRequest;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.MultiMap;
import io.vertx.reactivex.core.http.HttpServer;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.RoutingContext;
import io.vertx.reactivex.ext.web.handler.BodyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.hurence.webapiservice.historian.HistorianFields.*;
import static com.hurence.webapiservice.http.Codes.BAD_REQUEST;

public class HttpServerVerticle extends AbstractVerticle {

    /*
      CONFS
     */
    public static final String CONFIG_HTTP_SERVER_PORT = "port";
    public static final String CONFIG_HTTP_SERVER_HOSTNAME = "host";
    public static final String CONFIG_HISTORIAN_ADDRESS = "historian.address";
    public static final String CONFIG_TIMESERIES_ADDRESS = "timeseries.address";

    private static final GetTimeSerieRequestParser getTimeSerieParser = new GetTimeSerieRequestParser();
    private static TimeSeriesModeler timeserieModeler = new LogislandTimeSeriesModeler();


    private static final Logger LOGGER = LoggerFactory.getLogger(HttpServerVerticle.class);

    private HistorianService historianService;


    @Override
    public void start(Promise<Void> promise) throws Exception {
        LOGGER.debug("deploying {} verticle with config : {}", HttpServerVerticle.class.getSimpleName(), config().encodePrettily());
        String historianServiceAdr = config().getString(CONFIG_HISTORIAN_ADDRESS, "historian");
        historianService = com.hurence.webapiservice.historian.HistorianService.createProxy(vertx.getDelegate(), historianServiceAdr);

        HttpServer server = vertx.createHttpServer();

        Router router = Router.router(vertx);

//    router.route().handler(CookieHandler.create());
        router.route().handler(BodyHandler.create());
//    router.route().handler(SessionHandler.create(LocalSessionStore.create(vertx)));

        router.get("/timeseries").handler(this::getTimeSeries);
        Router graphanaApi = new GrafanaApiImpl(historianService).getGraphanaRouter(vertx);
        router.mountSubRouter("/api/grafana", graphanaApi);

        Router compactionApi = new CompactionApiImpl(historianService).getCompactionRouter(vertx);
        router.mountSubRouter("/api/compaction", compactionApi);
//    router.get("/doc/similarTo/:id").handler(this::getSimilarDoc);

        int portNumber = config().getInteger(CONFIG_HTTP_SERVER_PORT, 8080);
        String host = config().getString(CONFIG_HTTP_SERVER_HOSTNAME, "localhost");
        server
                .requestHandler(router)
                .listen(portNumber, host, ar -> {
                    if (ar.succeeded()) {
                        LOGGER.info("HTTP server running at {}:{} (verticle {})", host, portNumber, HttpServerVerticle.class.getSimpleName());
                        promise.complete();
                    } else {
                        String errorMsg = String.format("Could not start a HTTP server at %s:%s (verticle %s)",
                                host, portNumber, HttpServerVerticle.class.getSimpleName());
                        LOGGER.error(errorMsg, ar.cause());
                        promise.fail(ar.cause());
                    }
                });
    }

    /**
     * only from param is required
     *
     * @param context
     */
    private void getTimeSeries(RoutingContext context) {
        final TimeSeriesRequest request;
        try {
            MultiMap map = context.queryParams();
            request = getTimeSerieParser.parseRequest(map);
        } catch (Exception ex) {
            LOGGER.error("error parsing request", ex);
            context.response().setStatusCode(BAD_REQUEST);
            context.response().setStatusMessage(ex.getMessage());
            context.response().putHeader("Content-Type", "application/json");
            context.response().end();
            return;
        }

        final JsonObject getTimeSeriesChunkParams = buildHistorianRequest(request);

        historianService
                .rxGetTimeSeriesChunk(getTimeSeriesChunkParams)
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
                    JsonObject response = new JsonObject();
                    response
                            .put("query", "TODO")
                            .put("total_timeseries", "TODO")
                            .put("timeseries", timeseries);
                    context.response().setStatusCode(200);
                    context.response().putHeader("Content-Type", "application/json");
                    context.response().end(response.encode());
                }).subscribe();
    }

    private JsonObject buildHistorianRequest(TimeSeriesRequest request) {
        JsonArray fieldsToFetch = new JsonArray()
                .add(RESPONSE_CHUNK_VALUE_FIELD)
                .add(RESPONSE_CHUNK_START_FIELD)
                .add(RESPONSE_CHUNK_END_FIELD)
                .add(RESPONSE_CHUNK_SIZE_FIELD)
                .add(RESPONSE_METRIC_NAME_FIELD);
        request.getAggs().forEach(agg -> {
            final String aggField;
            switch (agg) {
                case MIN:
                    aggField = RESPONSE_CHUNK_MIN_FIELD;
                    break;
                case MAX:
                    aggField = RESPONSE_CHUNK_MAX_FIELD;
                    break;
                case AVG:
                    aggField = RESPONSE_CHUNK_AVG_FIELD;
                    break;
                case COUNT:
                    aggField = RESPONSE_CHUNK_SIZE_FIELD;
                    break;
                case SUM:
                    aggField = RESPONSE_CHUNK_SUM_FIELD;
                    break;
                default:
                    throw new IllegalStateException("Unsupported aggregation: " + agg);
            }
            fieldsToFetch.add(aggField);
        });
        return new JsonObject()
                .put(FROM_REQUEST_FIELD, request.getFrom())
                .put(TO_REQUEST_FIELD, request.getTo())
                .put(FIELDS_TO_FETCH_AS_LIST_REQUEST_FIELD, fieldsToFetch)
                .put(METRIC_NAMES_AS_LIST_REQUEST_FIELD, request.getMetricNames())
                .put(TAGS_TO_FILTER_ON_REQUEST_FIELD, request.getTags());
    }






}