package com.hurence.webapiservice.http;

import com.hurence.webapiservice.historian.reactivex.HistorianService;
import com.hurence.webapiservice.http.grafana.GrafanaApiImpl;
import com.hurence.webapiservice.timeseries.LogislandTimeSeriesModeler;
import com.hurence.webapiservice.timeseries.TimeSeriesModeler;
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
        final GetTimeSerieRequestParam request;
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
                    final long totalFound = chunkResponse.getLong(HistorianService.TOTAL_FOUND);
                    List<JsonObject> chunks = chunkResponse.getJsonArray(HistorianService.CHUNKS).stream()
                            .map(JsonObject.class::cast)
                            .collect(Collectors.toList());
                    if (totalFound != chunks.size())
                        //TODO add a test with more than 10 chunks then implement handling more than default 10 chunks of solr
                        //TODO should we add initial number of chunk to fetch in query param ?
                        throw new UnsupportedOperationException("not yet supported when matching more than "+
                                chunks.size() + " chunks (total found : " + totalFound +")");
                    Map<String, List<JsonObject>> chunksByName = chunks.stream().collect(
                            Collectors.groupingBy(chunk ->  chunk.getString(HistorianService.METRIC_NAME))
                    );
                    JsonArray timeseries = new JsonArray();
                    chunksByName.forEach((key, value) -> {
                        JsonObject agreggatedChunks = timeserieModeler.extractTimeSerieFromChunks(
                                request.getFrom(), request.getTo(),
                                request.getAggs(), request.getSamplingConf(), value);
                        timeseries.add(agreggatedChunks);
                    });
                    return timeseries;
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

    private JsonObject buildHistorianRequest(GetTimeSerieRequestParam request) {
        JsonArray fieldsToFetch = new JsonArray()
                .add(HistorianService.CHUNK_VALUE)
                .add(HistorianService.CHUNK_START)
                .add(HistorianService.CHUNK_END)
                .add(HistorianService.CHUNK_SIZE)
                .add(HistorianService.METRIC_NAME);
        request.getAggs().forEach(agg -> {
            final String aggField;
            switch (agg) {
                case MIN:
                    aggField = HistorianService.CHUNK_MIN;
                    break;
                case MAX:
                    aggField = HistorianService.CHUNK_MAX;
                    break;
                case AVG:
                    aggField = HistorianService.CHUNK_AVG;
                    break;
                case COUNT:
                    aggField = HistorianService.CHUNK_SIZE;
                    break;
                case SUM:
                    aggField = HistorianService.CHUNK_SUM;
                    break;
                default:
                    throw new IllegalStateException("Unsupported aggregation: " + agg);
            }
            fieldsToFetch.add(aggField);
        });
        return new JsonObject()
                .put(HistorianService.FROM, request.getFrom())
                .put(HistorianService.TO, request.getTo())
                .put(HistorianService.FIELDS_TO_FETCH, fieldsToFetch)
                .put(HistorianService.NAMES, request.getNames());
    }






}