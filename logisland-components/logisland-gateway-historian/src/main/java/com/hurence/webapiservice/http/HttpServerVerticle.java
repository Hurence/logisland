package com.hurence.webapiservice.http;

import com.hurence.logisland.record.FieldDictionary;
import com.hurence.webapiservice.historian.reactivex.HistorianService;
import com.hurence.webapiservice.modele.AGG;
import com.hurence.webapiservice.modele.SAMPLING;
import com.hurence.webapiservice.modele.SamplingAlgo;
import com.hurence.webapiservice.timeseries.reactivex.TimeseriesService;
import io.reactivex.Single;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;


public class HttpServerVerticle extends AbstractVerticle {

    /*
      CONFS
     */
    public static final String CONFIG_HTTP_SERVER_PORT = "port";
    public static final String CONFIG_HTTP_SERVER_HOSTNAME = "host";
    public static final String CONFIG_HISTORIAN_ADDRESS = "historian.address";
    public static final String CONFIG_TIMESERIES_ADDRESS = "timeseries.address";
    /*
      REST API PARAMS
     */
    public static final String QUERY_PARAM_FROM = "from";
    public static final String QUERY_PARAM_TO = "to";
    public static final String QUERY_PARAM_NAME = "name";
    public static final String QUERY_PARAM_AGGS = "aggs";
    public static final String QUERY_PARAM_SAMPLING = "sampling";

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpServerVerticle.class);

    private HistorianService historianService;
    private TimeseriesService timeseriesService;

    @Override
    public void start(Promise<Void> promise) throws Exception {

        String historianServiceAdr = config().getString(CONFIG_HISTORIAN_ADDRESS, "historian");
        historianService = com.hurence.webapiservice.historian.HistorianService.createProxy(vertx.getDelegate(), historianServiceAdr);

        String timeseriesServiceAdr = config().getString(CONFIG_TIMESERIES_ADDRESS, "timeseries");
        timeseriesService = com.hurence.webapiservice.timeseries.TimeseriesService.createProxy(vertx.getDelegate(), timeseriesServiceAdr);

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

    /**
     * only from param is required
     *
     * @param context
     */
    private void getTimeSeries(RoutingContext context) {
        MultiMap map = context.queryParams();
        final long from;
        final long to;
        final List<AGG> aggs;
        final SamplingAlgo samplingAlgo;
        try {
            from = Long.parseLong(map.get(QUERY_PARAM_FROM));
            if (map.contains(QUERY_PARAM_TO)) {
                to = Long.parseLong(map.get(QUERY_PARAM_TO));
            } else {
                to = Long.MAX_VALUE;
            }
            if (map.contains(QUERY_PARAM_AGGS)) {
                aggs = map.getAll(QUERY_PARAM_AGGS).stream()
                        .map(AGG::valueOf)//TODO more robust ?
                        .collect(Collectors.toList());
            } else {
                aggs = Collections.emptyList();
            }
            samplingAlgo = new SamplingAlgo(SAMPLING.MAX, 10, 1000);//TODO
        } catch (Exception ex) {
            context.response().setStatusCode(500);//TODO correct code
            context.response().putHeader("Content-Type", "application/json");
            context.response().end(ex.getMessage());
            return;
        }


        //TODO implement customisable args, for the moment only MAX and always calculated
        final JsonObject getTimeSeriesChunkParams = new JsonObject()
                .put(HistorianService.FROM, from)
                .put(HistorianService.TO, map.get(QUERY_PARAM_TO))
                .put(HistorianService.FIELDS_TO_FETCH, new JsonArray()
                        .add(FieldDictionary.CHUNK_VALUE)
                        .add(FieldDictionary.CHUNK_START)
                        .add(FieldDictionary.CHUNK_END)
                        .add(FieldDictionary.CHUNK_SIZE)
                        .add(FieldDictionary.RECORD_NAME)
                        .add(FieldDictionary.CHUNK_MAX)
                );
//            .put(HistorianService.RECORD_NAME, map.get(QUERY_PARAM_NAME));TODO


        historianService
                .rxGetTimeSeriesChunk(getTimeSeriesChunkParams)
                .map(chunkResponse -> {
                    final long totalFound = chunkResponse.getLong(HistorianService.TOTAL_FOUND);
                    List<JsonObject> chunks = chunkResponse.getJsonArray(HistorianService.CHUNKS).stream()
                            .map(JsonObject.class::cast)
                            .collect(Collectors.toList());
                    if (totalFound != chunks.size())
                        throw new UnsupportedOperationException("not yet supported when matching more than 10 chunks");//TODO
                    chunks = adjustChunk(from, to, aggs, chunks);
                    JsonObject timeSeriesAggs = calculAggs(aggs, chunks);
                    JsonArray points = samplePoints(samplingAlgo, chunks);

                    return new JsonObject()
                            .put("name", "TODO")
                            .put("points", points)
                            .put("chunk_aggs", timeSeriesAggs);
//                        .map(chunk -> {
//                            final JsonObject unCompressTimeSeriesParam = new JsonObject()
//                                    .put(TimeseriesService.CHUNK, chunk.getValue(FieldDictionary.CHUNK_VALUE))
//                                    .put(TimeseriesService.START, chunk.getValue(FieldDictionary.CHUNK_START));
//                            toOpt.ifPresent(to -> unCompressTimeSeriesParam
//                                    .put(TimeseriesService.END, chunk.getValue(FieldDictionary.CHUNK_END))
//                            );
//                            JsonObject chunkAggs = new JsonObject()
//                                    .put(AGG.MAX.toString(), chunk.getString(FieldDictionary.CHUNK_MAX));
//                            return timeseriesService.rxUnCompressTimeSeries(unCompressTimeSeriesParam)
//                                    .map(points -> new JsonObject()
//                                            .put("name", chunk.getString(FieldDictionary.RECORD_NAME))
//                                            .put("points", points)
//                                            .put("chunk_aggs", chunkAggs));
//                        });


//              return null;
                    //TODO must return a single
//              name:
//                type: string
//              points:
                    //              type: array
                    //              items:
                    //              $ref: "#/components/schemas/Point"
//              aggs:
//                $ref: "#/components/schemas/Aggs"
                })
                //TODO chain until end
                .doOnError(ex -> {
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

    /**
     *
     * @param samplingAlgo how to sample points to retrieve
     * @param chunks to sample. Should contain the compressed binary points as well as all needed aggs
     * @return sampled points as an array
     */
    private JsonArray samplePoints(SamplingAlgo samplingAlgo, List<JsonObject> chunks) {
        //TODO
        return new JsonArray()
                .add(new JsonObject().put("timestamp", 10L).put("value", 2d))
                .add(new JsonObject().put("timestamp", 12L).put("value", 3d));
    }

    /**
     *
     * @param aggs to calculate
     * @param chunks to use for calculation, each chunk should already contain needed agg. The chunk should
     *               reference the same timeserie.
     * @return a JsonObject containing aggregation over all chunks
     */
    private JsonObject calculAggs(List<AGG> aggs, List<JsonObject> chunks) {
        //TODO
        return new JsonObject()
                .put("max", 50);
    }

    /**
     *
     * @param from minimum timestamp for points in chunks.
     *             If a chunk contains a point with a lower timestamp it will be recalculated without those points
     * @param to maximum timestamp for points in chunks.
     *           If a chunk contains a point with a higher timestamp it will be recalculated without those points
     * @param aggs the desired aggs for the chunk, it will be recalculated if needed
     * @param chunks the current chunks
     * @return chunks if no points are out of bound otherwise return chunks with recalculated ones.
     */
    private List<JsonObject> adjustChunk(long from, long to, List<AGG> aggs, List<JsonObject> chunks) {
        //TODO
        return chunks;
    }

}