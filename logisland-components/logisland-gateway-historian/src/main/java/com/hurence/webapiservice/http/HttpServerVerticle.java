package com.hurence.webapiservice.http;

import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.Point;
import com.hurence.logisland.timeseries.converter.compaction.BinaryCompactionConverter;
import com.hurence.logisland.timeseries.sampling.SamplingAlgorithm;
import com.hurence.webapiservice.historian.reactivex.HistorianService;
import com.hurence.webapiservice.modele.AGG;
import com.hurence.webapiservice.modele.SamplingAlgo;
import com.hurence.webapiservice.timeseries.reactivex.TimeseriesService;
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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    /*
       REST API TIMESERIES RESPONSE
    */
    private static String TIMESERIES = "timeseries";
    private static String TIMESERIES_NAME = "name";
    private static String TIMESERIES_TIMESTAMPS = "timestamps";
    private static String TIMESERIES_VALUES = "values";
    private static String TIMESERIES_AGGS = "aggs";

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpServerVerticle.class);

    private HistorianService historianService;
    private TimeseriesService timeseriesService;
    private BinaryCompactionConverter compacter = new BinaryCompactionConverter.Builder().build();

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
        final Set<String> names; //TODO add possibility to select some names
        final long from;//TODO parseFrom, parseTo, parseAggs into methods
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
            samplingAlgo = new SamplingAlgo(SamplingAlgorithm.AVERAGE, 10, 1000);//TODO
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
                    //TODO groupBy record_name ???
                    final long totalFound = chunkResponse.getLong(HistorianService.TOTAL_FOUND);
                    List<JsonObject> chunks = chunkResponse.getJsonArray(HistorianService.CHUNKS).stream()
                            .map(JsonObject.class::cast)
                            .collect(Collectors.toList());
                    if (totalFound != chunks.size())
                        throw new UnsupportedOperationException("not yet supported when matching more than 10 chunks (total found : " + totalFound +")");//TODO
                    Map<String, List<JsonObject>> chunksByName = chunks.stream().collect(
                            Collectors.groupingBy(chunk ->  chunk.getString(FieldDictionary.RECORD_NAME))
                    );
                    JsonArray timeseries = new JsonArray();
                    chunksByName.forEach((key, value) -> {
                        JsonObject agreggatedChunks = agreggateChunks(from, to, aggs, samplingAlgo, value);
                        timeseries.add(agreggatedChunks);
                    });
                    return timeseries;
                })
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

    private JsonObject agreggateChunks(long from, long to, List<AGG> aggs, SamplingAlgo samplingAlgo, List<JsonObject> chunks) {
        if (chunks==null || chunks.isEmpty()) throw new IllegalArgumentException("chunks is null or empty !");
        chunks = adjustChunk(from, to, aggs, chunks);
        JsonObject timeSeriesAggs = calculAggs(aggs, chunks);
        JsonObject points = samplePoints(from, to, samplingAlgo, chunks);
        String name = chunks.stream().findFirst().get().getString(FieldDictionary.RECORD_NAME);
        return new JsonObject().mergeIn(points)
                .put(TIMESERIES_NAME, name)
                .put(TIMESERIES_AGGS, timeSeriesAggs);
    }

    /**
     *
     * @param samplingAlgo how to sample points to retrieve
     * @param chunks to sample, chunks should be corresponding to the same timeserie !*
     *               Should contain the compressed binary points as well as all needed aggs.
     *               Chunks should be ordered as well.
     * @return sampled points as an array
     * <pre>
     * {
     *     {@value TIMESERIES_TIMESTAMPS} : [longs]
     *     {@value TIMESERIES_VALUES} : [doubles]
     * }
     * DOCS contains at minimum chunk_value, chunk_start
     * </pre>
     */
    private JsonObject samplePoints(long from, long to, SamplingAlgo samplingAlgo, List<JsonObject> chunks) {
        List<Point> points = chunks.stream()
                .flatMap(chunk -> {
                    byte[] binaryChunk = chunk.getBinary(FieldDictionary.CHUNK_VALUE);
                    try {
                        return compacter.unCompressPoints(binaryChunk, from, to).stream();
                    } catch (IOException ex) {
                        throw new IllegalArgumentException("error during uncompression of a chunk !", ex);
                    }
                })
                .collect(Collectors.toList());
        List<Long> timestamps = points.stream()
                .map(Point::getTimestamp)
                .collect(Collectors.toList());
        List<Double> values = points.stream()
                .map(Point::getValue)
                .collect(Collectors.toList());
        return new JsonObject()
                .put(TIMESERIES_TIMESTAMPS, timestamps)
                .put(TIMESERIES_VALUES, values);
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