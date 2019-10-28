package com.hurence.webapiservice.http;

import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.Point;
import com.hurence.logisland.timeseries.converter.compaction.BinaryCompactionConverter;
import com.hurence.logisland.timeseries.sampling.Sampler;
import com.hurence.logisland.timeseries.sampling.SamplerFactory;
import com.hurence.webapiservice.historian.reactivex.HistorianService;
import com.hurence.webapiservice.modele.AGG;
import com.hurence.webapiservice.modele.SamplingConf;
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
    /*
       REST API TIMESERIES RESPONSE
    */
    private static String TIMESERIES = "timeseries";
    private static String TIMESERIES_NAME = "name";
    private static String TIMESERIES_TIMESTAMPS = "timestamps";
    private static String TIMESERIES_VALUES = "values";
    private static String TIMESERIES_AGGS = "aggs";
    private static String TIMESERIES_AGGS_MIN = "min";
    private static String TIMESERIES_AGGS_MAX = "max";
    private static String TIMESERIES_AGGS_AVG = "avg";
    private static String TIMESERIES_AGGS_COUNT = "count";
    private static String TIMESERIES_AGGS_SUM = "sum";


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
                            Collectors.groupingBy(chunk ->  chunk.getString(FieldDictionary.RECORD_NAME))
                    );
                    JsonArray timeseries = new JsonArray();
                    chunksByName.forEach((key, value) -> {
                        JsonObject agreggatedChunks = agreggateChunks(
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
                .add(FieldDictionary.CHUNK_VALUE)
                .add(FieldDictionary.CHUNK_START)
                .add(FieldDictionary.CHUNK_END)
                .add(FieldDictionary.CHUNK_SIZE)
                .add(FieldDictionary.RECORD_NAME);
        request.getAggs().forEach(agg -> {
            final String aggField;
            switch (agg) {
                case MIN:
                    aggField = FieldDictionary.CHUNK_MIN;
                    break;
                case MAX:
                    aggField = FieldDictionary.CHUNK_MAX;
                    break;
                case AVG:
                    aggField = FieldDictionary.CHUNK_AVG;
                    break;
                case COUNT:
                    aggField = FieldDictionary.CHUNK_SIZE;
                    break;
                case SUM:
                    aggField = FieldDictionary.CHUNK_SUM;
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


    private JsonObject agreggateChunks(long from, long to, List<AGG> aggs, SamplingConf samplingConf, List<JsonObject> chunks) {
        if (chunks==null || chunks.isEmpty()) throw new IllegalArgumentException("chunks is null or empty !");
        String name = chunks.stream().findFirst().get().getString(FieldDictionary.RECORD_NAME);
        JsonObject timeserie = new JsonObject()
                .put(TIMESERIES_NAME, name);
        chunks = adjustChunk(from, to, aggs, chunks);
        //TODO add possibility to not get points but only aggregation if wanted
        JsonObject points = samplePoints(from, to, samplingConf, chunks);
        timeserie.mergeIn(points);
        JsonObject timeSeriesAggs = calculAggs(aggs, chunks);
        if (!timeSeriesAggs.isEmpty())
            timeserie.put(TIMESERIES_AGGS, timeSeriesAggs);
        return timeserie;
    }

    /**
     *
     * @param samplingConf how to sample points to retrieve
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
    private JsonObject samplePoints(long from, long to, SamplingConf samplingConf, List<JsonObject> chunks) {
        Sampler<Point> sampler = SamplerFactory.getPointSampler(samplingConf.getAlgo(), samplingConf.getBucketSize());
        List<Point> sampledPoints = sampler.sample(getPoints(from, to, chunks));
        List<Long> timestamps = sampledPoints.stream()
                .map(Point::getTimestamp)
                .collect(Collectors.toList());
        List<Double> values = sampledPoints.stream()
                .map(Point::getValue)
                .collect(Collectors.toList());
        return new JsonObject()
                .put(TIMESERIES_TIMESTAMPS, timestamps)
                .put(TIMESERIES_VALUES, values);
    }

    /**
     *
     * @param from
     * @param to
     * @param chunks
     * @return return all points uncompressing chunks
     */
    private List<Point> getPoints(long from, long to, List<JsonObject> chunks) {
        return chunks.stream()
                .flatMap(chunk -> {
                    byte[] binaryChunk = chunk.getBinary(FieldDictionary.CHUNK_VALUE);
                    long chunkStart = chunk.getLong(FieldDictionary.CHUNK_START);
                    try {
                        return compacter.unCompressPoints(binaryChunk, chunkStart, to).stream();
                    } catch (IOException ex) {
                        throw new IllegalArgumentException("error during uncompression of a chunk !", ex);
                    }
                }).collect(Collectors.toList());
    }

    /**
     *
     * @param aggs to calculate
     * @param chunks to use for calculation, each chunk should already contain needed agg. The chunk should
     *               reference the same timeserie.
     * @return a JsonObject containing aggregation over all chunks
     */
    private JsonObject calculAggs(List<AGG> aggs, List<JsonObject> chunks) {
        final JsonObject aggsJson = new JsonObject();
        aggs.forEach(agg -> {
            //TODO find a way to not use a switch, indeed this would imply to modify this code every time we add an agg
            //TODO instead for example use an interface ? And a factory ?
            final String aggField;
            switch (agg) {
                case MIN:
                    //TODO
                    aggsJson.put(TIMESERIES_AGGS_MIN, "TODO");
                    break;
                case MAX:
                    aggsJson.put(TIMESERIES_AGGS_MAX, "TODO");
                    break;
                case AVG:
                    aggsJson.put(TIMESERIES_AGGS_AVG, "TODO");
                    break;
                case COUNT:
                    aggsJson.put(TIMESERIES_AGGS_COUNT, "TODO");
                    break;
                case SUM:
                    aggsJson.put(TIMESERIES_AGGS_SUM, "TODO");
                    break;
                default:
                    throw new IllegalStateException("Unsupported aggregation: " + agg);
            }
        });
        return aggsJson;
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
        //TODO only necessary if we want to optimize aggs. But if we have to recompute all chunks this may not be really helpfull.
        //TODO depending on the size of the bucket desired.
        return chunks;
    }

}