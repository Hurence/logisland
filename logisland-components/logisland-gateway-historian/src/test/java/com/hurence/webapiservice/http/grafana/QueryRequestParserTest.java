package com.hurence.webapiservice.http.grafana;

import com.hurence.webapiservice.timeseries.TimeSeriesRequest;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.IntStream;

import static com.hurence.webapiservice.historian.HistorianFields.RESPONSE_CHUNK_FIRST_VALUE_FIELD;
import static com.hurence.webapiservice.http.grafana.modele.QueryRequestParam.DEFAULT_BUCKET_SIZE;
import static com.hurence.webapiservice.http.grafana.modele.QueryRequestParam.DEFAULT_SAMPLING_ALGORITHM;
import static com.hurence.webapiservice.timeseries.BucketUtils.calculBucketSize;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class QueryRequestParserTest {

    private static Logger LOGGER = LoggerFactory.getLogger(QueryRequestParserTest.class);
    @Test
    public void testparsingRequest() {
        JsonObject requestBody = new JsonObject("{\"requestId\":\"Q108\",\"timezone\":\"\",\"panelId\":2,\"dashboardId\":2,\"range\":{\"from\":\"2019-11-14T02:56:53.285Z\",\"to\":\"2019-11-14T08:56:53.285Z\",\"raw\":{\"from\":\"now-6h\",\"to\":\"now\"}},\"interval\":\"30s\",\"intervalMs\":30000,\"targets\":[{\"target\":\"speed\",\"refId\":\"A\",\"type\":\"timeserie\"},{\"target\":\"pressure\",\"refId\":\"B\",\"type\":\"timeserie\"},{\"target\":\"rotation\",\"refId\":\"C\",\"type\":\"timeserie\"}],\"maxDataPoints\":844,\"scopedVars\":{\"__interval\":{\"text\":\"30s\",\"value\":\"30s\"},\"__interval_ms\":{\"text\":\"30000\",\"value\":30000}},\"startTime\":1573721813291,\"rangeRaw\":{\"from\":\"now-6h\",\"to\":\"now\"},\"adhocFilters\":[]}");
        final QueryRequestParser queryRequestParser = new QueryRequestParser();
        final TimeSeriesRequest request = queryRequestParser.parseRequest(requestBody);
        LOGGER.info("request : {}", request);
        assertEquals(1573700213285L, request.getFrom());
        assertEquals(1573721813285L, request.getTo());
        assertEquals(844, request.getSamplingConf().getMaxPoint());
        assertEquals(DEFAULT_BUCKET_SIZE, request.getSamplingConf().getBucketSize());
        assertEquals(DEFAULT_SAMPLING_ALGORITHM, request.getSamplingConf().getAlgo());
        assertEquals(Collections.emptyList(), request.getAggs());
        assertEquals(Arrays.asList("speed", "pressure", "rotation"), request.getMetricNames());
        assertEquals(Collections.emptyList(), request.getTags());
    }

    /**
     * a static SimpleDateFormat was causing trouble, so this test check this problem.
     */
    @Test
    public void testParsingRequestSupportMultiThreaded() {
        JsonObject requestBody = new JsonObject("{\"requestId\":\"Q108\",\"timezone\":\"\",\"panelId\":2,\"dashboardId\":2,\"range\":{\"from\":\"2019-11-14T02:56:53.285Z\",\"to\":\"2019-11-14T08:56:53.285Z\",\"raw\":{\"from\":\"now-6h\",\"to\":\"now\"}},\"interval\":\"30s\",\"intervalMs\":30000,\"targets\":[{\"target\":\"speed\",\"refId\":\"A\",\"type\":\"timeserie\"},{\"target\":\"pressure\",\"refId\":\"B\",\"type\":\"timeserie\"},{\"target\":\"rotation\",\"refId\":\"C\",\"type\":\"timeserie\"}],\"maxDataPoints\":844,\"scopedVars\":{\"__interval\":{\"text\":\"30s\",\"value\":\"30s\"},\"__interval_ms\":{\"text\":\"30000\",\"value\":30000}},\"startTime\":1573721813291,\"rangeRaw\":{\"from\":\"now-6h\",\"to\":\"now\"},\"adhocFilters\":[]}");
        IntStream
                .range(0, 10)
                .parallel()
                .forEach(i -> {
                    final QueryRequestParser queryRequestParser = new QueryRequestParser();
                    queryRequestParser.parseRequest(requestBody);
        });
    }

}
