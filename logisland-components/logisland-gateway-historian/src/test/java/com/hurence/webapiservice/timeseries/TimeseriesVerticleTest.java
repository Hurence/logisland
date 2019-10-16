package com.hurence.webapiservice.timeseries;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static com.hurence.webapiservice.historian.impl.SolrHistorianServiceImpl.CHUNK;
import static com.hurence.webapiservice.historian.impl.SolrHistorianServiceImpl.START;
import static com.hurence.webapiservice.timeseries.TimeseriesService.DEFAULT_ADRESS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith({VertxExtension.class})
public class TimeseriesVerticleTest {

    private static Logger logger = LoggerFactory.getLogger(TimeseriesVerticleTest.class);

    private com.hurence.webapiservice.timeseries.reactivex.TimeseriesService timeseries;

    public TimeseriesVerticleTest() { }

    @BeforeEach
    public void prepare(Vertx vertx, VertxTestContext context) throws InterruptedException {
        JsonObject conf = new JsonObject();
        vertx.deployVerticle(new TimeseriesVerticle(), new DeploymentOptions().setConfig(conf),
                context.succeeding(id -> {
                    timeseries = TimeseriesService.createProxy(vertx, DEFAULT_ADRESS);
                    context.completeNow();
                }));
    }

    @AfterEach
    public void finish(Vertx vertx) {
        assertThat(vertx.deploymentIDs())
                .isNotEmpty()
                .hasSize(1);
    }

    @Test
    @Timeout(value = 5000, timeUnit = TimeUnit.SECONDS)
    void unCompressTimeSeriesTest(VertxTestContext testContext) throws Throwable {
        JsonObject params = new JsonObject()
                .put(CHUNK, "H4sIAAAAAAAAAOPi1Hx2tvmJTmaQAxenpu7kadEvXro5cPEozGDWzLH9tl4z3sKBi1uAQXOD5Vd3ibchII6l5oyCA4cmf/YCcdg1L+2ctufX8gAQh0WzrsTi+5pKG6AJAheZNEvdpz/9Je8JkmLUdL68w/jtPX+Q1FY2zW+9XPt+73UH8Q4xan6Rm6K7tiAExLvAqCkb8mvGuxAviDaJbPtJUy66gqTmMWt+5TloKpweBHHT4rlWD31+hoOkTjFqlh+UzHObbg7inWbUtF915PryeieQQnnNgEjm+Y/2+YI4vJod66NqDJUcQBxWzcyfIRosC8Gmn2bXDLA60rhsGsj/AnMZNZOenLoWVWwH4v0COmpR6uvoZV4g3klGzUsbLmvuk/WFuOPnFz7RwzW2UJs3zYr6lb8/GOL6LpG7qo4uTlBd3MGZgjpVYDPOM2pWy00NOZgbAOJ9Ydfk1WWpn9wP1SZQbSO557gTKE4cHhv2zHriAooToAmmt5bt+pgRDNIznVkzxpI5fWZPKIg3jVkzxXO2flurB4h3j0Xz2lIxg58L/SHmKXEKLeM/HAwNqQ8K7ve8zltB3St++9jmK5lgR11j1FwopbfzPoc/1MHnjEvF9+w0hqqsYvn7lT0c6mk+hmWxnopgDpPm04h/D/5PBptxllGzguOoRGQv2Fkr2TSfmt28nXgGGpvVkyfz+NWAo/0ki6ZS7MJr9f0+ICkJTT2dryG5jc5gXYyaanldR14bBzsIMAAAIgPhmaMCAAA=")
                .put(START, 1570634183825L);

        timeseries.unCompressTimeSeries(params, testContext.succeeding(timeseries -> {
            testContext.verify(() -> {
                assertEquals(50, timeseries.size());
                JsonObject point1 = timeseries.getJsonObject(0);
                assertEquals(new JsonObject()
                        .put("timestamp", 1570634183825L)
                        .put("value", 73.64336502906335d), point1);
                JsonObject point25 = timeseries.getJsonObject(24);
                assertEquals(new JsonObject()
                        .put("timestamp", 1570634188310L)
                        .put("value", 29.487351719047158d), point25);
                JsonObject point50 = timeseries.getJsonObject(49);
                assertEquals(new JsonObject()
                        .put("timestamp", 1570634194375L)
                        .put("value", 76.81126512068349d), point50);
                testContext.completeNow();
            });
        }));
    }
}

