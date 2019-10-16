package com.hurence.webapiservice.timeseries.impl;

import com.hurence.logisland.record.Point;
import com.hurence.logisland.timeseries.converter.compaction.BinaryCompactionConverter;
import com.hurence.webapiservice.timeseries.TimeseriesService;
import io.vertx.core.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.solr.common.SolrDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class TimeseriesServiceImpl implements TimeseriesService {

  private static Logger logger = LoggerFactory.getLogger(TimeseriesServiceImpl.class);

  public static String TIMESTAMP = "timestamp";
  public static String VALUE = "value";
  public static String CHUNK = "chunk";
  public static String START = "start";
  public static String END = "end";

  private final Vertx vertx;
  private final BinaryCompactionConverter compacter;

  public TimeseriesServiceImpl(Vertx vertx, Handler<AsyncResult<TimeseriesService>> readyHandler) {
    this.vertx = vertx;
    this.compacter = new BinaryCompactionConverter.Builder().build();
    readyHandler.handle(Future.succeededFuture(this));
  }

  @Override
  public TimeseriesService unCompressTimeSeries(JsonObject params, Handler<AsyncResult<JsonArray>> resultHandler) {
    byte[] chunk = params.getBinary(CHUNK);
    if (chunk == null) throw new IllegalArgumentException("field "+ CHUNK + " is mandatory");
    Long start = params.getLong(START);
    if (start == null) throw new IllegalArgumentException("field "+ START + " is mandatory");
    long end = params.getLong(END, -1L);

    Handler<Promise<JsonArray>> unCompressChunk = p -> {
      try {
        List<Point> points = compacter.unCompressPoints(chunk, start, end);
        p.complete(new JsonArray(points.stream()
                .map(point -> new JsonObject()
                        .put(TIMESTAMP, point.getTimestamp())
                        .put(VALUE, point.getValue()))
                .collect(Collectors.toList())));
      } catch (IOException e) {
        p.fail(e);
      }
    };
    vertx.executeBlocking(unCompressChunk, resultHandler);
    return this;
  }
}
