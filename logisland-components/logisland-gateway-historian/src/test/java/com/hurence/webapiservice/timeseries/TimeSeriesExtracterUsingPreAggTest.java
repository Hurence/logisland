package com.hurence.webapiservice.timeseries;

import com.hurence.logisland.record.Point;
import com.hurence.logisland.timeseries.sampling.SamplingAlgorithm;
import com.hurence.util.modele.ChunkModele;
import com.hurence.webapiservice.modele.SamplingConf;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class TimeSeriesExtracterUsingPreAggTest {

    private static Logger LOGGER = LoggerFactory.getLogger(TimeSeriesExtracterUsingPreAggTest.class);


    JsonObject getChunk1() {
        ChunkModele chunk = ChunkModele.fromPoints("fake", Arrays.asList(
                new Point(0, 1477895624866L, 1),
                new Point(0, 1477916224866L, 1),
                new Point(0, 1477916224867L, 1),
                new Point(0, 1477916224868L, 1),
                new Point(0, 1477916224869L, 1),
                new Point(0, 1477916224870L, 1),
                new Point(0, 1477916224871L, 1),
                new Point(0, 1477916224872L, 1),
                new Point(0, 1477917224865L, 1)
        ));
        return chunk.toJson("id1");
    }

    JsonObject getChunk2() {
        ChunkModele chunk = ChunkModele.fromPoints("fake", Arrays.asList(
                new Point(0, 1477917224866L, 2),
                new Point(0, 1477917224867L, 2),
                new Point(0, 1477917224868L, 2)
        ));
        return chunk.toJson("id2");
    }


    JsonObject getChunk3() {
        ChunkModele chunk = ChunkModele.fromPoints("fake", Arrays.asList(
                new Point(0, 1477917224868L, 3),
                new Point(0, 1477917224869L, 3),
                new Point(0, 1477917224870L, 3)
        ));
        return chunk.toJson("id2");
    }

    JsonObject getChunk4() {
        ChunkModele chunk = ChunkModele.fromPoints("fake", Arrays.asList(
                new Point(0, 1477917224870L, 4),
                new Point(0, 1477917224871L, 4),
                new Point(0, 1477917224872L, 4)
        ));
        return chunk.toJson("id2");
    }

    JsonObject getChunk5() {
        ChunkModele chunk = ChunkModele.fromPoints("fake", Arrays.asList(
                new Point(0, 1477917224873L, 5),
                new Point(0, 1477917224874L, 5),
                new Point(0, 1477917224875L, 5)
        ));
        return chunk.toJson("id2");
    }

    @Test
    public void testNoSampler() {
        TimeSeriesExtracter extractor = new TimeSeriesExtracterUsingPreAgg("fake",
                1477895624866L , 1477917224866L,
                new SamplingConf(SamplingAlgorithm.NONE, 2, 3), 9);
        extractor.addChunk(getChunk1());
        extractor.flush();
        Assert.assertEquals(1, extractor.chunkCount());
        Assert.assertEquals(9, extractor.pointCount());
        JsonArray expectedPoints = new JsonArray();
        expectedPoints.add(new JsonArray(Arrays.asList(1.0, 1477895624866L)));
        Assert.assertEquals(new JsonObject()
                .put("target", "fake")
                .put("datapoints", expectedPoints), extractor.getTimeSeries());
    }

    @Test
    public void testAvgSampler() {
        TimeSeriesExtracter extractor = new TimeSeriesExtracterUsingPreAgg("fake",
                1477895624866L , 1477917224866L,
                new SamplingConf(SamplingAlgorithm.AVERAGE, 2, 3), 15);
        extractor.addChunk(getChunk1());
        extractor.addChunk(getChunk2());
        extractor.addChunk(getChunk3());
        extractor.flush();
        Assert.assertEquals(3, extractor.chunkCount());
        Assert.assertEquals(15, extractor.pointCount());
        JsonArray expectedPoints = new JsonArray();
        expectedPoints.add(new JsonArray(Arrays.asList(1.0, 1477895624866L)));
        expectedPoints.add(new JsonArray(Arrays.asList(2.5, 1477917224866L)));
        Assert.assertEquals(new JsonObject()
                .put("target", "fake")
                .put("datapoints", expectedPoints), extractor.getTimeSeries());
    }

    @Test
    public void testAvgSampler2() {
        TimeSeriesExtracter extractor = new TimeSeriesExtracterUsingPreAgg("fake",
                1477895624866L , 1477917224866L,
                new SamplingConf(SamplingAlgorithm.AVERAGE, 2, 3), 12);
        extractor.addChunk(getChunk2());
        extractor.addChunk(getChunk3());
        extractor.addChunk(getChunk4());
        extractor.addChunk(getChunk5());
        extractor.flush();
        Assert.assertEquals(4, extractor.chunkCount());
        Assert.assertEquals(12, extractor.pointCount());
        JsonArray expectedPoints = new JsonArray();
        expectedPoints.add(new JsonArray(Arrays.asList(2.5, 1477917224866L)));
        expectedPoints.add(new JsonArray(Arrays.asList(4.5, 1477917224870L)));
        Assert.assertEquals(new JsonObject()
                .put("target", "fake")
                .put("datapoints", expectedPoints), extractor.getTimeSeries());
    }

    @Test
    public void testMinSampler() {
        TimeSeriesExtracter extractor = new TimeSeriesExtracterUsingPreAgg("fake",
                1477895624866L , 1477917224866L,
                new SamplingConf(SamplingAlgorithm.MIN, 2, 3), 15);
        extractor.addChunk(getChunk1());
        extractor.addChunk(getChunk2());
        extractor.addChunk(getChunk3());
        extractor.flush();
        Assert.assertEquals(3, extractor.chunkCount());
        Assert.assertEquals(15, extractor.pointCount());
        JsonArray expectedPoints = new JsonArray();
        expectedPoints.add(new JsonArray(Arrays.asList(1.0, 1477895624866L)));
        expectedPoints.add(new JsonArray(Arrays.asList(2.0, 1477917224866L)));
        Assert.assertEquals(new JsonObject()
                        .put("target", "fake")
                        .put("datapoints", expectedPoints)
                , extractor.getTimeSeries());
    }

    @Test
    public void testMinSampler2() {
        TimeSeriesExtracter extractor = new TimeSeriesExtracterUsingPreAgg("fake",
                1477895624866L , 1477917224866L,
                new SamplingConf(SamplingAlgorithm.MIN, 2, 3), 12);
        extractor.addChunk(getChunk2());
        extractor.addChunk(getChunk3());
        extractor.addChunk(getChunk4());
        extractor.addChunk(getChunk5());
        extractor.flush();
        Assert.assertEquals(4, extractor.chunkCount());
        Assert.assertEquals(12, extractor.pointCount());
        JsonArray expectedPoints = new JsonArray();
        expectedPoints.add(new JsonArray(Arrays.asList(2, 1477917224866L)));
        expectedPoints.add(new JsonArray(Arrays.asList(4, 1477917224870L)));
        Assert.assertEquals(new JsonObject()
                        .put("target", "fake")
                        .put("datapoints", expectedPoints)
                , extractor.getTimeSeries());
    }

}
