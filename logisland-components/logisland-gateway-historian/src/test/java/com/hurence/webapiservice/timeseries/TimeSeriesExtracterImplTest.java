package com.hurence.webapiservice.timeseries;

import com.hurence.logisland.record.Point;
import com.hurence.logisland.timeseries.sampling.SamplingAlgorithm;
import com.hurence.util.modele.ChunkModele;
import com.hurence.webapiservice.modele.SamplingConf;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.json.simple.JSONArray;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class TimeSeriesExtracterImplTest {

    private static Logger LOGGER = LoggerFactory.getLogger(TimeSeriesExtracterImplTest.class);


    JsonObject getChunk1() {
        ChunkModele chunk = ChunkModele.fromPoints("fake", Arrays.asList(
                new Point(0, 1477895624866L, 1),
                new Point(0, 1477916224866L, 2),
                new Point(0, 1477917224866L, 3)
        ));
        return chunk.toJson("id1");
    }


    JsonObject getChunk2() {
        ChunkModele chunk = ChunkModele.fromPoints("fake", Arrays.asList(
                new Point(0, 1477916224866L, 4),
                new Point(0, 1477916224867L, 5),
                new Point(0, 1477916224868L, 6)
        ));
        return chunk.toJson("id1");
    }

    @Test
    public void testNoSampler() {
        TimeSeriesExtracter extractor = new TimeSeriesExtracterImpl("fake",
                1477895624866L , 1477917224866L,
                new SamplingConf(SamplingAlgorithm.NONE, 2, 3), 3);
        extractor.addChunk(getChunk1());
        extractor.flush();
        Assert.assertEquals(1, extractor.chunkCount());
        Assert.assertEquals(3, extractor.pointCount());
        JsonArray expectedPoints = new JsonArray();
        expectedPoints.add(new JsonArray(Arrays.asList(1, 1477895624866L)));
        expectedPoints.add(new JsonArray(Arrays.asList(2, 1477916224866L)));
        expectedPoints.add(new JsonArray(Arrays.asList(3, 1477917224866L)));
        Assert.assertEquals(new JsonObject()
                        .put("target", "fake")
                        .put("datapoints", expectedPoints)
                , extractor.getTimeSeries());
    }

    @Test
    public void testAvgSampler() {
        TimeSeriesExtracter extractor = new TimeSeriesExtracterImpl("fake",
                1477895624866L , 1477917224866L,
                new SamplingConf(SamplingAlgorithm.AVERAGE, 2, 3), 3);
        extractor.addChunk(getChunk1());
        extractor.flush();
        Assert.assertEquals(1, extractor.chunkCount());
        Assert.assertEquals(3, extractor.pointCount());
        JsonArray expectedPoints = new JsonArray();
        expectedPoints.add(new JsonArray(Arrays.asList(1.5, 1477895624866L)));
        expectedPoints.add(new JsonArray(Arrays.asList(3, 1477917224866L)));
        Assert.assertEquals(new JsonObject()
                .put("target", "fake")
                .put("datapoints", expectedPoints)
                , extractor.getTimeSeries());
    }

    @Test
    public void testNoSamplerWithIntersectingChunks() {
        TimeSeriesExtracter extractor = new TimeSeriesExtracterImpl("fake",
                1477895624866L , 1477917224866L,
                new SamplingConf(SamplingAlgorithm.NONE, 2, 6), 6);
        extractor.addChunk(getChunk1());
        extractor.addChunk(getChunk2());
        extractor.flush();
        Assert.assertEquals(2, extractor.chunkCount());
        Assert.assertEquals(6, extractor.pointCount());
        JsonArray expectedPoints = new JsonArray();
        expectedPoints.add(new JsonArray(Arrays.asList(1, 1477895624866L)));
        expectedPoints.add(new JsonArray(Arrays.asList(2, 1477916224866L)));
        expectedPoints.add(new JsonArray(Arrays.asList(4, 1477916224866L)));
        expectedPoints.add(new JsonArray(Arrays.asList(5, 1477916224867L)));
        expectedPoints.add(new JsonArray(Arrays.asList(6, 1477916224868L)));
        expectedPoints.add(new JsonArray(Arrays.asList(3, 1477917224866L)));
        Assert.assertEquals(new JsonObject()
                        .put("target", "fake")
                        .put("datapoints", expectedPoints)
                , extractor.getTimeSeries());
    }

    @Test
    public void testNoSamplerWithIntersectingChunks2() {
        TimeSeriesExtracter extractor = new TimeSeriesExtracterImpl("fake",
                1477895624866L , 1477917224866L,
                new SamplingConf(SamplingAlgorithm.NONE, 2, 2), 6);
        extractor.addChunk(getChunk1());
        extractor.addChunk(getChunk2());
        extractor.flush();
        Assert.assertEquals(2, extractor.chunkCount());
        Assert.assertEquals(6, extractor.pointCount());
        JsonArray expectedPoints = new JsonArray();
        expectedPoints.add(new JsonArray(Arrays.asList(1, 1477895624866L)));
        expectedPoints.add(new JsonArray(Arrays.asList(5, 1477916224867L)));
        Assert.assertEquals(new JsonObject()
                        .put("target", "fake")
                        .put("datapoints", expectedPoints)
                , extractor.getTimeSeries());
    }

}
