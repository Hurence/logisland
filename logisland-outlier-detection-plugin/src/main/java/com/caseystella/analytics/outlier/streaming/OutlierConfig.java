package com.caseystella.analytics.outlier.streaming;

import com.caseystella.analytics.distribution.GlobalStatistics;
import com.caseystella.analytics.distribution.config.RotationConfig;
import com.caseystella.analytics.distribution.scaling.ScalingFunctions;
import com.google.common.collect.ImmutableList;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OutlierConfig implements Serializable {
    private RotationConfig rotationPolicy;
    private RotationConfig chunkingPolicy;
    private GlobalStatistics globalStatistics;
    private OutlierAlgorithm sketchyOutlierAlgorithm;
    private com.caseystella.analytics.outlier.batch.OutlierAlgorithm batchOutlierAlgorithm;
    private ScalingFunctions scalingFunction = null;
    private List<Double> percentilesToTrack = ImmutableList.of(0.50d, 0.75d, 0.90d, 0.95d, 0.99d);
    private List<String> groupingKeys;
    private Map<String, Object> config = new HashMap<>();

    public List<Double> getPercentilesToTrack() {
        return percentilesToTrack;
    }

    public void setPercentilesToTrack(List<Double> percentilesToTrack) {
        this.percentilesToTrack = percentilesToTrack;
    }

    public List<String> getGroupingKeys() {
        return groupingKeys;
    }

    public void setGroupingKeys(List<String> groupingKeys) {
        this.groupingKeys = groupingKeys;
    }

    public Map<String, Object> getConfig() {
        return config;
    }

    public GlobalStatistics getGlobalStatistics() {
        return globalStatistics;
    }

    public OutlierAlgorithm getSketchyOutlierAlgorithm() {
        return sketchyOutlierAlgorithm;
    }

    public void setSketchyOutlierAlgorithm(String sketchyOutlierAlgorithm) {
        this.sketchyOutlierAlgorithm = OutlierAlgorithms.newInstance(sketchyOutlierAlgorithm);
    }
    public com.caseystella.analytics.outlier.batch.OutlierAlgorithm getBatchOutlierAlgorithm() {
        return batchOutlierAlgorithm;
    }

    public void setBatchOutlierAlgorithm(String batchOutlierAlgorithm) {
        this.batchOutlierAlgorithm= com.caseystella.analytics.outlier.batch.OutlierAlgorithms.newInstance(batchOutlierAlgorithm);
    }

    public void setGlobalStatistics(GlobalStatistics globalStatistics) {
        this.globalStatistics = globalStatistics;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }

    public ScalingFunctions getScalingFunction() {
        if(scalingFunction != null) {
            return scalingFunction;
        }
        else {
            if(globalStatistics != null && globalStatistics.getMin() != null && globalStatistics.getMin() < 0) {
                scalingFunction = ScalingFunctions.SHIFT_TO_POSITIVE;
            }
            else {
                scalingFunction = ScalingFunctions.NONE;
            }
            return scalingFunction;
        }
    }

    public void setScalingFunction(ScalingFunctions scalingFunction) {
        this.scalingFunction = scalingFunction;
    }

    public RotationConfig getRotationPolicy() {
        return rotationPolicy;
    }

    public void setRotationPolicy(RotationConfig rotationPolicy) {
        this.rotationPolicy = rotationPolicy;
    }

    public RotationConfig getChunkingPolicy() {
        return chunkingPolicy;
    }

    public void setChunkingPolicy(RotationConfig chunkingPolicy) {
        this.chunkingPolicy = chunkingPolicy;
    }

}
