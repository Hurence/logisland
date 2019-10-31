package com.hurence.webapiservice.http.grafana.modele;

import com.hurence.logisland.timeseries.sampling.SamplingAlgorithm;
import com.hurence.webapiservice.modele.AGG;
import com.hurence.webapiservice.modele.SamplingConf;
import com.hurence.webapiservice.timeseries.TimeSeriesRequest;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class QueryRequestParam implements TimeSeriesRequest {
    private List<Target> targets;
    private long from;
    private long to;
    private String format;
    private int maxDataPoints;

    private QueryRequestParam() { }

    public List<Target> getTargets() {
        return targets;
    }

    private void setTargets(List<Target> targets) {
        this.targets = targets;
    }

    public long getFrom() {
        return from;
    }

    private void setFrom(long from) {
        this.from = from;
    }

    public long getTo() {
        return to;
    }

    private void setTo(long to) {
        this.to = to;
    }

    public String getFormat() {
        return format;
    }

    private void setFormat(String format) {
        this.format = format;
    }

    public int getMaxDataPoints() {
        return maxDataPoints;
    }

    private void setMaxDataPoints(int maxDataPoints) {
        this.maxDataPoints = maxDataPoints;
    }

    public List<AGG> getAggs() {
        return Collections.emptyList();
    }

    public SamplingConf getSamplingConf() {
        return new SamplingConf(SamplingAlgorithm.NONE, 1000, getMaxDataPoints());
    }

    @Override
    public List<String> getNames() {
        return getTargets().stream()
                .map(Target::getTarget)
                .collect(Collectors.toList());
    }


    public static final class Builder {
        private List<Target> targets;
        private long from;
        private long to;
        private String format;
        private int maxDataPoints;

        public Builder() { }

        public Builder withTargets(List<Target> targets) {
            this.targets = targets;
            return this;
        }

        public Builder from(long from) {
            this.from = from;
            return this;
        }

        public Builder to(long to) {
            this.to = to;
            return this;
        }

        public Builder withFormat(String format) {
            this.format = format;
            return this;
        }

        public Builder withMaxDataPoints(int maxDataPoints) {
            this.maxDataPoints = maxDataPoints;
            return this;
        }

        public QueryRequestParam build() {
            QueryRequestParam getTimeSerieRequestParam = new QueryRequestParam();
            getTimeSerieRequestParam.setTargets(targets);
            getTimeSerieRequestParam.setFrom(from);
            getTimeSerieRequestParam.setTo(to);
            getTimeSerieRequestParam.setFormat(format);
            getTimeSerieRequestParam.setMaxDataPoints(maxDataPoints);
            return getTimeSerieRequestParam;
        }
    }
}
