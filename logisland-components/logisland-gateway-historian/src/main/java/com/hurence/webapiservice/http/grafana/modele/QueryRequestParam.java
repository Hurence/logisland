package com.hurence.webapiservice.http.grafana.modele;

import com.hurence.logisland.timeseries.sampling.SamplingAlgorithm;
import com.hurence.webapiservice.modele.AGG;
import com.hurence.webapiservice.modele.SamplingConf;

import java.util.Collections;
import java.util.List;

public class QueryRequestParam {
    private List<Target> targets;
    private long from;
    private long to;
    private String format;
    private long maxDataPoints;

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

    public long getMaxDataPoints() {
        return maxDataPoints;
    }

    private void setMaxDataPoints(long maxDataPoints) {
        this.maxDataPoints = maxDataPoints;
    }

    public List<AGG> getAggs() {
        return Collections.emptyList();
    }

    public SamplingConf getSamplingConf() {
        return new SamplingConf(SamplingAlgorithm.NONE, 1000, getMaxDataPoints());
    }


    public static final class Builder {
        private List<Target> targets;
        private long from;
        private long to;
        private String format;
        private long maxDataPoints;

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

        public Builder withMaxDataPoints(long maxDataPoints) {
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
