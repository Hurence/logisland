package com.hurence.webapiservice.http;

import com.hurence.webapiservice.modele.AGG;
import com.hurence.webapiservice.modele.SamplingConf;
import com.hurence.webapiservice.timeseries.TimeSeriesRequest;

import java.util.Collections;
import java.util.List;

public class GetTimeSerieRequestParam implements TimeSeriesRequest {
    private List<String> names;
    private long from;
    private long to;
    private List<AGG> aggs;
    private SamplingConf samplingConf;

    private GetTimeSerieRequestParam() { }

    public List<String> getMetricNames() {
        return names;
    }

    @Override
    public List<String> getTags() {
        return Collections.emptyList();
    }

    private void setNames(List<String> names) {
        this.names = names;
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

    public List<AGG> getAggs() {
        return aggs;
    }

    private void setAggs(List<AGG> aggs) {
        this.aggs = aggs;
    }

    public SamplingConf getSamplingConf() {
        return samplingConf;
    }

    private void setSamplingConf(SamplingConf samplingConf) {
        this.samplingConf = samplingConf;
    }

    public static final class Builder {
        private List<String> names;
        private long from;
        private long to;
        private List<AGG> aggs;
        private SamplingConf samplingConf;

        public Builder() { }

        public Builder withNames(List<String> names) {
            this.names = names;
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

        public Builder withAggs(List<AGG> aggs) {
            this.aggs = aggs;
            return this;
        }

        public Builder withSamplingConf(SamplingConf samplingConf) {
            this.samplingConf = samplingConf;
            return this;
        }

        public GetTimeSerieRequestParam build() {
            GetTimeSerieRequestParam getTimeSerieRequestParam = new GetTimeSerieRequestParam();
            getTimeSerieRequestParam.setNames(names);
            getTimeSerieRequestParam.setFrom(from);
            getTimeSerieRequestParam.setTo(to);
            getTimeSerieRequestParam.setAggs(aggs);
            getTimeSerieRequestParam.setSamplingConf(samplingConf);
            return getTimeSerieRequestParam;
        }
    }
}
