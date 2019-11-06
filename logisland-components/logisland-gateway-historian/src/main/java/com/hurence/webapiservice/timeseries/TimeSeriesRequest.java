package com.hurence.webapiservice.timeseries;

import com.hurence.webapiservice.modele.AGG;
import com.hurence.webapiservice.modele.SamplingConf;

import java.util.List;

public interface TimeSeriesRequest {
    long getFrom();

    List<AGG> getAggs();

    long getTo();

    SamplingConf getSamplingConf();

    List<String> getMetricNames();

    List<String> getTags();
}
