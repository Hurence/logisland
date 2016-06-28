package com.caseystella.analytics.outlier;

import com.caseystella.analytics.DataPoint;


import java.util.*;

public enum OutlierHelper {
    INSTANCE;
    public Map<String, Object> toJson(DataPoint dp) {
        Map<String, Object> json = new HashMap<>();
        json.put("timestamp", dp.getTimestamp());
        json.put("value", dp.getValue());
        json.put("source", dp.getSource());
        Set<String> constants = new HashSet<>();
        for(OutlierMetadataConstants constant : OutlierMetadataConstants.values()) {
            constants.add(constant.toString());
            json.put(constant.toString(), Double.valueOf(dp.getMetadata().get(constant.toString())));
        }
        for(Map.Entry<String, String> kv : dp.getMetadata().entrySet()) {
            if(!constants.contains(kv.getKey())) {
                json.put(kv.getKey(), kv.getValue());
            }
        }
        return json;
    }
}
