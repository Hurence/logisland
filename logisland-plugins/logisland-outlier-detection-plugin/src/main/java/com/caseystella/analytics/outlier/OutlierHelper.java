/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
