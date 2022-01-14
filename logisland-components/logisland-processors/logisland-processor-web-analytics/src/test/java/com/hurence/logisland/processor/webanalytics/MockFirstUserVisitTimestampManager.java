package com.hurence.logisland.processor.webanalytics;

import com.hurence.logisland.processor.webanalytics.modele.FirstUserVisitCompositeKey;
import com.hurence.logisland.processor.webanalytics.util.FirstUserVisitTimestampManager;

import java.util.HashMap;
import java.util.Map;

public class MockFirstUserVisitTimestampManager implements FirstUserVisitTimestampManager {
    @Override
    public Long getFirstUserVisitTimestamp(FirstUserVisitCompositeKey key) {
        return 1641548905L;
    }

    @Override
    public Map<String, String> getAdditionalFieldMapForFirstUserVisitKey() {
        return new HashMap<>();
    }
}
