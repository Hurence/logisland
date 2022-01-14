package com.hurence.logisland.processor.webanalytics.util;

import com.hurence.logisland.processor.webanalytics.modele.FirstUserVisitCompositeKey;

import java.util.Map;

public interface FirstUserVisitTimestampManager {

    /**
     * Gets the timestamp corresponding to the first visit of the user
     * Can be null if the userId was never created.
     *
     * @param key the user composite key (userId and optional additional attributes)
     * @return the timestamp
     */
    Long getFirstUserVisitTimestamp(FirstUserVisitCompositeKey key);

    Map<String, String> getAdditionalFieldMapForFirstUserVisitKey();
}
