package com.hurence.logisland.processor.webanalytics.modele;

import java.util.Map;
import java.util.Objects;

/**
 * Represents the key used for first user visit timestamp computation.
 * Besides the userId, it is possible to add more fields (key value mappings) to perform more specific aggregations
 * e.g. an additional field can be a subscription ID to compute the first user visit in the scope of this subscription
 */
public class FirstUserVisitCompositeKey {
    final private String userId;
    final private Map<String, String> additionalAttributeMap;

    public FirstUserVisitCompositeKey(String userId, Map<String, String> additionalAttributeMap) {
        this.userId = userId;
        this.additionalAttributeMap = additionalAttributeMap;
    }

    public String getUserId() {
        return userId;
    }

    public Map<String, String> getAdditionalAttributeMap() {
        return additionalAttributeMap;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FirstUserVisitCompositeKey that = (FirstUserVisitCompositeKey) o;
        return Objects.equals(userId, that.userId) && Objects.equals(additionalAttributeMap, that.additionalAttributeMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, additionalAttributeMap);
    }
}
