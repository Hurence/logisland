package com.hurence.logisland.processor.state;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MockStateMap implements StateMap {
    private final Map<String, String> stateValues;
    private final long version;

    public MockStateMap(final Map<String, String> stateValues, final long version) {
        this.stateValues = stateValues == null ? Collections.<String, String> emptyMap() : new HashMap<>(stateValues);
        this.version = version;
    }

    @Override
    public long getVersion() {
        return version;
    }

    @Override
    public String get(final String key) {
        return stateValues.get(key);
    }

    @Override
    public Map<String, String> toMap() {
        return Collections.unmodifiableMap(stateValues);
    }
}
