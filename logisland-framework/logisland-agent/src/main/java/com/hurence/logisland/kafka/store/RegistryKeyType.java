package com.hurence.logisland.kafka.store;

public enum RegistryKeyType {
    JOB("JOB"),
    TOPIC("TOPIC"),
    NOOP("NOOP");

    public final String keyType;

    private RegistryKeyType(String keyType) {
        this.keyType = keyType;
    }

    public static RegistryKeyType forName(String keyType) {
        if (JOB.keyType.equals(keyType)) {
            return JOB;
        } else if (TOPIC.keyType.equals(keyType)) {
            return TOPIC;
        } else if (NOOP.keyType.equals(keyType)) {
            return NOOP;
        } else {
            throw new IllegalArgumentException("Unknown registry key type : " + keyType
                    + " Valid key types are {config, schema}");
        }
    }
}

