package com.hurence.logisland.component;


public enum ComponentType {

    PROCESSOR,
    ENGINE,
    PARSER,
    SINK,
    PROCESSOR_CHAIN;

    public String toString() {
        return name().toLowerCase();
    }
}
