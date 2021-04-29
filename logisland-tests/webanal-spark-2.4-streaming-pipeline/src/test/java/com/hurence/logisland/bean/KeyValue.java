package com.hurence.logisland.bean;

import java.io.Serializable;

public class KeyValue implements Serializable {

    public String key;
    public Long count;

    public KeyValue(String key, Long count) {
        this.key = key;
        this.count = count;
    }

}
