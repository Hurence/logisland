package com.hurence.webapiservice.historian.impl;

import io.vertx.core.json.JsonObject;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;

public interface JsonStream extends Closeable, Serializable {

    void open() throws IOException;

    JsonObject read() throws IOException;
}
