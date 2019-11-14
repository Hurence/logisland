package com.hurence.webapiservice.historian.impl;

import io.vertx.core.json.JsonObject;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.TupleStream;

import java.io.IOException;

public class JsonStreamSolrStreamImpl implements JsonStream {

    private TupleStream stream;
    private long counter = 0L;

    public JsonStreamSolrStreamImpl(TupleStream stream) {
        this.stream = stream;
    }

    @Override
    public void open() throws IOException {
        stream.open();
    }

    @Override
    public JsonObject read() throws IOException {
        Tuple tuple = stream.read();
        counter++;
        return toJson(tuple);
    }

    @Override
    public long getNumberOfDocRead() {
        return counter;
    }

    private JsonObject toJson(Tuple tuple) {
        @SuppressWarnings("unchecked")
        final JsonObject json = new JsonObject(tuple.fields);
        return json;
    }

    @Override
    public void close() throws IOException {
        stream.close();
    }
}
