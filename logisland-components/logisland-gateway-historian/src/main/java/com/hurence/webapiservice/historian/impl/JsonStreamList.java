//package com.hurence.webapiservice.historian.impl;
//
//import io.vertx.core.json.JsonObject;
//import org.apache.solr.client.solrj.io.Tuple;
//import org.apache.solr.client.solrj.io.stream.TupleStream;
//
//import java.io.IOException;
//import java.util.List;
//
//public class JsonStreamList implements JsonStream {
//
//    private List<JsonObject> results;
//    private int index = 0;
//
//    public JsonStreamList(List<JsonObject> results) {
//        this.results = results;
//    }
//
//    @Override
//    public void open() throws IOException {
//        //nothing
//        index = 0;
//        //(first request ?) then paging ?
//    }
//
//    @Override
//    public JsonObject read() throws IOException {
//        return results.get(index);
//    }
//
//    @Override
//    public long getNumberOfDocRead() {
//        return results.size();
//    }
//
//    @Override
//    public void close() throws IOException {
//        //nothing
//    }
//}
