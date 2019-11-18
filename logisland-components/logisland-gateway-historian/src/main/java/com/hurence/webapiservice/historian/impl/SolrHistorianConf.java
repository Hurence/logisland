package com.hurence.webapiservice.historian.impl;

import org.apache.solr.client.solrj.SolrClient;

public class SolrHistorianConf {
    public SolrClient client;
    public String collection;
    public String streamEndPoint;
    public long limitNumberOfPoint;
    public long limitNumberOfChunks;
    public long sleepDurationBetweenTry;
    public int numberOfRetryToConnect;

    public SolrHistorianConf() {
    }

    public SolrHistorianConf(SolrClient client, String collection, String streamEndPoint, long limitNumberOfPoint, long limitNumberOfChunks, long sleepDurationBetweenTry, int numberOfRetryToConnect) {
        this.client = client;
        this.collection = collection;
        this.streamEndPoint = streamEndPoint;
        this.limitNumberOfPoint = limitNumberOfPoint;
        this.limitNumberOfChunks = limitNumberOfChunks;
        this.sleepDurationBetweenTry = sleepDurationBetweenTry;
        this.numberOfRetryToConnect = numberOfRetryToConnect;
    }


}