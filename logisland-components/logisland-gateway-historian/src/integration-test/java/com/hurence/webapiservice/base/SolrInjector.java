package com.hurence.webapiservice.base;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;

import java.io.IOException;

public interface SolrInjector {

    void injectChunks(SolrClient client) throws SolrServerException, IOException;
}
