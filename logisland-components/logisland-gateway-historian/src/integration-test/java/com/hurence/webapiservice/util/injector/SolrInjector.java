package com.hurence.webapiservice.util.injector;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;

import java.io.IOException;

public interface SolrInjector {

    void injectChunks(SolrClient client) throws SolrServerException, IOException;

}
