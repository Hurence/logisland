package com.hurence.logisland.processor.webAnalytics.modele;

import org.apache.http.client.utils.URIBuilder;

import java.net.URISyntaxException;

public class AllQueryParameterRemover implements QueryParameterRemover {

    public String removeQueryParameters(String url) throws URISyntaxException {
        URIBuilder uriBuilder = new URIBuilder(url);
        uriBuilder.removeQuery();
        return uriBuilder.build().toString();
    }
}
