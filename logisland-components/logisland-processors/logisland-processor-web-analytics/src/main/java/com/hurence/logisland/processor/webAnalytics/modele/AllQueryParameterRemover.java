package com.hurence.logisland.processor.webAnalytics.modele;

import org.apache.http.client.utils.URIBuilder;

import java.net.URISyntaxException;

public class AllQueryParameterRemover extends AbstractQueryParameterRemover implements QueryParameterRemover {

    @Override
    protected String removeQueryParameters(URIBuilder uriBuilder) throws URISyntaxException {
        uriBuilder.removeQuery();
        return uriBuilder.build().toString();
    }
}
