package com.hurence.logisland.processor.webAnalytics.modele;

import org.apache.http.client.utils.URIBuilder;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public abstract class AbstractQueryParameterRemover implements QueryParameterRemover {

    protected abstract String removeQueryParameters(URIBuilder uriBuilder) throws URISyntaxException;

    public String removeQueryParameters(String url) throws UnsupportedEncodingException, URISyntaxException {
        return tryHandlingCaseNotAValidURI(url);
//        try {
//            URIBuilder uriBuilder = new URIBuilder(url);
//            return removeQueryParameters(uriBuilder);
//        } catch (URISyntaxException e) {
//            return tryHandlingCaseNotAValidURI(url);
//        }
    }

    /**
     * If input is not a valid URI, this may be because the URI has already been decoded.
     * @param urlStr
     * @return
     * @throws UnsupportedEncodingException
     * @throws URISyntaxException
     */
    protected abstract String tryHandlingCaseNotAValidURI(String urlStr) throws UnsupportedEncodingException, URISyntaxException;
}
