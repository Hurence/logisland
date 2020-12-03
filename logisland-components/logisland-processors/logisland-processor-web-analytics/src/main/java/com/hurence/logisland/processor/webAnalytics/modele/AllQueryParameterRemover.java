package com.hurence.logisland.processor.webAnalytics.modele;

import org.apache.http.client.utils.URIBuilder;

import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.Arrays;

public class AllQueryParameterRemover extends AbstractQueryParameterRemover implements QueryParameterRemover {

    @Override
    protected String removeQueryParameters(URIBuilder uriBuilder) throws URISyntaxException {
        uriBuilder.removeQuery();
        return uriBuilder.build().toString();
    }

    @Override
    protected String tryHandlingCaseNotAValidURI(String urlStr) throws UnsupportedEncodingException, URISyntaxException {
            SplittedURI guessSplittedURI = SplittedURI.fromMalFormedURI(urlStr);
            return guessSplittedURI.getBeforeQueryWithoutQuestionMark() + guessSplittedURI.getAfterQuery();
    }
}
