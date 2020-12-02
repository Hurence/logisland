package com.hurence.logisland.processor.webAnalytics.modele;

import org.apache.http.client.utils.URIBuilder;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;

public abstract class AbstractQueryParameterRemover implements QueryParameterRemover {

    protected abstract String removeQueryParameters(URIBuilder uriBuilder) throws URISyntaxException;

    public String removeQueryParameters(String url) throws UnsupportedEncodingException, URISyntaxException {
        try {
            URIBuilder uriBuilder = new URIBuilder(url);
            return removeQueryParameters(uriBuilder);
        } catch (URISyntaxException e) {
            return tryHandlingCaseNotAValidURI(url);
        }
    }

    protected String tryHandlingCaseNotAValidURI(String urlStr) throws UnsupportedEncodingException, URISyntaxException {
        return "toto";
//        try {
//            String queryPart = urlStr.substring(urlStr.indexOf("?"), Integer.MAX_VALUE);
//            EncodedQueryPart =
//            URI uri = new URI(urlStr);
//            return "toto";
//            URIBuilder uriBuilder =  new URIBuilder(java.net.URLEncoder.encode(urlStr, "UTF-8"));
//            String queryString = uri.getQuery();
//            URIBuilder uriBuilder = new URIBuilder(url.toURI());
//            String newUriEncoded = removeQueryParameters(uriBuilder);
//            return java.net.URLDecoder.decode(newUriEncoded, "UTF-8");
//        } catch (MalformedURLException e) {
//            throw new RuntimeException("dans ton cul", e);
//        }

    }
}
