package com.hurence.logisland.processor.webAnalytics.modele;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;

import java.net.URISyntaxException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class KeepSomeQueryParameterRemover implements QueryParameterRemover {

    final Set<String> parameterToKeep;

    public KeepSomeQueryParameterRemover(Set<String> parameterToKeep) {
        this.parameterToKeep = parameterToKeep;
    }

    public String removeQueryParameters(String url) throws URISyntaxException {
        URIBuilder uriBuilder = new URIBuilder(url);
        List<NameValuePair> queryParameters = uriBuilder.getQueryParams()
                .stream()
                .filter(p -> parameterToKeep.contains(p.getName()))
                .collect(Collectors.toList());
        if (queryParameters.isEmpty()) {
            uriBuilder.removeQuery();
        } else {
            uriBuilder.setParameters(queryParameters);
        }
        return uriBuilder.build().toString();
    }
}
