package com.hurence.logisland.processor.webAnalytics.modele;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;

import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class RemoveSomeQueryParameterRemover extends AbstractQueryParameterRemover implements QueryParameterRemover {

    final Set<String> parameterToRemove;

    public RemoveSomeQueryParameterRemover(Set<String> parameterToRemove) {
        this.parameterToRemove = parameterToRemove;
    }

    @Override
    protected String removeQueryParameters(URIBuilder uriBuilder) throws URISyntaxException {
        List<NameValuePair> queryParameters = uriBuilder.getQueryParams()
                .stream()
                .filter(p -> !parameterToRemove.contains(p.getName()))
                .collect(Collectors.toList());
        if (queryParameters.isEmpty()) {
            uriBuilder.removeQuery();
        } else {
            uriBuilder.setParameters(queryParameters);
        }
        return uriBuilder.build().toString();
    }
}
