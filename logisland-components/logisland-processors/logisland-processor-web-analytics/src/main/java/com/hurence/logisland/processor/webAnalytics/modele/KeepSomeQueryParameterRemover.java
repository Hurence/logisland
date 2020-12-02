package com.hurence.logisland.processor.webAnalytics.modele;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class KeepSomeQueryParameterRemover extends AbstractQueryParameterRemover implements QueryParameterRemover {

    final Set<String> parameterToKeep;

    public KeepSomeQueryParameterRemover(Set<String> parameterToKeep) {
        this.parameterToKeep = parameterToKeep;
    }

    @Override
    protected String removeQueryParameters(URIBuilder uriBuilder) throws URISyntaxException {
        List<NameValuePair> queryParameters = uriBuilder.getQueryParams()
                .stream()
                .filter(p -> parameterToKeep.contains(p.getName()))
                .collect(Collectors.toList());
        if (queryParameters.isEmpty()) {
            uriBuilder.removeQuery();
        } else {
            uriBuilder.setParameters(queryParameters);
        }
        return  uriBuilder.build().toString();
    }

//    private void toString(URI uri) {
//        StringBuffer sb = new StringBuffer();
//        if (uri.getScheme() != null) {
//            sb.append(uri.getScheme());
//            sb.append(':');
//        }
//        if (isOpaque()) {
//            sb.append(schemeSpecificPart);
//        } else {
//            if (host != null) {
//                sb.append("//");
//                if (userInfo != null) {
//                    sb.append(userInfo);
//                    sb.append('@');
//                }
//                boolean needBrackets = ((host.indexOf(':') >= 0)
//                        && !host.startsWith("[")
//                        && !host.endsWith("]"));
//                if (needBrackets) sb.append('[');
//                sb.append(host);
//                if (needBrackets) sb.append(']');
//                if (port != -1) {
//                    sb.append(':');
//                    sb.append(port);
//                }
//            } else if (authority != null) {
//                sb.append("//");
//                sb.append(authority);
//            }
//            if (path != null)
//                sb.append(path);
//            if (query != null) {
//                sb.append('?');
//                sb.append(query);
//            }
//        }
//        if (fragment != null) {
//            sb.append('#');
//            sb.append(fragment);
//        }
//        string = sb.toString();
//    }
}
