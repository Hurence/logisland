package com.hurence.logisland.processor.webAnalytics.modele;

import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.*;
import java.util.stream.Collectors;

public class RemoveSomeQueryParameterRemover implements QueryParameterRemover {

    final Set<String> parameterToRemove;

    public RemoveSomeQueryParameterRemover(Set<String> parameterToRemove) {
        this.parameterToRemove = parameterToRemove;
    }

    public String removeQueryParameters(String urlStr) {
        SplittedURI guessSplittedURI = SplittedURI.fromMalFormedURI(urlStr);
        if (guessSplittedURI.getQuery().isEmpty()) return urlStr;
        Map<String, String> paramsNameValue = Arrays.stream(guessSplittedURI.getQuery().split("&"))
                .map(queryString -> queryString.split("="))
                .collect(Collectors.toMap(
                        keyValueArr -> keyValueArr[0],
                        keyValueArr -> {
                            String[] values = Arrays.copyOfRange(keyValueArr, 1, keyValueArr.length);
                            return String.join("", values);
                        },
                        (x, y) -> y,
                        LinkedHashMap::new
                ));
        List<Map.Entry<String, String>> paramsNameValueFiltred = paramsNameValue.entrySet().stream()
                .filter(p -> !parameterToRemove.contains(p.getKey()))
                .collect(Collectors.toList());
        if (paramsNameValueFiltred.isEmpty()) {
            return guessSplittedURI.getBeforeQueryWithoutQuestionMark() + guessSplittedURI.getAfterQuery();
        } else {
            String newQueryString = paramsNameValueFiltred.stream()
                    .map(entry -> entry.getKey() + "=" + entry.getValue())
                    .collect(Collectors.joining("&"));
            return guessSplittedURI.getBeforeQuery() +
                    newQueryString +
                    guessSplittedURI.getAfterQuery();
        }
    }
}
