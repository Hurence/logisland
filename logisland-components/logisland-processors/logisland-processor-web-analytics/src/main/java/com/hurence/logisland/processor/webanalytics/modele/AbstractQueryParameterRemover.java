package com.hurence.logisland.processor.webanalytics.modele;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class AbstractQueryParameterRemover implements  QueryParameterRemover {
    final char keyValueSeparator;
    final char parameterSeparator;

    public AbstractQueryParameterRemover(char keyValueSeparator, char parameterSeparator) {
        this.keyValueSeparator = keyValueSeparator;
        this.parameterSeparator = parameterSeparator;
    }

    public String removeQueryParameters(String urlStr) {
        SplittedURI guessSplittedURI = SplittedURI.fromMalFormedURI(urlStr);
        if (guessSplittedURI.getQuery().isEmpty()) return urlStr;
        Map<String, String> paramsNameValue = Arrays.stream(guessSplittedURI.getQuery().split(String.valueOf(parameterSeparator)))
                .map(queryString -> queryString.split(String.valueOf(keyValueSeparator)))
                .collect(Collectors.toMap(
                        keyValueArr -> keyValueArr[0],
                        keyValueArr -> {
                            String[] values = Arrays.copyOfRange(keyValueArr, 1, keyValueArr.length);
                            return String.join("", values);
                        },
                        (x, y) -> y,
                        LinkedHashMap::new
                ));
        List<Map.Entry<String, String>> paramsNameValueFiltred = filterParams(paramsNameValue);
        if (paramsNameValueFiltred.isEmpty()) {
            return guessSplittedURI.getBeforeQueryWithoutQuestionMark() + guessSplittedURI.getAfterQuery();
        } else {
            String newQueryString = paramsNameValueFiltred.stream()
                    .map(entry -> {
                        if (entry.getValue().isEmpty()) {
                            return entry.getKey();
                        } else {
                            return entry.getKey() + keyValueSeparator + entry.getValue();
                        }
                    })
                    .collect(Collectors.joining(String.valueOf(parameterSeparator)));
            return guessSplittedURI.getBeforeQuery() +
                    newQueryString +
                    guessSplittedURI.getAfterQuery();
        }
    }

    protected abstract List<Map.Entry<String, String>> filterParams(Map<String, String> paramsNameValue);
}
