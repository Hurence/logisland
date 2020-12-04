/**
 * Copyright (C) 2020 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.processor.webanalytics.modele;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class AbstractQueryParameterRemover implements  QueryParameterRemover {
    private final char keyValueSeparator;
    private final char parameterSeparator;

    public AbstractQueryParameterRemover(char keyValueSeparator, char parameterSeparator) {
        this.keyValueSeparator = keyValueSeparator;
        this.parameterSeparator = parameterSeparator;
    }

    public String removeQueryParameters(String urlStr) {
        SplittedURI guessSplittedURI = SplittedURI.fromMalFormedURI(urlStr);
        if (guessSplittedURI.getQuery().isEmpty()) return urlStr;
        Map<String, String> paramsNameValue = Arrays.stream(guessSplittedURI.getQuery().split(String.valueOf(parameterSeparator)))
                .map(queryString -> {
                    String[] split = queryString.split(String.valueOf(keyValueSeparator));
                    if (split.length==1 && queryString.contains(String.valueOf(keyValueSeparator))) {
                        return new String[]{split[0], ""};
                    } else {
                        return split;
                    }
                })
                .collect(LinkedHashMap::new,
                        (map, keyValueArr) -> {
                            String[] values = Arrays.copyOfRange(keyValueArr, 1, keyValueArr.length);
                            if (values.length == 0) {
                                map.put(keyValueArr[0], null);
                            } else {
                                map.put(keyValueArr[0], String.join("", values));
                            }
                        },
                        LinkedHashMap::putAll);

        List<Map.Entry<String, String>> paramsNameValueFiltred = filterParams(paramsNameValue);
        if (paramsNameValueFiltred.isEmpty()) {
            return guessSplittedURI.getBeforeQueryWithoutQuestionMark() + guessSplittedURI.getAfterQuery();
        } else {
            String newQueryString = paramsNameValueFiltred.stream()
                    .map(entry -> {
                        if (entry.getValue() == null) {
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
