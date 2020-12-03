package com.hurence.logisland.processor.webanalytics.modele;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class KeepSomeQueryParameterRemover extends AbstractQueryParameterRemover implements QueryParameterRemover {

    final Set<String> parameterToKeep;

    public KeepSomeQueryParameterRemover(Set<String> parameterToKeep, char keyValueSeparator, char parameterSeparator) {
        super(keyValueSeparator, parameterSeparator);
        this.parameterToKeep = parameterToKeep;
    }

    protected List<Map.Entry<String, String>> filterParams(Map<String, String> paramsNameValue) {
        List<Map.Entry<String, String>> paramsNameValueFiltred = paramsNameValue.entrySet().stream()
                .filter(p -> parameterToKeep.contains(p.getKey()))
                .collect(Collectors.toList());
        return paramsNameValueFiltred;
    }
}
