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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class KeepSomeQueryParameterRemover extends AbstractQueryParameterRemover implements QueryParameterRemover {

    private final Set<String> parameterToKeep;

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
