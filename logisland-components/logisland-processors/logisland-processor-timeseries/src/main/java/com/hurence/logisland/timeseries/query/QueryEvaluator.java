/*
 * Copyright (C) 2016 QAware GmbH
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.hurence.logisland.timeseries.query;


import com.hurence.logisland.timeseries.functions.*;
import com.hurence.logisland.timeseries.metric.ChronixType;
import com.hurence.logisland.timeseries.metric.ChronixTypes;
import com.hurence.logisland.timeseries.metric.MetricType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author f.lautenschlager
 */
public final class QueryEvaluator {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryEvaluator.class);

    private static final String AGGREGATION_ARGUMENT_DELIMITER = ":";
    private static final String AGGREGATION_ARGUMENT_SPLITTER = ",";
    private static final String FUNCTION_ARGUMENT_SPLITTER = ";";
    private static final String TYPE_SPLITS = ".*\\{.*\\}?(.*\\{.*\\})+";

    private static final String TYPE_FUNCTION_START = "{";

    /**
     * Analyzes the filter queries and parses them for chronix fucntions
     *
     * @param chronixFunctions the filter queries (solr api)
     * @return a set of chronix analyses asked in the filter queries
     */
    public static TypeFunctions extractFunctions(String[] chronixFunctions) {

        //The result that contains the asked analyses
        final TypeFunctions result = new TypeFunctions();
        //Check if there are filter queries with functions
        if (isEmpty(chronixFunctions)) {
            //return a empty result
            return result;
        }

        //Placeholder for arguments
        String[] arguments = new String[0];

        //Iterate over all filter queries
        for (String unmodifiedAnalysis : chronixFunctions) {

            //For each type
            String[] types = unmodifiedAnalysis.split(TYPE_SPLITS);

            for (String type : types) {

                //split the type functions
                String typeName = type.substring(0, type.indexOf(TYPE_FUNCTION_START));

                ChronixType chronixType = new MetricType();//plugInTypes.getTypeForName(typeName);

                if (chronixType == null) {
                    LOGGER.info("Type {} not supported", typeName);
                    continue;
                }

                QueryFunctions resultingTypeFunctions = new QueryFunctions();
                String typeFunctions = type.substring(type.indexOf(TYPE_FUNCTION_START)).replaceAll("(\\{|\\})", "");
                String[] queryFunctions = typeFunctions.split(FUNCTION_ARGUMENT_SPLITTER);

                //run over the functions
                for (String queryFunction : queryFunctions) {
                    //function has an argument
                    if (queryFunction.contains(AGGREGATION_ARGUMENT_DELIMITER)) {
                        arguments = extractAggregationParameter(queryFunction);
                        queryFunction = queryFunction.substring(0, queryFunction.indexOf(AGGREGATION_ARGUMENT_DELIMITER));
                    }

                    ChronixFunction chronixFunction = chronixType.getFunction(queryFunction, arguments);

                    //No function found.

                    if (chronixFunction == null) {
                        LOGGER.debug("Could not find custom function {} for type {}", queryFunction, typeName);
                        continue;
                    }


                    switch (chronixFunction.getType()) {
                        case AGGREGATION:
                            resultingTypeFunctions.addAggregation((ChronixAggregation) chronixFunction);
                            break;
                        case TRANSFORMATION:
                            resultingTypeFunctions.addTransformation((ChronixTransformation) chronixFunction);
                            break;
                        case ANALYSIS:
                            resultingTypeFunctions.addAnalysis((ChronixAnalysis) chronixFunction);
                            break;
                        case ENCODING:
                            resultingTypeFunctions.addEncoding((ChronixEncoding) chronixFunction);
                            break;
                        default:
                            //ignore
                            break;

                    }
                }
                result.setTypeFunctions(chronixType, resultingTypeFunctions);
            }
        }

        return result;
    }


    /**
     * Helper to check if the given string array is empty.
     * A string is empty if it is null, or ""
     *
     * @param fqs the string array
     * @return true if empty, otherwise false
     */

    private static boolean isEmpty(String[] fqs) {
        if (fqs == null || fqs.length == 0) {
            return true;
        }

        for (String fq : fqs) {
            if (!fq.isEmpty()) {
                return false;
            }
        }
        return true;
    }


    private static String[] extractAggregationParameter(String argumentString) {
        String arguments = extractArguments(argumentString);
        return arguments.split(AGGREGATION_ARGUMENT_SPLITTER);
    }

    private static String extractArguments(String argumentString) {
        return extract(argumentString, AGGREGATION_ARGUMENT_DELIMITER);
    }

    private static String extract(String argumentString, String aggregationArgumentDelimiter) {
        int index = argumentString.indexOf(aggregationArgumentDelimiter);
        if (index == -1) {
            throw new IllegalStateException("Invalid query syntax. No delimiter '" + aggregationArgumentDelimiter + "' found");
        }
        return argumentString.substring(index + 1);
    }
}
