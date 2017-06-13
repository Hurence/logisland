/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
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
package com.hurence.logisland.processor;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;
import uk.co.flax.luwak.MonitorQueryParser;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * A query parser that uses the default Lucene parser
 */
public class NumericQueryParser implements MonitorQueryParser {

    Set<String> numericFields = new HashSet<>();

    public void setNumericField(String numericField) {

        if (!numericFields.contains(numericField)) {
            numericFields.add(numericField);
        }
    }

    class NumericRangeQueryParser
            extends QueryParser {

        Set<String> numericFields = new HashSet<>();

        public NumericRangeQueryParser(Set<String> numericFields, String f, Analyzer a) {
            super(f, a);
            this.numericFields = numericFields;
        }

        protected Query newRangeQuery(String field, String part1, String part2, boolean startInclusive,
                                      boolean endInclusive) {

            if (numericFields.contains(field)) {
                return NumericRangeQuery.newDoubleRange(field, Double.parseDouble(part1), Double.parseDouble(part2),
                        startInclusive, endInclusive);
            }
            return (TermRangeQuery) super.newRangeQuery(field, part1, part2, startInclusive, endInclusive);
        }


        protected Query newTermQuery(Term term) {
            if (numericFields.contains(term.field())) {

                BytesRefBuilder byteRefBuilder = new BytesRefBuilder();
                NumericUtils.intToPrefixCoded(Integer.parseInt(term.text()), 0, byteRefBuilder);
                TermQuery tq = new TermQuery(new Term(term.field(), byteRefBuilder.get()));

                return tq;
            }
            return super.newTermQuery(term);

        }
    }

    private final String defaultField;
    private final Analyzer analyzer;

    /**
     * Creates a parser with a given default field and analyzer
     *
     * @param defaultField the default field
     * @param analyzer     an analyzer to use to analyzer query terms
     */
    public NumericQueryParser(String defaultField, Analyzer analyzer) {
        this.defaultField = defaultField;
        this.analyzer = analyzer;
    }

    /**
     * Creates a parser using a lucene {@link StandardAnalyzer}
     *
     * @param defaultField the default field
     */
    public NumericQueryParser(String defaultField) {
        this(defaultField, new StandardAnalyzer());
    }

    @Override
    public Query parse(String query, Map<String, String> metadata) throws Exception {
        return new NumericRangeQueryParser(numericFields, defaultField, analyzer).parse(query);
    }
}

