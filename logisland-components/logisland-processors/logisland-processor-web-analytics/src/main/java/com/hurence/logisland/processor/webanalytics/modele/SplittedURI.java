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

public class SplittedURI {
        private String beforeQuery;
        private String query;
        private String afterQuery;

        private SplittedURI(String beforeQuery, String query, String afterQuery) {
            this.beforeQuery = beforeQuery;
            this.query = query;
            this.afterQuery = afterQuery;
        }

        public String getBeforeQuery() {
            return beforeQuery;
        }

        public String getBeforeQueryWithoutQuestionMark() {
            if (beforeQuery.isEmpty()) return beforeQuery;
            char lastChar = beforeQuery.charAt(beforeQuery.length() - 1);
            if (lastChar == '?') return beforeQuery.substring(0, beforeQuery.length() - 1);
            return beforeQuery;
    }

        public String getQuery() {
            return query;
        }

        public String getAfterQuery() {
            return afterQuery;
        }

    public static SplittedURI fromMalFormedURI(String malformedUri) {
        if (malformedUri.isEmpty()) return new SplittedURI("", "", "");
        //select from first ? to # or end
        String beforeQueryString = "";
        String queryString = "";
        String afterQueryString = "";
        int indexStr = 0;
        int urlSize = malformedUri.length();
        char currentChar = malformedUri.charAt(0);
        while(currentChar != '?' && currentChar != '#') {
            beforeQueryString += currentChar;
            indexStr++;
            if (indexStr >= urlSize) return new SplittedURI(beforeQueryString, queryString, afterQueryString);
            currentChar=malformedUri.charAt(indexStr);
        }
        if (currentChar == '?') {
            beforeQueryString += currentChar;
            indexStr++;
            if (indexStr >= urlSize) return new SplittedURI(beforeQueryString, queryString, afterQueryString);
            currentChar=malformedUri.charAt(indexStr);
            while(currentChar != '#') {
                queryString += currentChar;
                indexStr++;
                if (indexStr >= urlSize) return new SplittedURI(beforeQueryString, queryString, afterQueryString);
                currentChar=malformedUri.charAt(indexStr);
            }
        }
        while(true) {
            afterQueryString += currentChar;
            indexStr++;
            if (indexStr >= urlSize) return new SplittedURI(beforeQueryString, queryString, afterQueryString);
            currentChar=malformedUri.charAt(indexStr);
        }
    }
}
