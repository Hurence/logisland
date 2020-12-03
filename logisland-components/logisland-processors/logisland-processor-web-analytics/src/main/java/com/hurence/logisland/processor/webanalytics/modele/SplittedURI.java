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
