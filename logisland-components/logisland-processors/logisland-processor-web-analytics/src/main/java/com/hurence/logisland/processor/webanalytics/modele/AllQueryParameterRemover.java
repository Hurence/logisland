package com.hurence.logisland.processor.webanalytics.modele;

public class AllQueryParameterRemover implements QueryParameterRemover {

    public String removeQueryParameters(String urlStr) {
            SplittedURI guessSplittedURI = SplittedURI.fromMalFormedURI(urlStr);
            return guessSplittedURI.getBeforeQueryWithoutQuestionMark() + guessSplittedURI.getAfterQuery();
    }
}
