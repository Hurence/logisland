package com.hurence.logisland.processor.webAnalytics.modele;

import java.net.URISyntaxException;

public interface QueryParameterRemover {
    String removeQueryParameters(String url) throws URISyntaxException;
}
