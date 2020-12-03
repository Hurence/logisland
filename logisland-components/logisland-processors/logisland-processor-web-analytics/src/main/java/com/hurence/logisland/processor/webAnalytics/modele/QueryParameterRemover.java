package com.hurence.logisland.processor.webAnalytics.modele;

import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;

public interface QueryParameterRemover {
    String removeQueryParameters(String url);
}
