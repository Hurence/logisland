package com.hurence.webapiservice.historian;

import com.hurence.logisland.record.FieldDictionary;

/**
 * Static class to put field names used by HistorianService.
 */
/*
 Does not move those fields inside HistorianService for any reason !
 Indeed it is hard to refactor HistorianService as there is many other generated classes from HistorianService.
 THe source code generated from HistorianService copy paste static variables... So When you refactor them it is not
 taking in account by your code referencing auto generated source code.
 */
public class HistorianFields {
    private HistorianFields() {}
    //Request fields
    public static String FROM_REQUEST_FIELD = "from";
    public static String TO_REQUEST_FIELD = "to";
    public static String FIELDS_TO_FETCH_AS_LIST_REQUEST_FIELD = "fields";
    public static String TAGS_TO_FILTER_ON_REQUEST_FIELD = "tags";
    public static String METRIC_NAMES_AS_LIST_REQUEST_FIELD = "names";
    public static String MAX_TOTAL_CHUNKS_TO_RETRIEVE_REQUEST_FIELD = "total_max_chunks";
    public static String SAMPLING_ALGO_REQUEST_FIELD = "sampling_algo";
    public static String BUCKET_SIZE_REQUEST_FIELD = "bucket_size";
    public static String MAX_POINT_BY_METRIC_REQUEST_FIELD = "max_points_to_return_by_metric";

    //Response fields
    public static String TOTAL_POINTS_RESPONSE_FIELD = "total_points";
    public static String TIMESERIES_RESPONSE_FIELD = "timeseries";

    public static String RESPONSE_CHUNKS = "chunks";
    public static String RESPONSE_METRICS = "metrics";
    public static String RESPONSE_TOTAL_FOUND = "total_hit";
    public static String RESPONSE_METRIC_NAME_FIELD = "name";
    public static String RESPONSE_TAG_NAME_FIELD = "tagname";
    public static String RESPONSE_CHUNK_ID_FIELD = "id";
    public static String RESPONSE_CHUNK_VERSION_FIELD = "_version_";
    public static String RESPONSE_CHUNK_VALUE_FIELD = FieldDictionary.CHUNK_VALUE;
    public static String RESPONSE_CHUNK_MAX_FIELD = FieldDictionary.CHUNK_MAX;
    public static String RESPONSE_CHUNK_MIN_FIELD = FieldDictionary.CHUNK_MIN;
    public static String RESPONSE_CHUNK_START_FIELD = FieldDictionary.CHUNK_START;
    public static String RESPONSE_CHUNK_END_FIELD = FieldDictionary.CHUNK_END;
    public static String RESPONSE_CHUNK_AVG_FIELD = FieldDictionary.CHUNK_AVG;
    public static String RESPONSE_CHUNK_SIZE_FIELD = FieldDictionary.CHUNK_SIZE;
    public static String RESPONSE_CHUNK_SUM_FIELD = FieldDictionary.CHUNK_SUM;
    public static String RESPONSE_CHUNK_SAX_FIELD = FieldDictionary.CHUNK_SAX;
    public static String RESPONSE_CHUNK_WINDOW_MS_FIELD = FieldDictionary.CHUNK_WINDOW_MS;
    public static String RESPONSE_CHUNK_TREND_FIELD = FieldDictionary.CHUNK_TREND;
    public static String RESPONSE_CHUNK_SIZE_BYTES_FIELD = FieldDictionary.CHUNK_SIZE_BYTES;
}
