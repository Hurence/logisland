package com.hurence.logisland.processor.webAnalytics.util;

import com.hurence.logisland.processor.webAnalytics.IncrementalWebSession;
import com.hurence.logisland.processor.webAnalytics.modele.TestMappings;
import com.hurence.logisland.processor.webAnalytics.modele.WebSession;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.service.elasticsearch.ElasticsearchClientService;
import com.hurence.logisland.util.runner.MockRecord;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

import static com.hurence.logisland.processor.webAnalytics.IncrementalWebSession.defaultOutputFieldNameForEsIndex;
import static com.hurence.logisland.processor.webAnalytics.IncrementalWebSession.defaultOutputFieldNameForEsType;

public class ElasticsearchServiceUtil {

    public static final String SESSION_SUFFIX_FORMATTER_STRING = "yyyy.MM.dd";
    public static final String EVENT_SUFFIX_FORMATTER_STRING = "yyyy.MM.dd";
    public static final DateTimeFormatter SESSION_SUFFIX_FORMATTER = DateTimeFormatter.ofPattern(
            SESSION_SUFFIX_FORMATTER_STRING,
            Locale.ENGLISH
    );
    public static final String SESSION_INDEX_PREFIX = "openanalytics_websessions-";
    public static final DateTimeFormatter EVENT_SUFFIX_FORMATTER = DateTimeFormatter.ofPattern(
            EVENT_SUFFIX_FORMATTER_STRING,
            Locale.ENGLISH
    );
    public static final String EVENT_INDEX_PREFIX = "openanalytics_webevents.";

//    public static void injectSessionsThenRefresh(ElasticsearchClientService esClientService,
//                                                 List<MockRecord> sessions) {
//        injectSessionsWithoutRefreshing(esClientService, sessions);
//        String[] indicesToWaitFor = sessions.stream()
//                .map(session -> session.getField(defaultOutputFieldNameForEsIndex).asString())
//                .toArray(String[]::new);
//        esClientService.waitUntilCollectionIsReadyAndRefreshIfAnyPendingTasks(indicesToWaitFor, 100000L);
//    }
//
//    public static void injectSessionsWithoutRefreshing(ElasticsearchClientService esClientService,
//                                      List<MockRecord> sessions) {
//        final String sessionType = "sessions";
//        sessions.forEach(session -> {
//
//            String sessionIndex = toSessionIndexName(session.getField(TestMappings.sessionInternalFields.getTimestampField()).asLong());
//            esClientService.bulkPut( sessionIndex + "," + sessionType, session);
//        });
//        esClientService.bulkFlush();
//    }

    public static SearchResponse getAllSessionsRaw(ElasticsearchClientService esClientService,
                                                   RestHighLevelClient esclient) throws IOException {
        esClientService.waitUntilCollectionIsReadyAndRefreshIfAnyPendingTasks(SESSION_INDEX_PREFIX + "*", 100000L);
        SearchRequest searchRequest = new SearchRequest(SESSION_INDEX_PREFIX + "*");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
        return esclient.search(searchRequest, RequestOptions.DEFAULT);
    }

    public static List<WebSession> getAllSessions(ElasticsearchClientService esClientService,
                                                  RestHighLevelClient esclient) throws IOException {
        SearchResponse esRsp = getAllSessionsRaw(esClientService, esclient);
        return Arrays.stream(esRsp.getHits().getHits())
                .map(hit -> {
                    Record record = new StandardRecord();
                    hit.getSourceAsMap().forEach((name, value) -> {
                        record.setField(name, FieldType.STRING, value);
                    });
                    return new WebSession(record, TestMappings.sessionInternalFields);
                })
                .collect(Collectors.toList());
    }

    public static Record getSessionFromEs(ElasticsearchClientService esClientService,
                                          RestHighLevelClient esclient, String sessionId,
                                          WebSession.InternalFields fields) throws IOException {
        esClientService.waitUntilCollectionIsReadyAndRefreshIfAnyPendingTasks(SESSION_INDEX_PREFIX + "*", 100000L);
        SearchRequest searchRequest = new SearchRequest(SESSION_INDEX_PREFIX + "*");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.idsQuery().addIds(sessionId));
        searchRequest.source(searchSourceBuilder);
        SearchResponse rsp = esclient.search(searchRequest, RequestOptions.DEFAULT);
        assert (rsp.getHits().getTotalHits().value <= 1);
        if (rsp.getHits().getTotalHits().value != 1) {
            try {
                Thread.sleep(2000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return getSessionFromEs(esClientService, esclient, sessionId, fields);
        }
        return WebSession.fromMap(rsp.getHits().getHits()[0].getSourceAsMap(), fields, "test").getRecord();
    }

    /**
     * Returns the name of the event index corresponding to the specified date such as
     * ${event-index-name}.${event-suffix}.
     * Eg. openanalytics-webevents.2018.01.31
     *
     * @param date the ZonedDateTime of the event to store in the index.
     * @return the name of the event index corresponding to the specified date.
     */
    public static String toEventIndexName(final ZonedDateTime date) {
        return Utils.buildIndexName(EVENT_INDEX_PREFIX, EVENT_SUFFIX_FORMATTER, date, date.getZone());
    }

    /**
     * Returns the name of the event index corresponding to the specified date such as
     * ${session-index-name}${session-suffix}.
     * Eg. openanalytics-webevents.2018.01.31
     *
     * @param date the ZonedDateTime timestamp of the first event of the session.
     * @return the name of the session index corresponding to the specified timestamp.
     */
    public static String toSessionIndexName(final ZonedDateTime date) {
        return Utils.buildIndexName(SESSION_INDEX_PREFIX, SESSION_SUFFIX_FORMATTER, date, date.getZone());
    }

    public static Map<String, Object> getEventFromEs(ElasticsearchClientService esClientService,
                                                     RestHighLevelClient esclient,
                                                     WebEvent event) throws IOException {
        String indexName = toEventIndexName(event.getZonedDateTime());
        esClientService.waitUntilCollectionIsReadyAndRefreshIfAnyPendingTasks(indexName, 100000L);
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.idsQuery().addIds(event.getId()));
        searchRequest.source(searchSourceBuilder);
        SearchResponse rsp = esclient.search(searchRequest, RequestOptions.DEFAULT);
        assert (rsp.getHits().getTotalHits().value <= 1);
        if (rsp.getHits().getTotalHits().value != 1) {
            try {
                Thread.sleep(2000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return getEventFromEs(esClientService, esclient, event);
        }
        return rsp.getHits().getHits()[0].getSourceAsMap();
    }
}
