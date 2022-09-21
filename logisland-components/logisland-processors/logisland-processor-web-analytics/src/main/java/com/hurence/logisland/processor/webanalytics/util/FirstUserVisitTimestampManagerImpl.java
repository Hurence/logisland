package com.hurence.logisland.processor.webanalytics.util;

import com.hurence.logisland.processor.webanalytics.modele.FirstUserVisitCompositeKey;
import com.hurence.logisland.service.cache.CacheService;
import com.hurence.logisland.service.datastore.model.*;
import com.hurence.logisland.service.datastore.model.bool.BoolCondition;
import com.hurence.logisland.service.datastore.model.bool.BoolQueryRecord;
import com.hurence.logisland.service.datastore.model.bool.TermQueryRecord;
import com.hurence.logisland.service.elasticsearch.ElasticsearchClientService;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

/**
 * This class is responsible for handling the requests for first user visit timestamps.
 * The mappings are cached (they are immutable by nature). If a requested mapping is not present in the cache, it is
 * retrieved with an aggregation in Elasticsearch.
 */
public class FirstUserVisitTimestampManagerImpl implements FirstUserVisitTimestampManager {

    private final ElasticsearchClientService elasticsearchClientService;
    private final String esSessionIndexPrefix;
    private final CacheService<FirstUserVisitCompositeKey, Long> firstUserVisitCacheService;
    private final Map<String, String> additionalFieldMapForFirstUserVisitKey;

    public FirstUserVisitTimestampManagerImpl(
            @NotNull ElasticsearchClientService elasticsearchClientService,
            @NotNull String esSessionIndexPrefix,
            @NotNull CacheService<FirstUserVisitCompositeKey, Long> firstUserVisitCacheService,
            @NotNull Map<String, String> additionalFieldMapForFirstUserVisitKey) {
        this.elasticsearchClientService = elasticsearchClientService;
        this.esSessionIndexPrefix = esSessionIndexPrefix;
        this.firstUserVisitCacheService = firstUserVisitCacheService;
        this.additionalFieldMapForFirstUserVisitKey = additionalFieldMapForFirstUserVisitKey;
    }

    @Override
    public Long getFirstUserVisitTimestamp(FirstUserVisitCompositeKey key, Long candidateFirstUserVisitTimestamp) {
        Long cacheFirstUserVisitTimestamp = firstUserVisitCacheService.get(key);
        if (cacheFirstUserVisitTimestamp == null) { // nothing in the cache so we check in ES
            cacheFirstUserVisitTimestamp = this.getFirstUserVisitTimestampFromEs(key);
        }
        if (candidateFirstUserVisitTimestamp != null && (cacheFirstUserVisitTimestamp == null || cacheFirstUserVisitTimestamp > candidateFirstUserVisitTimestamp)) {
            firstUserVisitCacheService.set(key, candidateFirstUserVisitTimestamp);
            return candidateFirstUserVisitTimestamp;
        }
        if (cacheFirstUserVisitTimestamp != null) {
            firstUserVisitCacheService.set(key, cacheFirstUserVisitTimestamp);
        }
        return cacheFirstUserVisitTimestamp;
    }

    @Override
    public Map<String, String> getAdditionalFieldMapForFirstUserVisitKey() {
        return this.additionalFieldMapForFirstUserVisitKey;
    }

    private Long getFirstUserVisitTimestampFromEs(FirstUserVisitCompositeKey key) {
        AggregationRecord minAggregationRecord = new MinAggregationRecord("minAgg", "firstEventEpochSeconds");
        BoolQueryRecord userIdBoolQuery = new TermQueryRecord("userId.raw", key.getUserId());
        QueryRecord queryRecord = new QueryRecord()
                .addCollection(this.esSessionIndexPrefix + "*")
                .addAggregationQuery(minAggregationRecord)
                .addBoolQuery(userIdBoolQuery, BoolCondition.MUST)
                .size(0);
        for (Map.Entry<String, String> additionalFieldEntry: this.additionalFieldMapForFirstUserVisitKey.entrySet()) {
            queryRecord.addBoolQuery(new TermQueryRecord(additionalFieldEntry.getValue(), key.getAdditionalAttributeMap().get(additionalFieldEntry.getKey())), BoolCondition.MUST);
        }
        QueryResponseRecord queryResponseRecord = elasticsearchClientService.queryGet(queryRecord);
        List<AggregationResponseRecord> aggregationResponseRecords = queryResponseRecord.getAggregations();
        if (aggregationResponseRecords.size() != 1) {
            throw new IllegalStateException("Expected one aggregation, but found " + aggregationResponseRecords.size());
        }
        AggregationResponseRecord aggregationResponseRecord = aggregationResponseRecords.get(0);
        if (!(aggregationResponseRecord instanceof MinAggregationResponseRecord)) {
            throw new IllegalStateException("Expected the aggregation class to be " + MinAggregationResponseRecord.class.getName() + " but actual class is " + aggregationResponseRecord.getClass().getName());
        }
        return ((MinAggregationResponseRecord) aggregationResponseRecord).getMinimum();
    }
}
