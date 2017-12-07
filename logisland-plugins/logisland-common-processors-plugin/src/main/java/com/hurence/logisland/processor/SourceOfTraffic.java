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

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.elasticsearch.AbstractElasticsearchProcessor;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.service.cache.CacheService;
import com.hurence.logisland.service.elasticsearch.multiGet.InvalidMultiGetQueryRecordException;
import com.hurence.logisland.service.elasticsearch.multiGet.MultiGetQueryRecord;
import com.hurence.logisland.service.elasticsearch.multiGet.MultiGetQueryRecordBuilder;
import com.hurence.logisland.service.elasticsearch.multiGet.MultiGetResponseRecord;
import com.hurence.logisland.validator.StandardValidators;
import org.apache.commons.collections.map.HashedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

@Tags({"record", "traffic", "source"})
@CapabilityDescription("Compute the source of traffic of a user from a website visit\n" +
        "...")
public class SourceOfTraffic extends AbstractElasticsearchProcessor {


    private static final Logger logger = LoggerFactory.getLogger(SourceOfTraffic.class);
    private static final String SOURCE_OF_TRAFFIC_SUFFIX_NAME = "source_of_traffic";
    protected CacheService<String, CacheEntry> cacheService;
    static final String DEBUG_FROM_CACHE_SUFFIX = "_from_cache";
    protected static final String PROP_CACHE_SERVICE = "cache.service";
    protected static final String PROP_CACHE_VALIDITY_TIMEOUT = "cache.validity.timeout";
    protected static final String PROP_DEBUG = "debug";
    protected static final String DEFAULT_CACHE_VALIDITY_PERIOD = "0";
    private static final String SOCIAL_NETWORK_SITE = "social_network";
    private static final String SEARCH_ENGINE_SITE = "search_engine";
    private static final String REFERRING_SITE = "referral";
    private static final String DIRECT_TRAFFIC = "direct";
    protected boolean debug = false;

    public static final PropertyDescriptor ES_INDEX_FIELD = new PropertyDescriptor.Builder()
            .name("es.index")
            .description("The name of the ES index to use in multiget query. ")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ES_TYPE_FIELD = new PropertyDescriptor.Builder()
            .name("es.type")
            .description("The name of the ES type to use in multiget query.")
            .required(false)
            .defaultValue("default")
            .build();

    public static final PropertyDescriptor ES_SEARCH_ENGINE_FIELD = new PropertyDescriptor.Builder()
            .name("es.search_engine.field")
            .description("The name of the ES field used to specify that the host is a search engine.")
            .required(false)
            .defaultValue(SEARCH_ENGINE_SITE)
            .build();

    public static final PropertyDescriptor ES_SOCIAL_NETWORK_FIELD = new PropertyDescriptor.Builder()
            .name("es.social_network.field")
            .description("The name of the ES field used to specify that the host is a social network.")
            .required(false)
            .defaultValue(SOCIAL_NETWORK_SITE)
            .build();

    public static final PropertyDescriptor CONFIG_CACHE_SERVICE = new PropertyDescriptor.Builder()
            .name(PROP_CACHE_SERVICE)
            .description("The name of the cache service to use.")
            .required(true)
            .identifiesControllerService(CacheService.class)
            .build();

    public static final PropertyDescriptor CONFIG_CACHE_VALIDITY_TIMEOUT = new PropertyDescriptor.Builder()
            .name(PROP_CACHE_VALIDITY_TIMEOUT)
            .description("The name of the cache service to use.")
            .required(false)
            .defaultValue(DEFAULT_CACHE_VALIDITY_PERIOD)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();

    public static final PropertyDescriptor CONFIG_DEBUG = new PropertyDescriptor.Builder()
            .name(PROP_DEBUG)
            .description("If true, an additional debug field is added. If the source info fields prefix is X," +
                    " a debug field named X" + DEBUG_FROM_CACHE_SUFFIX + " contains a boolean value" +
                    " to indicate the origin of the source fields. The default value for this property is false (debug is disabled).")
            .required(false)
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    private static final PropertyDescriptor REFERER_FIELD = new PropertyDescriptor.Builder()
            .name("referer.field")
            .description("Name of the field containing the referer value in the record")
            .required(false)
            .defaultValue("referer")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final PropertyDescriptor UTM_SOURCE_FIELD = new PropertyDescriptor.Builder()
            .name("utm_source.field")
            .description("Name of the field containing the utm_source value in the record")
            .required(false)
            .defaultValue("utm_source")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final PropertyDescriptor UTM_CAMPAIGN_FIELD = new PropertyDescriptor.Builder()
            .name("utm_campaign.field")
            .description("Name of the field containing the utm_campaign value in the record")
            .required(false)
            .defaultValue("utm_campaign")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final PropertyDescriptor UTM_MEDIUM_FIELD = new PropertyDescriptor.Builder()
            .name("utm_medium.field")
            .description("Name of the field containing the utm_medium value in the record")
            .required(false)
            .defaultValue("utm_medium")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final PropertyDescriptor UTM_CONTENT_FIELD = new PropertyDescriptor.Builder()
            .name("utm_content.field")
            .description("Name of the field containing the utm_content value in the record")
            .required(false)
            .defaultValue("utm_content")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final PropertyDescriptor UTM_TERM_FIELD = new PropertyDescriptor.Builder()
            .name("utm_term.field")
            .description("Name of the field containing the utm_term value in the record")
            .required(false)
            .defaultValue("utm_term")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final PropertyDescriptor SOURCE_OF_TRAFFIC_SUFFIX_FIELD = new PropertyDescriptor.Builder()
            .name("source.out.field")
            .description("Name of the field containing the source of the traffic outcome")
            .required(false)
            .defaultValue(SOURCE_OF_TRAFFIC_SUFFIX_NAME)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    /**
     * The source identified for the web session
     */
    public static final String SOURCE_OF_TRAFIC_FIELD_SOURCE = "source";

    /**
     * The medium identified for the web session
     */
    public static final String SOURCE_OF_TRAFIC_FIELD_MEDIUM = "medium";

    /**
     * The campaign identified for the web session
     */
    public static final String SOURCE_OF_TRAFIC_FIELD_CAMPAIGN = "campaign";

    /**
     * The content identified for the web session
     */
    public static final String SOURCE_OF_TRAFIC_FIELD_CONTENT = "content";

    /**
     * The term/keyword identified for the web session
     */
    public static final String SOURCE_OF_TRAFIC_FIELD_KEYWORD = "keyword";

    /**
     * Wether source of trafic is an organic search or not for the web session
     */
    public static final String SOURCE_OF_TRAFIC_FIELD_ORGANIC_SEARCHES = "organic_search";

    /**
     * The referral path identified for the web session
     */
    public static final String SOURCE_OF_TRAFIC_FIELD_REFERRAL_PATH = "referral_path";

    // Supported field names. Key: source field name, Value: the field type to use
    static Map<String, FieldType> supportedSourceOfTrafficFieldNames = new HashMap<String, FieldType>() {{
        put(SOURCE_OF_TRAFIC_FIELD_SOURCE, FieldType.STRING);
        put(SOURCE_OF_TRAFIC_FIELD_MEDIUM, FieldType.STRING);
        put(SOURCE_OF_TRAFIC_FIELD_CAMPAIGN, FieldType.STRING);
        put(SOURCE_OF_TRAFIC_FIELD_CONTENT, FieldType.STRING);
        put(SOURCE_OF_TRAFIC_FIELD_KEYWORD, FieldType.STRING);
        put(SOURCE_OF_TRAFIC_FIELD_ORGANIC_SEARCHES, FieldType.BOOLEAN);
        put(SOURCE_OF_TRAFIC_FIELD_REFERRAL_PATH, FieldType.STRING);
    }};

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(REFERER_FIELD);
        descriptors.add(UTM_SOURCE_FIELD);
        descriptors.add(UTM_MEDIUM_FIELD);
        descriptors.add(UTM_CAMPAIGN_FIELD);
        descriptors.add(UTM_CONTENT_FIELD);
        descriptors.add(UTM_TERM_FIELD);
        descriptors.add(SOURCE_OF_TRAFFIC_SUFFIX_FIELD);
        descriptors.add(ELASTICSEARCH_CLIENT_SERVICE);
        descriptors.add(CONFIG_CACHE_SERVICE);
        descriptors.add(CONFIG_CACHE_VALIDITY_TIMEOUT);
        descriptors.add(CONFIG_DEBUG);
        descriptors.add(ES_INDEX_FIELD);
        descriptors.add(ES_TYPE_FIELD);
        descriptors.add(ES_SEARCH_ENGINE_FIELD);
        descriptors.add(ES_SOCIAL_NETWORK_FIELD);
        return Collections.unmodifiableList(descriptors);
    }

    @Override
    public void init(final ProcessContext context) {

        debug = context.getPropertyValue(CONFIG_DEBUG).asBoolean();

        /**
         * Get source of traffic service (aka Elasticsearch service) and the cache
         */
        super.init(context);

        cacheService = context.getPropertyValue(CONFIG_CACHE_SERVICE).asControllerService(CacheService.class);
        if(cacheService == null) {
            logger.error("Cache service is not initialized!");
        }
    }

    public void processSession(ProcessContext context, Record record)
    {
        final String utm_source_field         = context.getPropertyValue(UTM_SOURCE_FIELD).asString();
        final String utm_medium_field         = context.getPropertyValue(UTM_MEDIUM_FIELD).asString();
        final String utm_campaign_field       = context.getPropertyValue(UTM_CAMPAIGN_FIELD).asString();
        final String utm_content_field        = context.getPropertyValue(UTM_CONTENT_FIELD).asString();
        final String utm_term_field           = context.getPropertyValue(UTM_TERM_FIELD).asString();
        final String source_of_traffic_suffix = context.getPropertyValue(SOURCE_OF_TRAFFIC_SUFFIX_FIELD).asString();
        final String referer_field            = context.getPropertyValue(REFERER_FIELD).asString();

        SourceOfTrafficMap sourceOfTraffic = new SourceOfTrafficMap();
        // Check if this is a custom campaign
        if (record.getField(utm_source_field) != null){
            String utm_source = record.getField(utm_source_field).asString();
            sourceOfTraffic.setSource(utm_source);
            if (record.getField(utm_campaign_field) != null){
                String utm_campaign = record.getField(utm_campaign_field).asString();
                sourceOfTraffic.setCampaign(utm_campaign);
            }
            if (record.getField(utm_medium_field) != null){
                String utm_medium = record.getField(utm_medium_field).asString();
                sourceOfTraffic.setMedium(utm_medium);
            }
            if (record.getField(utm_content_field) != null){
                String utm_content = record.getField(utm_content_field).asString();
                sourceOfTraffic.setContent(utm_content);
            }
            if (record.getField(utm_term_field) != null){
                String utm_term = record.getField(utm_term_field).asString();
                sourceOfTraffic.setKeyword(utm_term);
            }
        }
        else if(record.getField(referer_field) != null){
            String referer = record.getField(referer_field).asString();
            URL referer_url = null;
            try {
                referer_url = new URL(referer);
            } catch (MalformedURLException e) {
                e.printStackTrace();
                return;
            }
            String hostname = referer_url.getHost();
            String[] hostname_splitted = hostname.split("\\.");
            String domain = null;
            if (hostname_splitted.length > 1){
                domain = hostname_splitted[hostname_splitted.length-2];
            }
            else if (hostname_splitted.length == 1) {
                domain = hostname_splitted[0];
            }
            else {
                return;
            }
            // Is the referer a known search engine ?
            if (is_search_engine(domain, context, record)){
                // This is an organic search engine
                sourceOfTraffic.setSource(domain);
                sourceOfTraffic.setMedium(SEARCH_ENGINE_SITE);
                sourceOfTraffic.setOrganic_searches(Boolean.TRUE);
            }
            else if (is_social_network(domain, context, record)){
                // This is social network
                sourceOfTraffic.setSource(domain);
                sourceOfTraffic.setMedium(SOCIAL_NETWORK_SITE);
            }
            // This is a referring site
            sourceOfTraffic.setSource(domain);
            sourceOfTraffic.setMedium(REFERRING_SITE);
        }
        else {
            // Direct access
            sourceOfTraffic.setSource(DIRECT_TRAFFIC);
            sourceOfTraffic.setMedium("");
            sourceOfTraffic.setCampaign(DIRECT_TRAFFIC);
        }
        record.setField(source_of_traffic_suffix, FieldType.MAP, sourceOfTraffic.getSourceOfTrafficMap());
    }


    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {
        for (Record record : records) {
            processSession(context, record);
        }
        return records;
    }

    /*
     * Returns true if the domain is a known social network website, false otherwise.
     * To figure out wether the domain is a known social network, we first lookup the domain in our local cache.
     * If the domain is present, then we pick up the info from here; otherwise we lookup the info from Elasticsearch.
     */
    private boolean is_social_network(String domain, ProcessContext context, Record record) {
        String es_social_network_field = context.getPropertyValue(ES_SOCIAL_NETWORK_FIELD).asString();
        return has_domain_flag(domain, es_social_network_field, context, record);
    }

    /*
     * Returns true if the domain is a known search engine, false otherwise.
     * To figure out wether the domain is a known search engine, we first lookup the domain in our local cache.
     * If the domain is present, then we pick up the info from here; otherwise we lookup the info from Elasticsearch.
     */
    private boolean is_search_engine(String domain, ProcessContext context, Record record) {
        String es_search_engine_field = context.getPropertyValue(ES_SEARCH_ENGINE_FIELD).asString();
        return has_domain_flag(domain, es_search_engine_field, context, record);
    }

    private boolean has_domain_flag(String domain, String flag, ProcessContext context, Record record){
        final String source_of_traffic_suffix = context.getPropertyValue(SOURCE_OF_TRAFFIC_SUFFIX_FIELD).asString();
        final long cacheValidityPeriodSec = context.getPropertyValue(CONFIG_CACHE_VALIDITY_TIMEOUT).asLong();
        boolean has_flag = false;
        /**
         * Attempt to find domain related info from the cache
         */
        SourceOfTraffic.CacheEntry cacheEntry = null;
        try {
            cacheEntry = cacheService.get(domain);
        } catch (Exception e) {
            logger.trace("Could not use cache!");
        }
        /**
         * If something in the cache, get it and be sure it is not obsolete
         */
        Map<String, Object> sourceInfo = null;
        boolean fromCache = true;
        if (cacheEntry != null) { // Something in the cache?
            sourceInfo = cacheEntry.getSourceInfo();
            if (cacheValidityPeriodSec > 0) { // Cache validity period enabled?
                long cacheTime = cacheEntry.getTime();
                long now = System.currentTimeMillis();
                long cacheAge = now - cacheTime;
                if (cacheAge > (cacheValidityPeriodSec * 1000L)) { // Cache entry older than allowed max age?
                    sourceInfo = null; // Cache entry expired, force triggering a new request
                }
            }
        }

        if (sourceInfo == null){
            fromCache = false;
            /**
             * Not in the cache or cache entry expired
             * Call the elasticsearch service and fill responses as new fields
             */
            String recordKeyName = domain;
            String indexName = context.getPropertyValue(ES_INDEX_FIELD).asString();
            String typeName = context.getPropertyValue(ES_TYPE_FIELD).asString();
            MultiGetQueryRecordBuilder mgqrBuilder = new MultiGetQueryRecordBuilder();
            mgqrBuilder.add(indexName, typeName, null, recordKeyName);
            List<MultiGetResponseRecord> multiGetResponseRecords = null;
            try {
                List<MultiGetQueryRecord> mgqrs = mgqrBuilder.build();

                multiGetResponseRecords = elasticsearchClientService.multiGet(mgqrs);
            } catch (InvalidMultiGetQueryRecordException e ){
                // should never happen
                e.printStackTrace();
            }

            if (multiGetResponseRecords == null || multiGetResponseRecords.isEmpty()) {
                // The domain is not known in the Elasticsearch special index
                // Therefore it is neither a search engine nor a social network
                sourceInfo = new HashMap();
                sourceInfo.put(REFERRING_SITE, true);
            }
            else {
                Optional<MultiGetResponseRecord> mgrr = multiGetResponseRecords.stream().findFirst();
                MultiGetResponseRecord mgrr_record;
                if (mgrr.isPresent()){
                    mgrr_record = mgrr.get();
                    sourceInfo = new HashedMap();
                    Map[] sourceInfoArray = { sourceInfo };
                    mgrr_record.getRetrievedFields().forEach((k, v) -> {
                        String fieldName = k.toString();
                        String fieldValue = v.toString();
                        // Initialize sourceInfo; i-e Map<String, Object>
                        sourceInfoArray[0].put(fieldName,fieldValue);
                            }
                    );
                }
            }

            try {
                // Store the sourceInfo into the cache
                cacheEntry = new CacheEntry(sourceInfo, System.currentTimeMillis());
                cacheService.set(domain, cacheEntry);
            } catch (Exception e) {
                logger.trace("Could not put entry in the cache:" + e.getMessage());
            }
        }

        if (sourceInfo != null) {
            if (sourceInfo.containsKey(flag)) {
                boolean val = Boolean.parseBoolean((String)sourceInfo.get(flag));
                has_flag = val;
            }
        }

        if (debug)
        {
            // Add some debug fields
            record.setField(source_of_traffic_suffix + DEBUG_FROM_CACHE_SUFFIX, FieldType.BOOLEAN, fromCache);
        }
        return has_flag;
    }

    /**
     * Cached entity for search engine and social network sites
     */
    private static class CacheEntry
    {
        // sourceInfo translated from the referer_hostname
        private Map<String, Object> sourceInfo = null;
        // Time at which this cache entry has been stored in the cache service
        private long time = 0L;

        public CacheEntry(Map<String, Object> sourceInfo, long time)
        {
            this.sourceInfo = sourceInfo;
            this.time = time;
        }

        public Map<String, Object> getSourceInfo()
        {
            return sourceInfo;
        }

        public long getTime()
        {
            return time;
        }
    }

    private static class SourceOfTrafficMap {
        // sourceInfo translated from the ip (or the ip if the sourceInfo could not be found)
        private Map<String, Object> sourceOfTrafficMap = new HashMap();

        public Map<String, Object> getSourceOfTrafficMap() {
            return sourceOfTrafficMap;
        }

        public void setMedium(String medium) {
            this.sourceOfTrafficMap.put(SOURCE_OF_TRAFIC_FIELD_MEDIUM, medium);
        }

        public void setCampaign(String campaign) {
            this.sourceOfTrafficMap.put(SOURCE_OF_TRAFIC_FIELD_CAMPAIGN, campaign);
        }

        public void setContent(String content) {
            this.sourceOfTrafficMap.put(SOURCE_OF_TRAFIC_FIELD_CONTENT, content);
        }

        public void setKeyword(String keyword) {
            this.sourceOfTrafficMap.put(SOURCE_OF_TRAFIC_FIELD_KEYWORD, keyword);
        }

        public void setOrganic_searches(Boolean organic_searches) {
            this.sourceOfTrafficMap.put(SOURCE_OF_TRAFIC_FIELD_ORGANIC_SEARCHES, organic_searches);
        }


        public void setReferral_path(String referral_path) {
            this.sourceOfTrafficMap.put(SOURCE_OF_TRAFIC_FIELD_REFERRAL_PATH, referral_path);
        }

        public void setSource(String source){
            this.sourceOfTrafficMap.put(SOURCE_OF_TRAFIC_FIELD_SOURCE, source);
        }

        public SourceOfTrafficMap(){
            setOrganic_searches(Boolean.FALSE); // By default we consider source of traffic to not be organic searches.
        }
    }

}

