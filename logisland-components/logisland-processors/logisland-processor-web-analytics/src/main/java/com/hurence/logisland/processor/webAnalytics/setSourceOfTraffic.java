/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.processor.webAnalytics;

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.classloading.PluginProxy;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.service.cache.CacheService;
import com.hurence.logisland.service.elasticsearch.ElasticsearchClientService;
import com.hurence.logisland.service.elasticsearch.multiGet.InvalidMultiGetQueryRecordException;
import com.hurence.logisland.service.elasticsearch.multiGet.MultiGetQueryRecord;
import com.hurence.logisland.service.elasticsearch.multiGet.MultiGetQueryRecordBuilder;
import com.hurence.logisland.service.elasticsearch.multiGet.MultiGetResponseRecord;
import com.hurence.logisland.validator.StandardValidators;
import org.apache.commons.collections.map.HashedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.*;

import static com.hurence.logisland.processor.webAnalytics.setSourceOfTraffic.*;

@Tags({"session", "traffic", "source", "web", "analytics"})
@CapabilityDescription("Compute the source of traffic of a web session. Users arrive at a website or application through a variety of sources, \n" +
        "including advertising/paying campaigns, search engines, social networks, referring sites or direct access. \n" +
        "When analysing user experience on a webshop, it is crucial to collects, processes, and reports the campaign and traffic-source data. \n" +
        "To compute the source of traffic of a web session, the user has to provide the utm_* related properties if available\n" +
        "i-e: **" + PROP_UTM_SOURCE + "**, **" + PROP_UTM_MEDIUM + "**, **" + PROP_UTM_CAMPAIGN + "**, **" + PROP_UTM_CONTENT + "**, **" + PROP_UTM_TERM + "**)\n" +
        ", the referer (**" + PROP_REFERER + "** property) and the first visited page of the session (**" + PROP_FIRST_VISITED_PAGE + "** property).\n" +
        "By default the source of traffic informations are placed in a flat structure (specified by the **" + PROP_SOURCE_OF_TRAFFIC_SUFFIX + "** property\n" +
        " with a default value of " + SOURCE_OF_TRAFFIC_SUFFIX_NAME + "_). To work properly the setSourceOfTraffic processor needs to have access to an \n" +
        "Elasticsearch index containing a list of the most popular search engines and social networks. The ES index (specified by the **" + PROP_ES_INDEX + "** property) " +
        "should be structured such that the _id of an ES document MUST be the name of the domain. If the domain is a search engine, the related ES doc MUST have a boolean field " +
        "(default being " + SEARCH_ENGINE_SITE + ") specified by the property **" + PROP_ES_SEARCH_ENGINE + "** with a value set to true. If the domain is a social network " +
        ", the related ES doc MUST have a boolean field (default being " + SOCIAL_NETWORK_SITE + ") specified by the property **" + PROP_ES_SOCIAL_NETWORK + "** with a value set to true. ")
public class setSourceOfTraffic extends AbstractProcessor {


    private static final Logger logger = LoggerFactory.getLogger(setSourceOfTraffic.class);
    protected static final String PROP_ES_INDEX = "es.index";
    private static final String PROP_ES_TYPE = "es.type";
    protected static final String PROP_ES_SEARCH_ENGINE = "es.search_engine.field";
    protected static final String PROP_ES_SOCIAL_NETWORK = "es.social_network.field";
    private static final String PROP_HIERARCHICAL = "source_of_traffic.hierarchical";
    protected static final String PROP_CACHE_SERVICE = "cache.service";
    protected static final String PROP_CACHE_VALIDITY_TIMEOUT = "cache.validity.timeout";
    protected static final String PROP_UTM_SOURCE = "utm_source.field";
    protected static final String PROP_UTM_CAMPAIGN = "utm_campaign.field";
    protected static final String PROP_UTM_MEDIUM = "utm_medium.field";
    protected static final String PROP_UTM_CONTENT = "utm_content.field";
    protected static final String PROP_UTM_TERM = "utm_term.field";
    protected static final String PROP_REFERER = "referer.field";
    protected static final String PROP_FIRST_VISITED_PAGE = "first.visited.page.field";
    protected static final String PROP_SOURCE_OF_TRAFFIC_SUFFIX = "source_of_traffic.suffix";
    protected static final String PROP_DEBUG = "debug";
    protected static final String DEFAULT_CACHE_VALIDITY_PERIOD = "0";
    protected static final String SOURCE_OF_TRAFFIC_SUFFIX_NAME = "source_of_traffic";
    protected static final String SOCIAL_NETWORK_SITE = "social_network";
    protected static final String SEARCH_ENGINE_SITE = "search_engine";
    private static final String REFERRING_SITE = "referral";
    private static final String DIRECT_TRAFFIC = "direct";
    protected CacheService<String, CacheEntry> cacheService;
    static final String DEBUG_FROM_CACHE_SUFFIX = "_from_cache";
    protected boolean debug = false;

    public static final PropertyDescriptor ELASTICSEARCH_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("elasticsearch.client.service")
            .description("The instance of the Controller Service to use for accessing Elasticsearch.")
            .required(true)
            .identifiesControllerService(ElasticsearchClientService.class)
            .build();


    public static final PropertyDescriptor ES_INDEX_FIELD = new PropertyDescriptor.Builder()
            .name(PROP_ES_INDEX)
            .description("Name of the ES index containing the list of search engines and social network. ")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ES_TYPE_FIELD = new PropertyDescriptor.Builder()
            .name(PROP_ES_TYPE)
            .description("Name of the ES type to use.")
            .required(false)
            .defaultValue("default")
            .build();

    public static final PropertyDescriptor ES_SEARCH_ENGINE_FIELD = new PropertyDescriptor.Builder()
            .name(PROP_ES_SEARCH_ENGINE)
            .description("Name of the ES field used to specify that the domain is a search engine.")
            .required(false)
            .defaultValue(SEARCH_ENGINE_SITE)
            .build();

    public static final PropertyDescriptor ES_SOCIAL_NETWORK_FIELD = new PropertyDescriptor.Builder()
            .name(PROP_ES_SOCIAL_NETWORK)
            .description("Name of the ES field used to specify that the domain is a social network.")
            .required(false)
            .defaultValue(SOCIAL_NETWORK_SITE)
            .build();

    public static final PropertyDescriptor CONFIG_CACHE_SERVICE = new PropertyDescriptor.Builder()
            .name(PROP_CACHE_SERVICE)
            .description("Name of the cache service to use.")
            .required(true)
            .identifiesControllerService(CacheService.class)
            .build();

    public static final PropertyDescriptor CONFIG_CACHE_VALIDITY_TIMEOUT = new PropertyDescriptor.Builder()
            .name(PROP_CACHE_VALIDITY_TIMEOUT)
            .description("Timeout validity (in seconds) of an entry in the cache.")
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
            .name(PROP_REFERER)
            .description("Name of the field containing the referer value in the session")
            .required(false)
            .defaultValue("referer")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final PropertyDescriptor FIRST_VISITED_PAGE_FIELD = new PropertyDescriptor.Builder()
            .name(PROP_FIRST_VISITED_PAGE)
            .description("Name of the field containing the first visited page in the session")
            .required(false)
            .defaultValue("firstVisitedPage")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final PropertyDescriptor UTM_SOURCE_FIELD = new PropertyDescriptor.Builder()
            .name(PROP_UTM_SOURCE)
            .description("Name of the field containing the utm_source value in the session")
            .required(false)
            .defaultValue("utm_source")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final PropertyDescriptor UTM_CAMPAIGN_FIELD = new PropertyDescriptor.Builder()
            .name(PROP_UTM_CAMPAIGN)
            .description("Name of the field containing the utm_campaign value in the session")
            .required(false)
            .defaultValue("utm_campaign")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final PropertyDescriptor UTM_MEDIUM_FIELD = new PropertyDescriptor.Builder()
            .name(PROP_UTM_MEDIUM)
            .description("Name of the field containing the utm_medium value in the session")
            .required(false)
            .defaultValue("utm_medium")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final PropertyDescriptor UTM_CONTENT_FIELD = new PropertyDescriptor.Builder()
            .name(PROP_UTM_CONTENT)
            .description("Name of the field containing the utm_content value in the session")
            .required(false)
            .defaultValue("utm_content")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final PropertyDescriptor UTM_TERM_FIELD = new PropertyDescriptor.Builder()
            .name(PROP_UTM_TERM)
            .description("Name of the field containing the utm_term value in the session")
            .required(false)
            .defaultValue("utm_term")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final PropertyDescriptor SOURCE_OF_TRAFFIC_SUFFIX_FIELD = new PropertyDescriptor.Builder()
            .name(PROP_SOURCE_OF_TRAFFIC_SUFFIX)
            .description("Suffix for the source of the traffic related fields")
            .required(false)
            .defaultValue(SOURCE_OF_TRAFFIC_SUFFIX_NAME)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor HIERARCHICAL = new PropertyDescriptor.Builder()
            .name(PROP_HIERARCHICAL)
            .description("Should the additional source of trafic information fields be added under a hierarchical father field or not.")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
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
        descriptors.add(FIRST_VISITED_PAGE_FIELD);
        descriptors.add(UTM_SOURCE_FIELD);
        descriptors.add(UTM_MEDIUM_FIELD);
        descriptors.add(UTM_CAMPAIGN_FIELD);
        descriptors.add(UTM_CONTENT_FIELD);
        descriptors.add(UTM_TERM_FIELD);
        descriptors.add(SOURCE_OF_TRAFFIC_SUFFIX_FIELD);
        descriptors.add(HIERARCHICAL);
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


    protected ElasticsearchClientService elasticsearchClientService;

    @Override
    public boolean hasControllerService() {
        return true;
    }


    @Override
    public void init(final ProcessContext context) {

        debug = context.getPropertyValue(CONFIG_DEBUG).asBoolean();
        Object o = PluginProxy.unwrap(context.getPropertyValue(ELASTICSEARCH_CLIENT_SERVICE).asControllerService());
        elasticsearchClientService = (ElasticsearchClientService) PluginProxy.create(o);
        if (elasticsearchClientService == null) {
            logger.error("Elasticsearch client service is not initialized!");
        }

        cacheService = PluginProxy.unwrap(context.getPropertyValue(CONFIG_CACHE_SERVICE).asControllerService());
        if (cacheService == null) {
            logger.error("Cache service is not initialized!");
        }
    }

    public void processSession(ProcessContext context, Record record) {
        final String utm_source_field = context.getPropertyValue(UTM_SOURCE_FIELD).asString();
        final String utm_medium_field = context.getPropertyValue(UTM_MEDIUM_FIELD).asString();
        final String utm_campaign_field = context.getPropertyValue(UTM_CAMPAIGN_FIELD).asString();
        final String utm_content_field = context.getPropertyValue(UTM_CONTENT_FIELD).asString();
        final String utm_term_field = context.getPropertyValue(UTM_TERM_FIELD).asString();
        final String SOURCE_OF_TRAFFIC_SUFFIX = context.getPropertyValue(SOURCE_OF_TRAFFIC_SUFFIX_FIELD).asString();
        final String FLAT_SEPARATOR = "_";
        final String referer_field = context.getPropertyValue(REFERER_FIELD).asString();
        final boolean hierarchical = context.getPropertyValue(HIERARCHICAL).asBoolean();

        SourceOfTrafficMap sourceOfTraffic = new SourceOfTrafficMap();
        // Check if this is a custom campaign
        if (record.getField(utm_source_field) != null) {
            String utm_source = null;
            try {
                utm_source = URLDecoder.decode(record.getField(utm_source_field).asString(), "UTF-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
                utm_source = record.getField(utm_source_field).asString();
            }
            sourceOfTraffic.setSource(utm_source);
            if (record.getField(utm_campaign_field) != null) {
                String utm_campaign = null;
                try {
                    utm_campaign = URLDecoder.decode(record.getField(utm_campaign_field).asString(), "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                    utm_campaign = record.getField(utm_campaign_field).asString();
                }
                sourceOfTraffic.setCampaign(utm_campaign);
            }
            if (record.getField(utm_medium_field) != null) {
                String utm_medium = null;
                try {
                    utm_medium = URLDecoder.decode(record.getField(utm_medium_field).asString(), "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                    utm_medium = record.getField(utm_medium_field).asString();
                }
                sourceOfTraffic.setMedium(utm_medium);
            }
            if (record.getField(utm_content_field) != null) {
                String utm_content = null;
                try {
                    utm_content = URLDecoder.decode(record.getField(utm_content_field).asString(), "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                    utm_content = record.getField(utm_content_field).asString();
                }
                sourceOfTraffic.setContent(utm_content);
            }
            if (record.getField(utm_term_field) != null) {
                String utm_term = null;
                try {
                    utm_term = URLDecoder.decode(record.getField(utm_term_field).asString(), "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                    utm_term = record.getField(utm_term_field).asString();
                }
                sourceOfTraffic.setKeyword(utm_term);
            }
        } else if ((record.getField(referer_field) != null) && (record.getField(referer_field).asString() != null)) {
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
            if (hostname_splitted.length > 1) {
                domain = hostname_splitted[hostname_splitted.length - 2];
            } else if (hostname_splitted.length == 1) {
                domain = hostname_splitted[0];
            } else {
                return;
            }
            // Is referer under the webshop domain ?
            if (is_refer_under_site_domain(domain, context, record)) {
                // This is a direct access
                sourceOfTraffic.setSource(DIRECT_TRAFFIC);
                sourceOfTraffic.setMedium("");
                sourceOfTraffic.setCampaign(DIRECT_TRAFFIC);
            } else {
                // Is the referer a known search engine ?
                if (is_search_engine(domain, context, record)) {
                    // This is an organic search engine
                    sourceOfTraffic.setSource(domain);
                    sourceOfTraffic.setMedium(SEARCH_ENGINE_SITE);
                    sourceOfTraffic.setOrganic_searches(Boolean.TRUE);
                } else if (is_social_network(domain, context, record)) {
                    // This is social network
                    sourceOfTraffic.setSource(domain);
                    sourceOfTraffic.setMedium(SOCIAL_NETWORK_SITE);
                } else {
                    // If the referer is not in the website domain, neither a search engine nor a social network,
                    // then it is a referring site
                    sourceOfTraffic.setSource(domain);
                    sourceOfTraffic.setMedium(REFERRING_SITE);
                    String referral_path = null;
                    try {
                        referral_path = URLDecoder.decode(referer, "UTF-8");
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                        referral_path = referer;
                    }
                    sourceOfTraffic.setReferral_path(referral_path);
                }
            }
        } else {
            // Direct access
            sourceOfTraffic.setSource(DIRECT_TRAFFIC);
            sourceOfTraffic.setMedium("");
            sourceOfTraffic.setCampaign(DIRECT_TRAFFIC);
        }
        if (hierarchical) {
            record.setField(SOURCE_OF_TRAFFIC_SUFFIX, FieldType.MAP, sourceOfTraffic.getSourceOfTrafficMap());
        } else {
            Map<String, Object> sot = sourceOfTraffic.getSourceOfTrafficMap();
            sot.forEach((k, v) -> {
                record.setField(SOURCE_OF_TRAFFIC_SUFFIX + FLAT_SEPARATOR + k, supportedSourceOfTrafficFieldNames.get(k), v);
            });
        }
    }

    private boolean is_refer_under_site_domain(String domain, ProcessContext context, Record record) {
        boolean res = false;
        String first_visited_page_field = context.getPropertyValue(FIRST_VISITED_PAGE_FIELD).asString();
        if (record.hasField(first_visited_page_field)) {
            String first_page = record.getField(first_visited_page_field).asString();
            URL first_page_url = null;
            try {
                first_page_url = new URL(first_page);
            } catch (MalformedURLException e) {
                e.printStackTrace();
                return res;
            }
            String host = first_page_url.getHost();
            String[] host_array = host.split("\\.");
            String first_visited_page_domain;
            if (host_array.length > 1) {
                first_visited_page_domain = host_array[host_array.length - 2];
            } else if (host_array.length == 1) {
                first_visited_page_domain = host_array[0];
            } else {
                return res;
            }
            if (domain.equals(first_visited_page_domain)) {
                res = true;
            } else {
                res = false;
            }
        }
        return res;
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

    private boolean has_domain_flag(String domain, String flag, ProcessContext context, Record record) {
        final String source_of_traffic_suffix = context.getPropertyValue(SOURCE_OF_TRAFFIC_SUFFIX_FIELD).asString();
        final long cacheValidityPeriodSec = context.getPropertyValue(CONFIG_CACHE_VALIDITY_TIMEOUT).asLong();
        boolean has_flag = false;
        /**
         * Attempt to find domain related info from the cache
         */
        setSourceOfTraffic.CacheEntry cacheEntry = null;
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

        if (sourceInfo == null) {
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
            } catch (InvalidMultiGetQueryRecordException e) {
                // should never happen
                e.printStackTrace();
            }

            if (multiGetResponseRecords == null || multiGetResponseRecords.isEmpty()) {
                // The domain is not known in the Elasticsearch special index
                // Therefore it is neither a search engine nor a social network
                sourceInfo = new HashMap();
                sourceInfo.put(REFERRING_SITE, true);
            } else {
                Optional<MultiGetResponseRecord> mgrr = multiGetResponseRecords.stream().findFirst();
                MultiGetResponseRecord mgrr_record;
                if (mgrr.isPresent()) {
                    mgrr_record = mgrr.get();
                    sourceInfo = new HashedMap();
                    Map[] sourceInfoArray = {sourceInfo};
                    if (mgrr_record.getRetrievedFields() != null) {
                        mgrr_record.getRetrievedFields().forEach((k, v) -> {
                                    String fieldName = k.toString();
                                    String fieldValue = v.toString();
                                    // Initialize sourceInfo; i-e Map<String, Object>
                                    sourceInfoArray[0].put(fieldName, fieldValue);
                                }
                        );
                    } else {
                        // The domain is not known in the Elasticsearch special index
                        // Therefore it is neither a search engine nor a social network
                        sourceInfo = new HashMap();
                        sourceInfo.put(REFERRING_SITE, true);
                    }
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
                boolean val = Boolean.parseBoolean((String) sourceInfo.get(flag));
                has_flag = val;
            }
        }

        if (debug) {
            // Add some debug fields
            record.setField(source_of_traffic_suffix + DEBUG_FROM_CACHE_SUFFIX, FieldType.BOOLEAN, fromCache);
        }
        return has_flag;
    }

    /**
     * Cached entity for search engine and social network sites
     */
    private static class CacheEntry {
        // sourceInfo translated from the referer_hostname
        private Map<String, Object> sourceInfo = null;
        // Time at which this cache entry has been stored in the cache service
        private long time = 0L;

        public CacheEntry(Map<String, Object> sourceInfo, long time) {
            this.sourceInfo = sourceInfo;
            this.time = time;
        }

        public Map<String, Object> getSourceInfo() {
            return sourceInfo;
        }

        public long getTime() {
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

        public void setSource(String source) {
            this.sourceOfTrafficMap.put(SOURCE_OF_TRAFIC_FIELD_SOURCE, source);
        }

        public SourceOfTrafficMap() {
            setOrganic_searches(Boolean.FALSE); // By default we consider source of traffic to not be organic searches.
        }
    }

}

