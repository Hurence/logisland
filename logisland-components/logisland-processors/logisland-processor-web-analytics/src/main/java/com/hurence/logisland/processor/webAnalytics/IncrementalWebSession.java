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
package com.hurence.logisland.processor.webAnalytics;

import com.hurence.logisland.annotation.documentation.*;
import com.hurence.logisland.classloading.PluginProxy;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.ProcessException;
import com.hurence.logisland.processor.webAnalytics.modele.*;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.service.cache.CacheService;
import com.hurence.logisland.service.datastore.*;
import com.hurence.logisland.service.elasticsearch.ElasticsearchClientService;
import com.hurence.logisland.validator.StandardValidators;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hurence.logisland.processor.webAnalytics.util.Utils.isFieldAssigned;

@Category(ComponentCategory.ANALYTICS)
@Tags({"analytics", "web", "session"})
@CapabilityDescription(
value = "This processor creates and updates web-sessions based on incoming web-events." +
        " Note that both web-sessions and web-events are stored in elasticsearch.\n" +

        " Firstly, web-events are grouped by their session identifier and processed in chronological order.\n" +
        " Then each web-session associated to each group is retrieved from elasticsearch.\n" +
        " In case none exists yet then a new web session is created based on the first web event.\n" +
        " The following fields of the newly created web session are set based on the associated web event:" +
        " session identifier, first timestamp, first visited page." +

        " Secondly, once created, or retrieved, the web session is updated by the remaining web-events.\n" +
        " Updates have impacts on fields of the web session such as event counter, last visited page, " +
        " session duration, ...\n" +
        " Before updates are actually applied, checks are performed to detect rules that would trigger the creation" +
        " of a new session:\n\n" +
        "\tthe duration between the web session and the web event must not exceed the specified time-out,\n" +
        "\tthe web session and the web event must have timestamps within the same day (at midnight a new web session " +
        "is created),\n" +
        "\tsource of traffic (campaign, ...) must be the same on the web session and the web event.\n" +
        "\n" +
        " When a breaking rule is detected, a new web session is created with a new session identifier where as" +
        " remaining web-events still have the original session identifier. The new session identifier is the original" +
        " session suffixed with the character '#' followed with an incremented counter. This new session identifier" +
        " is also set on the remaining web-events.\n" +

        " Finally when all web events were applied, all web events -potentially modified with a new session" +
        " identifier- are save in elasticsearch. And web sessions are passed to the next processor.\n" +
        "\n" +
        "WebSession information are:\n" +
        "- first and last visited page\n" +
        "- first and last timestamp of processed event \n" +
        "- total number of processed events\n" +
        "- the userId\n" +
        "- a boolean denoting if the web-session is still active or not\n" +
        "- an integer denoting the duration of the web-sessions\n" +
        "- optional fields that may be retrieved from the processed events\n" +
        "\n"
)
@ExtraDetailFile("./details/IncrementalWebSession-Detail.rst")
public class IncrementalWebSession
       extends AbstractProcessor
{
    /**
     * The extra character added in case a missed new session is detected. In that case the original session identifier
     * is suffixes with that special character and the next session number.
     * Eg id-session, id-session#2, id-session#3, ...
     */
    public static final String EXTRA_SESSION_DELIMITER = "#";

    /**
     * The type of the output record.
     */
    public static final String OUTPUT_RECORD_TYPE = "consolidate-session";

    //Session Elasticsearch indices
    public static final String PROP_ES_SESSION_INDEX_PREFIX = "es.session.index.prefix";
    public static final String PROP_ES_SESSION_INDEX_SUFFIX_FORMATTER = "es.session.index.suffix.date";
    public static final String PROP_ES_SESSION_TYPE_NAME = "es.session.type.name";
    //Event Elasticsearch indices
    public static final String PROP_ES_EVENT_INDEX_PREFIX = "es.event.index.prefix";
    public static final String PROP_ES_EVENT_INDEX_SUFFIX_FORMATTER = "es.event.index.suffix.date";
    public static final String PROP_ES_EVENT_TYPE_NAME = "es.event.type.name";

    /**
     * Extra fields - for convenience - avoiding to parse the human readable first and last timestamps.
     */
    public static final String _FIRST_EVENT_EPOCH_SECONDS_FIELD = "firstEventEpochSeconds";
    public static final String _LAST_EVENT_EPOCH_SECONDS_FIELD = "lastEventEpochSeconds";

    public static final PropertyDescriptor ELASTICSEARCH_CLIENT_SERVICE_CONF =
            new PropertyDescriptor.Builder()
                    .name("elasticsearch.client.service")
                    .description("The instance of the Controller Service to use for accessing Elasticsearch.")
                    .required(true)
                    .identifiesControllerService(ElasticsearchClientService.class)
                    .build();

    public static final PropertyDescriptor DEBUG_CONF =
            new PropertyDescriptor.Builder()
                    .name("debug")
                    .description("Enable debug. If enabled, debug information are logged.")
                    .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
                    .required(false)
                    .defaultValue("false")
                    .build();

    public static final PropertyDescriptor ES_SESSION_INDEX_PREFIX_CONF =
            new PropertyDescriptor.Builder()
                    .name(PROP_ES_SESSION_INDEX_PREFIX)
                    .description("Prefix of the indices containing the web session documents.")
                    .required(true)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .build();

    public static final PropertyDescriptor ES_SESSION_INDEX_SUFFIX_FORMATTER_CONF =//TODO P3 date validator SimpleDateFormat
            new PropertyDescriptor.Builder()
                    .name(PROP_ES_SESSION_INDEX_SUFFIX_FORMATTER)
                    .description("suffix to add to prefix for web session indices. It should be valid date format [yyyy.MM].")
                    .required(true)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .build();

    public static final PropertyDescriptor ES_SESSION_TYPE_NAME_CONF =
            new PropertyDescriptor.Builder()
                    .name(PROP_ES_SESSION_TYPE_NAME)
                    .description("Name of the ES type of web session documents.")
                    .required(true)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .build();

    public static final PropertyDescriptor SESSION_INACTIVITY_TIMEOUT_CONF =
            new PropertyDescriptor.Builder()
                    .name("session.timeout")
                    .description("session timeout in sec")
                    .required(false)
                    .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
                    .defaultValue("1800")
                    .build();

    public static final PropertyDescriptor SESSION_ID_FIELD_CONF =
            new PropertyDescriptor.Builder()
                    .name("sessionid.field")
                    .description("the name of the field containing the session id => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("sessionId")
                    .build();

    public static final PropertyDescriptor ES_EVENT_INDEX_PREFIX_CONF =
            new PropertyDescriptor.Builder()
                    .name(PROP_ES_EVENT_INDEX_PREFIX)
                    .description("Prefix of the index containing the web event documents.")
                    .required(true)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .build();

    public static final PropertyDescriptor ES_EVENT_INDEX_SUFFIX_FORMATTER_CONF =//TODO P3 date validator DateTimeFormatter
            new PropertyDescriptor.Builder()
                    .name(PROP_ES_EVENT_INDEX_SUFFIX_FORMATTER)
                    .description("suffix to add to prefix for web event indices. It should be valid date format [yyyy.MM].")
                    .required(true)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .build();

    public static final PropertyDescriptor ES_EVENT_TYPE_NAME_CONF =
            new PropertyDescriptor.Builder()
                    .name(PROP_ES_EVENT_TYPE_NAME)
                    .description("Name of the ES type of web event documents.")
                    .required(true)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .build();

    public static final PropertyDescriptor TIMESTAMP_FIELD_CONF =
            new PropertyDescriptor.Builder()
                    .name("timestamp.field")
                    .description("the name of the field containing the timestamp => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("h2kTimestamp")
                    .build();

    public static final PropertyDescriptor VISITED_PAGE_FIELD =
            new PropertyDescriptor.Builder()
                    .name("visitedpage.field")
                    .description("the name of the field containing the visited page => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("location")
                    .build();

    public static final PropertyDescriptor USER_ID_FIELD =
            new PropertyDescriptor.Builder()
                    .name("userid.field")
                    .description("the name of the field containing the userId => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("userId")
                    .build();

    public static final PropertyDescriptor FIELDS_TO_RETURN =
            new PropertyDescriptor.Builder()
                    .name("fields.to.return")
                    .description("the list of fields to return")
                    .required(false)
                    .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
                    .build();

    public static final PropertyDescriptor FIRST_VISITED_PAGE_FIELD =
            new PropertyDescriptor.Builder()
                    .name("firstVisitedPage.out.field")
                    .description("the name of the field containing the first visited page => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("firstVisitedPage")
                    .build();

    public static final PropertyDescriptor LAST_VISITED_PAGE_FIELD =
            new PropertyDescriptor.Builder()
                    .name("lastVisitedPage.out.field")
                    .description("the name of the field containing the last visited page => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("lastVisitedPage")
                    .build();

    public static final PropertyDescriptor IS_SESSION_ACTIVE_FIELD =
            new PropertyDescriptor.Builder()
                    .name("isSessionActive.out.field")
                    .description("the name of the field stating whether the session is active or not => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("is_sessionActive")
                    .build();

    public static final PropertyDescriptor SESSION_DURATION_FIELD =
            new PropertyDescriptor.Builder()
                    .name("sessionDuration.out.field")
                    .description("the name of the field containing the session duration => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("sessionDuration")
                    .build();

    public static final PropertyDescriptor SESSION_INACTIVITY_DURATION_FIELD =
            new PropertyDescriptor.Builder()
                    .name("sessionInactivityDuration.out.field")
                    .description("the name of the field containing the session inactivity duration => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("sessionInactivityDuration")
                    .build();

    public static final PropertyDescriptor EVENTS_COUNTER_FIELD =
            new PropertyDescriptor.Builder()
                    .name("eventsCounter.out.field")
                    .description("the name of the field containing the session duration => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("eventsCounter")
                    .build();

    public static final PropertyDescriptor FIRST_EVENT_DATETIME_FIELD =
            new PropertyDescriptor.Builder()
                    .name("firstEventDateTime.out.field")
                    .description("the name of the field containing the date of the first event => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("firstEventDateTime")
                    .build();

    public static final PropertyDescriptor LAST_EVENT_DATETIME_FIELD =
            new PropertyDescriptor.Builder()
                    .name("lastEventDateTime.out.field")
                    .description("the name of the field containing the date of the last event => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("lastEventDateTime")
                    .build();

    public static final PropertyDescriptor NEW_SESSION_REASON_FIELD =
            new PropertyDescriptor.Builder()
                    .name("newSessionReason.out.field")
                    .description("the name of the field containing the reason why a new session was created => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("reasonForNewSession")
                    .build();

    public static final PropertyDescriptor TRANSACTION_IDS =
            new PropertyDescriptor.Builder()
                    .name("transactionIds.out.field")
                    .description("the name of the field containing all transactionIds => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("transactionIds")
                    .build();

    protected static final String PROP_CACHE_SERVICE = "cache.service";

    public static final PropertyDescriptor CONFIG_CACHE_SERVICE = new PropertyDescriptor.Builder()
            .name(PROP_CACHE_SERVICE)
            .description("The name of the cache service to use.")
            .required(true)
            .identifiesControllerService(CacheService.class)
            .build();
    /**
     * The source identified for the web session
     */
    public static final String SOURCE_OF_TRAFFIC_FIELD_SOURCE = "source";

    /**
     * The medium identified for the web session
     */
    public static final String SOURCE_OF_TRAFFIC_FIELD_MEDIUM = "medium";

    /**
     * The campaign identified for the web session
     */
    public static final String SOURCE_OF_TRAFFIC_FIELD_CAMPAIGN = "campaign";

    /**
     * The content identified for the web session
     */
    public static final String SOURCE_OF_TRAFFIC_FIELD_CONTENT = "content";

    /**
     * The term/keyword identified for the web session
     */
    public static final String SOURCE_OF_TRAFFIC_FIELD_KEYWORD = "keyword";

    protected static final String PROP_SOURCE_OF_TRAFFIC_PREFIX = "source_of_traffic.prefix";//TODO tell in doc that changed
    protected static final String SOURCE_OF_TRAFFIC_PREFIX_NAME = "source_of_traffic";
    public static final String DIRECT_TRAFFIC = "direct";

    public final String FLAT_SEPARATOR = "_";
    public static final PropertyDescriptor SOURCE_OF_TRAFFIC_PREFIX_FIELD =
            new PropertyDescriptor.Builder()
                 .name(PROP_SOURCE_OF_TRAFFIC_PREFIX)
                 .description("Prefix for the source of the traffic related fields")
                 .required(false)
                 .defaultValue(SOURCE_OF_TRAFFIC_PREFIX_NAME)
                 .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                 .build();

    /**
     * The properties of this processor.
     */
    private final static List<PropertyDescriptor> SUPPORTED_PROPERTY_DESCRIPTORS =
            Collections.unmodifiableList(Arrays.asList(
                    DEBUG_CONF,
                    ES_SESSION_INDEX_PREFIX_CONF,
                    ES_SESSION_INDEX_SUFFIX_FORMATTER_CONF,
                    ES_SESSION_TYPE_NAME_CONF,
                    ES_EVENT_INDEX_PREFIX_CONF,
                    ES_EVENT_INDEX_SUFFIX_FORMATTER_CONF,
                    ES_EVENT_TYPE_NAME_CONF,
                    SESSION_ID_FIELD_CONF,
                    TIMESTAMP_FIELD_CONF,
                    VISITED_PAGE_FIELD,
                    USER_ID_FIELD,
                    FIELDS_TO_RETURN,
                    FIRST_VISITED_PAGE_FIELD,
                    LAST_VISITED_PAGE_FIELD,
                    IS_SESSION_ACTIVE_FIELD,
                    SESSION_DURATION_FIELD,
                    SESSION_INACTIVITY_DURATION_FIELD,
                    SESSION_INACTIVITY_TIMEOUT_CONF,
                    EVENTS_COUNTER_FIELD,
                    FIRST_EVENT_DATETIME_FIELD,
                    LAST_EVENT_DATETIME_FIELD,
                    NEW_SESSION_REASON_FIELD,
                    TRANSACTION_IDS,
                    SOURCE_OF_TRAFFIC_PREFIX_FIELD,
                    // Service
                    ELASTICSEARCH_CLIENT_SERVICE_CONF,
                    CONFIG_CACHE_SERVICE
            ));

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return SUPPORTED_PROPERTY_DESCRIPTORS;
    }

    @Override
    public boolean hasControllerService()
    {
        return true;
    }

    private ElasticsearchClientService elasticsearchClientService;
    private CacheService<String/*sessionId*/, WebSession> cacheService;
    private long _SESSION_INACTIVITY_TIMEOUT_IN_SECONDS;
    private String _SESSION_ID_FIELD;
    private String _TIMESTAMP_FIELD;
    private Collection<String> _FIELDS_TO_RETURN;
    private String _IS_SESSION_ACTIVE_FIELD;
    private String _SESSION_DURATION_FIELD;
    private String _EVENTS_COUNTER_FIELD;
    private String _SOT_SOURCE_FIELD;
    private String _ES_SESSION_INDEX_PREFIX;
    private SimpleDateFormat _ES_SESSION_INDEX_SUFFIX_FORMATTER;
    private String _ES_SESSION_TYPE_NAME;
    private String _ES_EVENT_INDEX_PREFIX;
    private DateTimeFormatter _ES_EVENT_INDEX_SUFFIX_FORMATTER;
    private String _ES_EVENT_TYPE_NAME;
    private final long maxNumberOfEventForCurrentSessionRequested = 10000L;
    private Collection<SessionCheck> checkers;
    private Event.InternalFields eventsInternalFields;
    private WebSession.InternalFields sessionInternalFields;

    @Override
    public void init(final ProcessContext context) throws InitializationException
    {
        super.init(context);
        this.elasticsearchClientService = PluginProxy.rewrap(context.getPropertyValue(ELASTICSEARCH_CLIENT_SERVICE_CONF)
                .asControllerService());
        if (elasticsearchClientService == null)
        {
            getLogger().error("Elasticsearch client service is not initialized!");
        }
        cacheService = PluginProxy.rewrap(context.getPropertyValue(CONFIG_CACHE_SERVICE).asControllerService());
        if (cacheService == null) {
            getLogger().error("Cache service is not initialized!");
        }
        this._SESSION_INACTIVITY_TIMEOUT_IN_SECONDS = context.getPropertyValue(SESSION_INACTIVITY_TIMEOUT_CONF).asLong();
        this._SESSION_ID_FIELD = context.getPropertyValue(SESSION_ID_FIELD_CONF).asString();
        this._TIMESTAMP_FIELD = context.getPropertyValue(TIMESTAMP_FIELD_CONF).asString();
        final String _VISITED_PAGE_FIELD = context.getPropertyValue(VISITED_PAGE_FIELD).asString();

        final String fieldsToReturn = context.getPropertyValue(FIELDS_TO_RETURN).asString();
        if (fieldsToReturn != null && !fieldsToReturn.isEmpty()) {
            this._FIELDS_TO_RETURN = Arrays.asList(fieldsToReturn.split(","));
        } else {
            this._FIELDS_TO_RETURN = Collections.emptyList();
        }

        final String _USERID_FIELD = context.getPropertyValue(USER_ID_FIELD).asString();
        final String _FIRST_VISITED_PAGE_FIELD = context.getPropertyValue(FIRST_VISITED_PAGE_FIELD).asString();
        final String _LAST_VISITED_PAGE_FIELD = context.getPropertyValue(LAST_VISITED_PAGE_FIELD).asString();
        this._IS_SESSION_ACTIVE_FIELD = context.getPropertyValue(IS_SESSION_ACTIVE_FIELD).asString();
        this._SESSION_DURATION_FIELD = context.getPropertyValue(SESSION_DURATION_FIELD).asString();
        this._EVENTS_COUNTER_FIELD = context.getPropertyValue(EVENTS_COUNTER_FIELD).asString();
        final String _FIRST_EVENT_DATETIME_FIELD = context.getPropertyValue(FIRST_EVENT_DATETIME_FIELD).asString();
        final String _LAST_EVENT_DATETIME_FIELD = context.getPropertyValue(LAST_EVENT_DATETIME_FIELD).asString();
        String _SESSION_INACTIVITY_DURATION_FIELD = context.getPropertyValue(SESSION_INACTIVITY_DURATION_FIELD)
                .asString();
        final String _NEW_SESSION_REASON_FIELD = context.getPropertyValue(NEW_SESSION_REASON_FIELD).asString();
        final String _TRANSACTION_IDS = context.getPropertyValue(TRANSACTION_IDS).asString();

        final String sotPrefix = context.getPropertyValue(SOURCE_OF_TRAFFIC_PREFIX_FIELD).asString() + FLAT_SEPARATOR;

        final String _SOT_SOURCE_FIELD = sotPrefix + SOURCE_OF_TRAFFIC_FIELD_SOURCE;
        final String _SOT_CAMPAIGN_FIELD = sotPrefix + SOURCE_OF_TRAFFIC_FIELD_CAMPAIGN;
        final String _SOT_MEDIUM_FIELD = sotPrefix + SOURCE_OF_TRAFFIC_FIELD_MEDIUM;
        final String _SOT_CONTENT_FIELD = sotPrefix + SOURCE_OF_TRAFFIC_FIELD_CONTENT;
        final String _SOT_KEYWORD_FIELD = sotPrefix + SOURCE_OF_TRAFFIC_FIELD_KEYWORD;

        //Sessions indices
        this._ES_SESSION_INDEX_PREFIX = context.getPropertyValue(ES_SESSION_INDEX_PREFIX_CONF).asString();
        Objects.requireNonNull(this._ES_SESSION_INDEX_PREFIX, "Property required: " + ES_SESSION_INDEX_PREFIX_CONF);
        //TODO P3 try to use same way to convert dates... But it is the deployed way in a prod environment so
        // think this a lot before changing it
        this._ES_SESSION_INDEX_SUFFIX_FORMATTER = new java.text.SimpleDateFormat(
                context.getPropertyValue(ES_SESSION_INDEX_SUFFIX_FORMATTER_CONF).asString()
        );
        Objects.requireNonNull(this._ES_SESSION_INDEX_SUFFIX_FORMATTER, "Property required: " + ES_SESSION_INDEX_SUFFIX_FORMATTER_CONF);
        this._ES_SESSION_TYPE_NAME = context.getPropertyValue(ES_SESSION_TYPE_NAME_CONF).asString();
        Objects.requireNonNull(this._ES_SESSION_TYPE_NAME, "Property required: " + ES_SESSION_TYPE_NAME_CONF);
        //Events indices
        this._ES_EVENT_INDEX_PREFIX = context.getPropertyValue(ES_EVENT_INDEX_PREFIX_CONF).asString();
        Objects.requireNonNull(this._ES_EVENT_INDEX_PREFIX, "Property required: " + ES_EVENT_INDEX_PREFIX_CONF);
        //TODO P3 try to use same way to convert dates... But it is the deployed way in a prod environment so
        // think this a lot before changing it
        this._ES_EVENT_INDEX_SUFFIX_FORMATTER = DateTimeFormatter.ofPattern(
                context.getPropertyValue(ES_EVENT_INDEX_SUFFIX_FORMATTER_CONF).asString(),
                Locale.ENGLISH
        );
        Objects.requireNonNull(this._ES_EVENT_INDEX_SUFFIX_FORMATTER, "Property required: " + ES_EVENT_INDEX_SUFFIX_FORMATTER_CONF);
        this._ES_EVENT_TYPE_NAME = context.getPropertyValue(ES_EVENT_TYPE_NAME_CONF).asString();
        Objects.requireNonNull(this._ES_EVENT_TYPE_NAME, "Property required: " + ES_EVENT_TYPE_NAME_CONF);

        this.eventsInternalFields = new Event.InternalFields()
                .setSessionIdField(_SESSION_ID_FIELD)
                .setTimestampField(_TIMESTAMP_FIELD)
                .setVisitedPageField(_VISITED_PAGE_FIELD)
                .setSourceOffTrafficCampaignField(_SOT_CAMPAIGN_FIELD)
                .setSourceOffTrafficContentField(_SOT_CONTENT_FIELD)
                .setSourceOffTrafficKeyWordField(_SOT_KEYWORD_FIELD)
                .setSourceOffTrafficMediumField(_SOT_MEDIUM_FIELD)
                .setSourceOffTrafficSourceField(_SOT_SOURCE_FIELD)
                .setNewSessionReasonField(_NEW_SESSION_REASON_FIELD)
                .setUserIdField(_USERID_FIELD);

        this.sessionInternalFields = new WebSession.InternalFields()
                .setSessionIdField(_SESSION_ID_FIELD)
                .setTimestampField(_TIMESTAMP_FIELD)
                .setSourceOffTrafficCampaignField(_SOT_CAMPAIGN_FIELD)
                .setSourceOffTrafficContentField(_SOT_CONTENT_FIELD)
                .setSourceOffTrafficKeyWordField(_SOT_KEYWORD_FIELD)
                .setSourceOffTrafficMediumField(_SOT_MEDIUM_FIELD)
                .setSourceOffTrafficSourceField(_SOT_SOURCE_FIELD)
                .setIsSessionActiveField(_IS_SESSION_ACTIVE_FIELD)
                .setSessionDurationField(_SESSION_DURATION_FIELD)
                .setSessionInactivityDurationField(_SESSION_INACTIVITY_DURATION_FIELD)
                .setEventsCounterField(_EVENTS_COUNTER_FIELD)
                .setFirstEventDateTimeField(_FIRST_EVENT_DATETIME_FIELD)
                .setFirstEventEpochSecondsField(_FIRST_EVENT_EPOCH_SECONDS_FIELD)
                .setFirstVisitedPageField(_FIRST_VISITED_PAGE_FIELD)
                .setLastEventDateTimeField(_LAST_EVENT_DATETIME_FIELD)
                .setLastEventEpochSecondsField(_LAST_EVENT_EPOCH_SECONDS_FIELD)
                .setLastVisitedPageField(_LAST_VISITED_PAGE_FIELD)
                .setTransactionIdsField(_TRANSACTION_IDS)
                .setUserIdField(_USERID_FIELD);

        this.checkers = Arrays.asList(
                // Day overlap
                (session, event) ->
                {
                    final ZonedDateTime firstEvent = session.getFirstEvent();
                    final ZonedDateTime lastEvent = session.getLastEvent();

                    final ZonedDateTime timestamp = event.getTimestamp();

                    boolean isValid = firstEvent.getDayOfYear() == timestamp.getDayOfYear()
                            && lastEvent.getDayOfYear() == timestamp.getDayOfYear()
                            && firstEvent.getYear() == timestamp.getYear()
                            && lastEvent.getYear() == timestamp.getYear();

                    if (_DEBUG && !isValid) {
                        debug("'Day overlap' isValid=" + isValid + " session-id=" + session.getSessionId());
                    }

                    return isValid ? VALID : DAY_OVERLAP;
                },

                // Timeout exceeded
                (session, event) ->
                {
                    final long durationInSeconds = Duration.between(session.getLastEvent(), event.getTimestamp())
                            .getSeconds();
                    boolean isValid = durationInSeconds <= this._SESSION_INACTIVITY_TIMEOUT_IN_SECONDS;

                    if (_DEBUG && !isValid) {
                        debug("'Timeout exceeded' isValid=" + isValid + " seconds=" + durationInSeconds +
                                " timeout=" + this._SESSION_INACTIVITY_TIMEOUT_IN_SECONDS + " session-id=" + session.getSessionId());
                    }

                    return isValid ? VALID : SESSION_TIMEDOUT;
                },

                // One Campaign Per Session—Each visit to your site from a different campaign—organic or paid—triggers a
                // new session, regardless of the actual time elapsed in the current session.
                (session, event) ->
                {
                    boolean isValid = Objects.equals(event.getValue(_SOT_SOURCE_FIELD), DIRECT_TRAFFIC) ||
                            Objects.deepEquals(session.getSourceOfTraffic(), event.getSourceOfTraffic());

                    if (_DEBUG && !isValid) {
                        debug("'Fields of traffic' isValid=" + isValid + " session-id=" + session.getSessionId());
                    }

                    return isValid ? VALID : SOURCE_OF_TRAFFIC;
                });
    }

    /**
     * Processes the incoming records and returns their result.
     *
     * @param records the records to process.
     * @return the result of the processing of the incoming records.
     * @throws ProcessException if something went wrong.
     */
    @Override
    public Collection<Record> process(final ProcessContext context,
                                      final Collection<Record> records)
        throws ProcessException
    {
        return processRecords(records);
    }

    public Collection<Record> processRecords(final Collection<Record> records) {
        final Collection<Events> groupOfEvents = toWebEvents(records);
        final Collection<String> inputDivolteSessions = groupOfEvents.stream()
                .map(Events::getSessionId)
                .collect(Collectors.toList());
        final Map<String/*sessionId*/, Optional<WebSession>> lastSessionMapping = getMapping(inputDivolteSessions);

        //This method may update lastSessionMapping to current session (only the name will be used) when rewind is detected !
        SplittedEvents splittedEvents = handleRewindAndGetAllNeededEvents(groupOfEvents, lastSessionMapping);
        Collection<Events> allEvents = Stream.concat(
                splittedEvents.getEventsfromPast().stream(),
                splittedEvents.getEventsInNominalMode().stream()
        ).collect(Collectors.toList());

        final boolean isRewind = !splittedEvents.getEventsfromPast().isEmpty();
        final Collection<SessionsCalculator> calculatedSessions;
        if (isRewind) {
            Set<String> sessionsInRewind = splittedEvents.getEventsfromPast().stream()
                    .map(Events::getSessionId)
                    .collect(Collectors.toSet());
            calculatedSessions = this.processEvents(allEvents, lastSessionMapping, sessionsInRewind);
        } else {
            calculatedSessions = this.processEvents(allEvents, lastSessionMapping);
        }
        saveEventsToEs(allEvents);
        //update cache
        calculatedSessions
                .forEach(sessionsCalculator -> {
                    String divolteSession = sessionsCalculator.getDivolteSessionId();
                    WebSession lastSession = sessionsCalculator.getCalculatedSessions().stream()
                            .filter(session -> session.getSessionId().equals(sessionsCalculator.getLastSessionId()))
                            .findFirst().get();
                    cacheService.set(divolteSession, lastSession);
                });

        final Collection<WebSession> flattenedSessions = calculatedSessions.stream()
                .flatMap(sessionsCalculator -> sessionsCalculator.getCalculatedSessions().stream())
                .collect(Collectors.toList());
        debug("Processing done. Outcoming records size=%d ", flattenedSessions.size());

        return flattenedSessions
                .stream().map(WebSession::getRecord)
                .collect(Collectors.toList());
    }


    private void saveEventsToEs(Collection<Events> allEvents) {
        allEvents.stream()
                .flatMap(Collection::stream)
                .forEach(event ->
                {
                    final Map<String, Object> map = toMap(event.cloneRecord());

                    elasticsearchClientService.bulkPut(toEventIndexName(event.getTimestamp()),
                            _ES_EVENT_TYPE_NAME,
                            map,
                            Optional.of((String) map.get(FieldDictionary.RECORD_ID)));
                });
        elasticsearchClientService.bulkFlush();
    }

    private SplittedEvents handleRewindAndGetAllNeededEvents(final Collection<Events> groupOfEvents,
                                                             final Map<String/*sessionId*/, Optional<WebSession>> lastSessionMapping) {

        final SplittedEvents splittedEvents = getSplittedEvents(groupOfEvents, lastSessionMapping);
        final Collection<Events> eventsFromPast = splittedEvents.getEventsfromPast();
        if (eventsFromPast.isEmpty()) {
            return splittedEvents;
        }
        deleteFuturSessions(eventsFromPast);
        Collection<Event> eventsFromEs = getNeededEventsFromEs(eventsFromPast, lastSessionMapping);
        Map<String/*sessionId*/, List<Event>> eventsFromEsBySessionId = eventsFromEs
                .stream()
                .collect(Collectors.groupingBy(Event::getSessionId));
        //merge those events into the lists
        for (Events events : eventsFromPast) {
            List<Event> eventsFromEsForSession = eventsFromEsBySessionId.getOrDefault(events.getSessionId(), Collections.emptyList());
            events.addAll(eventsFromEsForSession);//TODO P2 faire un test qui verifie que les events deja dans events sont prioritaire
        }
        return splittedEvents;
    }

    /**
     * For all Events, find current session of the first event (from ES).
     * Then Find all events from this session that are before or at the same time than the first event in input.
     * Return those events.
     * @param eventsFromPast
     * @return
     */
    private Collection<Event> getNeededEventsFromEs(final Collection<Events> eventsFromPast,
                                                    final Map<String/*sessionId*/, Optional<WebSession>> lastSessionMapping) {

        /*
            Pour chaque events trouver les evènements de la session en cour nécessaire.
            C'est de requêter tous les events de la sessionId et timestamp <= firstEventTs(input events)
        */
        //TODO P2 look when do we need to refresh some indices
//        String[] indicesToRefresh = null;
//        elasticsearchClientService.waitUntilCollectionIsReadyAndRefreshIfAnyPendingTasks(indicesToRefresh, 60000L);
        //TODO P2 END
        Map<String, ZonedDateTime> divoltSessionToFirstEvent = eventsFromPast.stream()
                .collect(Collectors.toMap(
                        Events::getSessionId,
                        events -> events.first().getTimestamp()
                ));
        Map<String/*divolteSession*/, WebSession> currentSessionsOfEvents = requestCurrentSessionsToEs(eventsFromPast);
        uptadateLastSessionsMapping(lastSessionMapping, currentSessionsOfEvents);
        resetCacheWithSessions(currentSessionsOfEvents);
        Collection<Event> neededEventsFromEs = requestEventsFromSessionsToEs(
                currentSessionsOfEvents.values(),
                divoltSessionToFirstEvent
        );
        return neededEventsFromEs;
    }

    private void uptadateLastSessionsMapping(Map<String, Optional<WebSession>> lastSessionMapping,
                                             Map<String, WebSession> currentSessionsOfEvents) {
        currentSessionsOfEvents.forEach((divolteSessions, session) -> lastSessionMapping.put(divolteSessions, Optional.of(session)));
    }

    private void resetCacheWithSessions(Map<String, WebSession> currentSessionsOfEvents) {
        currentSessionsOfEvents.forEach((divolteSessions, session) -> this.cacheService.set(divolteSessions, session));
    }


    private Collection<Event> requestEventsFromSessionsToEs(Collection<WebSession> currentSessionsOfEvents,
                                                            Map<String, ZonedDateTime> divoltSessionToFirstEvent) {
        MultiQueryResponseRecord eventsRsp = getMissingEventsForSessionsFromEs(currentSessionsOfEvents, divoltSessionToFirstEvent);
        return convertEsRToEvents(eventsRsp);
    }


    private Collection<Event> convertEsRToEvents(MultiQueryResponseRecord eventsRsp) {
        final List<Event> events = new ArrayList<>();
        eventsRsp.getResponses().forEach(rsp -> {
            if (rsp.getTotalMatched() > maxNumberOfEventForCurrentSessionRequested) {
                Event firstEvent = esDoc2Event(rsp.getDocs().get(0).getRetrievedFields());
                String errorMsg = "A query to search events for current session exceeds " + maxNumberOfEventForCurrentSessionRequested +
                        " events ! either increases maximum expected either verify if this sessions '" +
                        firstEvent.getSessionId() +"' has really this much of events !";
                getLogger().error(errorMsg);
                throw new IllegalStateException(errorMsg);
            }
            rsp.getDocs().stream().forEach(rspRecord -> {
                events.add(esDoc2Event(rspRecord.getRetrievedFields()));
            });
        });
        return events;
    }

/*
    GET new_openanalytics_webevents.2020.10/_search
    {
        "query": {
        "bool": {
            "must": [
            {
                "term": {
                "sessionId.raw": {
                    "value": "0:kfdxb7hf:U4e3OplHDO8Hda8yIS3O2iCdBOcVE_al#4"
                }
            }
            },
            {
                "range": {
                "h2kTimestamp": {
                    "lte": Tmin
                }
            }
            }
          ]
        }
    },
        "sort": [
        {
            "h2kTimestamp": {
            "order": "desc"
        }
        }
      ],
        "size": 10000
    }
*/
    private MultiQueryResponseRecord getMissingEventsForSessionsFromEs(Collection<WebSession> currentSessionsOfEvents,
                                                                       Map<String, ZonedDateTime> divoltSessionToFirstEvent) {
        final List<QueryRecord> queries = new ArrayList<>();
        currentSessionsOfEvents.forEach(currentSession -> {
            ZonedDateTime firstEventTimeStamp = divoltSessionToFirstEvent.get(currentSession.getOriginalSessionId());
            String indexName = toEventIndexName(firstEventTimeStamp);
            QueryRecord query = new QueryRecord()
                    .addCollection(indexName)
                    .addType(_ES_EVENT_TYPE_NAME)
                    .addTermQuery(
                            new TermQueryRecord(_SESSION_ID_FIELD + ".raw", currentSession.getSessionId())
                    ).addRangeQuery(
                            new RangeQueryRecord(_TIMESTAMP_FIELD)
                                    .setTo(firstEventTimeStamp.toInstant().toEpochMilli())
    //TODO P2 +1000 car dans session / 1000...? Mais je crois que c'est un timestamp qui vient dun event donc non c okay ?
                                    .setIncludeUpper(true)
                    ).size(10000);
            queries.add(query);
        });
        MultiQueryRecord multiQuery = new MultiQueryRecord(queries);
        String[] indicesToWaitFor = currentSessionsOfEvents.stream()
                .map(currentSession -> {
                    ZonedDateTime firstEventTimeStamp = divoltSessionToFirstEvent.get(currentSession.getOriginalSessionId());
                    return toEventIndexName(firstEventTimeStamp);
                })
                .toArray(String[]::new);
        this.elasticsearchClientService.waitUntilCollectionIsReadyAndRefreshIfAnyPendingTasks(indicesToWaitFor, 100000L);
        return elasticsearchClientService.multiQueryGet(multiQuery);
    }

    private Map<String, WebSession> requestCurrentSessionsToEs(Collection<Events> eventsFromPast) {
        MultiQueryResponseRecord sessionEsRsp = requestSessions(eventsFromPast);
        return checkAndTransformToMap(sessionEsRsp);
    }

    /**
     * ensure there is only one response by query and return as map with divolte session as key.
     * @param sessionEsRsp
     * @return
     */
    private Map<String, WebSession> checkAndTransformToMap(MultiQueryResponseRecord sessionEsRsp) {
        final Map<String, WebSession> divoltSessionToCurrentSessions = new HashMap<>();
        sessionEsRsp.getResponses().forEach(rsp -> {
            if (rsp.getTotalMatched() != 1) {
                String errorMsg = "A query to search for current session does not matched exactly one document but matched " + rsp.getTotalMatched() + " documents. That should never happen ! Exiting";
                getLogger().error(errorMsg);
                throw new IllegalStateException(errorMsg);
            }
            WebSession currentSession = esDoc2WebSession(rsp.getDocs().get(0).getRetrievedFields());
            divoltSessionToCurrentSessions.put(currentSession.getOriginalSessionId(), currentSession);
        });
        return divoltSessionToCurrentSessions;
    }

    private MultiQueryResponseRecord requestSessions(Collection<Events> eventsFromPast) {
        final List<QueryRecord> queries = new ArrayList<>();
        eventsFromPast.stream().forEach(events -> {
            String divolteSession = events.getSessionId();
            long epochSecondFirstEvent = events.first().getEpochTimeStampSeconds();
            QueryRecord query = new QueryRecord()
                    .addCollection(_ES_SESSION_INDEX_PREFIX + "*")
                    .addType(_ES_SESSION_TYPE_NAME)
                    .addWildCardQuery(
                            new WildCardQueryRecord(_SESSION_ID_FIELD + ".raw", divolteSession + "*")
                    ).addRangeQuery(
                            new RangeQueryRecord(_FIRST_EVENT_EPOCH_SECONDS_FIELD)
                                    .setTo(epochSecondFirstEvent)
                                    .setIncludeUpper(true)
                    ).addRangeQuery(
                            new RangeQueryRecord(_LAST_EVENT_EPOCH_SECONDS_FIELD)
                                    .setFrom(epochSecondFirstEvent)
                                    .setIncludeLower(true)
                    ).size(1);
            queries.add(query);
        });
        MultiQueryRecord multiQuery = new MultiQueryRecord(queries);
        MultiQueryResponseRecord rsp = elasticsearchClientService.multiQueryGet(multiQuery);
        return rsp;
    }

//    GET new_openanalytics_websessions-*/_search
//{
//    "query": {
//      "bool": {
//          "must": [
//          {
//              "wildcard": {
//              "sessionId.raw": {
//                  "value": "0:kfdxb7hf:U4e3OplHDO8Hda8yIS3O2iCdBOcVE_al*"
//              }
//          }
//          },
//          {
//              "range": {
//              "h2kTimestamp": {
//                  "gt": 1601448439663
//              }
//          }
//          }
//        ]
//      }
//    }
//}
    private void deleteFuturSessions(Collection<Events> eventsFromPast) {
        /*
            Pour chaque events trouver le min, effacer toutes les sessions avec
            originalSessionId = sessionId && firstEventTs(session) > firstEventTs(input events)
           ==> du coup on a plus que les sessions plus ancienne ou la session actuelle dans es.
        */
        //We could use a deleteByQuery here, but in 2.4 this is a plugin and may not be available.
        // Another solution is to use the Bulk api with delete query using id of documents.
        // We could add a method in ElasticSearchCLient interface isSupportingDeleteByQuery() to use it when available.
        //TODO bugged ! should use a  (session1 & t1 >) || (session2 & t2 >) || ... || (session & tn >)
        QueryRecord queryRecord = new QueryRecord();
        queryRecord.setRefresh(false);
        for (Events events : eventsFromPast) {
            Event firstEvent = events.first();
            final String sessionIndexName = toSessionIndexName(firstEvent.getEpochTimeStampMilli());
            final String divolteSession = events.getSessionId();//divolt session
            queryRecord
                    .addCollection(sessionIndexName)
                    .addType(_ES_SESSION_TYPE_NAME)
                    .addWildCardQuery(new WildCardQueryRecord(_SESSION_ID_FIELD + ".raw", divolteSession + "*"))
                    .addRangeQuery(
                            new RangeQueryRecord(_TIMESTAMP_FIELD)
                                    .setFrom(firstEvent.getEpochTimeStampMilli())
                                    .setIncludeLower(false)
                    );
        }
        elasticsearchClientService.deleteByQuery(queryRecord);
    }

    private SplittedEvents getSplittedEvents(Collection<Events> groupOfEvents,
                                             Map<String/*sessionId*/, Optional<WebSession>> lastSessionMapping) {
        final Collection<Events> eventsFromPast = new ArrayList<>();
        final Collection<Events> eventsOk = new ArrayList<>();
        for (Events events : groupOfEvents) {
            Optional<WebSession> lastSession = lastSessionMapping.get(events.getSessionId());
            if (lastSession.isPresent() &&
                    lastSession.get().timestampFromPast(events.first().getTimestamp())) {
//                    !lastSession.get().containsTimestamp(events.first().getTimestamp()) &&
//                    lastSession.get().getLastEvent().toInstant().toEpochMilli() > events.first().getEpochTimeStampMilli()) {//> ou >= ?
                eventsFromPast.add(events);
            } else {
                eventsOk.add(events);
            }
        }
        return new SplittedEvents(eventsOk, eventsFromPast);
    }

    /**
     * Filter out record without sessionId or timestamp
     * Returns the provided remaining records as a collection of Events instances.
     *
     * Provided records are grouped by session identifier so that each Events contains all events from a specific sessionId.
     *
     * @param records a collection of records representing web-events.
     * @return the provided records as a collection of Events instances.
     */
    private Collection<Events> toWebEvents(final Collection<Record> records) {
        // Create webEvents from input records.
        // A webEvents contains all webEvent instances of its session id.
        final Collection<Events> result =
                records.stream()
                        // Remove record without session Id or timestamp.
                        .filter(record -> isFieldAssigned(record.getField(_SESSION_ID_FIELD))
                                && isFieldAssigned(record.getField(_TIMESTAMP_FIELD)))
                        // Create web-event from record.
                        .map(record -> new Event(record, this.eventsInternalFields))
                        // Group records per session Id.
                        .collect(Collectors.groupingBy(Event::getSessionId))
                        // Ignore keys (sessionId) and stream over list of associated events.
                        .values()
                        .stream()
                        // Wrapped grouped web-events of sessionId in WebEvents.
                        .map(Events::new)
                        .collect(Collectors.toList());

        return result;
    }

    /**
     * Returns the name of the event index corresponding to the specified date such as
     * ${event-index-name}.${event-suffix}.
     * Eg. openanalytics-webevents.2018.01.31
     *
     * @param date the timestamp of the event to store in the index.
     * @return the name of the event index corresponding to the specified date.
     */
    private String toEventIndexName(final ZonedDateTime date) {
        return _ES_EVENT_INDEX_PREFIX + _ES_EVENT_INDEX_SUFFIX_FORMATTER.format(date);
    }

    /**
     * Returns the name of the event index corresponding to the specified date such as
     * ${session-index-name}${session-suffix}.
     * Eg. openanalytics-webevents.2018.01.31
     *
     * @param epochMilli the milli timestamp epoc of the event of the session.
     * @return the name of the session index corresponding to the specified timestamp.
     */
    private String toSessionIndexName(long epochMilli) {
        Date date = new java.util.Date(epochMilli);
        return _ES_SESSION_INDEX_PREFIX + _ES_SESSION_INDEX_SUFFIX_FORMATTER.format(date);
    }

    /**
     * Processes the provided events and returns their resulting sessions.
     * All sessions are retrieved from the cache or elasticsearch and then updated with the specified web events.
     * One serie of events may result into multiple sessions.
     *
     * @param webEvents the web events from a same session to process.
     * @return web sessions resulting of the processing of the web events.
     */
    private Collection<SessionsCalculator> processEvents(final Collection<Events> webEvents,
                                                         final Map<String, Optional<WebSession>> lastSessionMapping,
                                                         final Set<String> sessionsInRewind) {
        // Applies all events to session documents and collect results.
        return webEvents.stream()
                .map(events -> {
                    String divolteSession = events.getSessionId();
                    if (lastSessionMapping.get(divolteSession).isPresent()) {
                        boolean isRewind = sessionsInRewind.contains(divolteSession);
                        return new SessionsCalculator(checkers,
                                _SESSION_INACTIVITY_TIMEOUT_IN_SECONDS,
                                sessionInternalFields,
                                eventsInternalFields,
                                _FIELDS_TO_RETURN,
                                lastSessionMapping.get(events.getSessionId()).get()).processEvents(events, isRewind);
                    } else {
                        return new SessionsCalculator(checkers,
                                _SESSION_INACTIVITY_TIMEOUT_IN_SECONDS,
                                sessionInternalFields,
                                eventsInternalFields,
                                _FIELDS_TO_RETURN,
                                divolteSession).processEvents(events, false);
                    }
                })
                .collect(Collectors.toList());
    }

    /**
     * Processes the provided events and returns their resulting sessions.
     * All sessions are retrieved from the cache or elasticsearch and then updated with the specified web events.
     * One serie of events may result into multiple sessions.
     *
     * @param webEvents the web events from a same session to process.
     * @return web sessions resulting of the processing of the web events.
     */
    private Collection<SessionsCalculator> processEvents(final Collection<Events> webEvents,
                                                         final Map<String, Optional<WebSession>> lastSessionMapping) {
        // Applies all events to session documents and collect results.
        return webEvents.stream()
                .map(events -> {
                    String divolteSession = events.getSessionId();
                    if (lastSessionMapping.get(divolteSession).isPresent()) {
                        return new SessionsCalculator(checkers,
                                _SESSION_INACTIVITY_TIMEOUT_IN_SECONDS,
                                sessionInternalFields,
                                eventsInternalFields,
                                _FIELDS_TO_RETURN,
                                lastSessionMapping.get(events.getSessionId()).get()).processEvents(events, false);
                    } else {
                        return new SessionsCalculator(checkers,
                                _SESSION_INACTIVITY_TIMEOUT_IN_SECONDS,
                                sessionInternalFields,
                                eventsInternalFields,
                                _FIELDS_TO_RETURN,
                                divolteSession).processEvents(events, false);
                    }
                })
                .collect(Collectors.toList());
    }

//    GET new_openanalytics_websessions-*/_search
//{
//    "query": {
//    "wildcard": {
//        "sessionId.raw": {
//            "value": "<divolte_session>*"
//        }
//    }
//},
//    "sort": [
//    {
//        "h2kTimestamp": {
//        "order": "desc"
//    }
//    }
//  ],
//    "size": 1
//}

    /**
     * query all last hurence session for each divolte session in ES if it is not already present in cache.
     * THen fill up cache with new data, finally return the <code>mapping[divolte_session,last_hurence_session]</code>
     * @param divolteSessions
     * @return
     */
    //TODO P3 (can we avoid two query of this method ?)
    private Map<String/*divolteSession*/, Optional<WebSession>/*lastHurenceSession*/> getMapping(final Collection<String> divolteSessions) {
        final Map<String, Optional<WebSession>> mappingToReturn = new HashMap<>();
        final List<QueryRecord> sessionsRequests = new ArrayList<>();
        divolteSessions.forEach(divoltSession -> {
            WebSession cachedSession = cacheService.get(divoltSession);
            if (cachedSession != null) {
                mappingToReturn.put(divoltSession, Optional.of(cachedSession));
            } else {
                QueryRecord request = new QueryRecord()
                        .addCollection(_ES_SESSION_INDEX_PREFIX + "*")//TODO P2 put this on root, I think we should create a MultiQueryRecord instead...
                        .addType(_ES_SESSION_TYPE_NAME)
                        .addWildCardQuery(new WildCardQueryRecord(_SESSION_ID_FIELD + ".raw", divoltSession + "*"))
                        .addSortQuery(new SortQueryRecord(_TIMESTAMP_FIELD, SortOrder.DESC))
                        .size(1);//only need the last mapping
                sessionsRequests.add(request);
            }
        });
        if (sessionsRequests.isEmpty()) return mappingToReturn;

        this.elasticsearchClientService.waitUntilCollectionIsReadyAndRefreshIfAnyPendingTasks(_ES_SESSION_INDEX_PREFIX + "*", 100000L);
        MultiQueryResponseRecord multiQueryResponses = this.elasticsearchClientService.multiQueryGet(
                new MultiQueryRecord(sessionsRequests)
        );
        Map<String, WebSession> sessionsFromEs = transformIntoWebSessions(multiQueryResponses);

        divolteSessions.forEach(divoltSession -> {
            if (!mappingToReturn.containsKey(divoltSession)) {
                if (!sessionsFromEs.containsKey(divoltSession)) {
                    mappingToReturn.put(divoltSession, Optional.empty());//TODO P2 how to tell the cache there is no value in es !
                } else {
                    WebSession lastSessionInEs = sessionsFromEs.get(divoltSession);
                    cacheService.set(divoltSession, lastSessionInEs);
                    mappingToReturn.put(divoltSession, Optional.of(lastSessionInEs));
                }
            }
        });
        return mappingToReturn;
    }

    private Map<String/*divolteId*/, WebSession> transformIntoWebSessions(MultiQueryResponseRecord multiQueryResponses) {
        return multiQueryResponses.getDocs().stream()
                .map(doc -> {
                    return esDoc2WebSession(doc.getRetrievedFields());
                })
                .collect(Collectors.toMap(
                        WebSession::getOriginalSessionId,
                        Function.identity()
                ));
    }

    /**
     * Returns a new record based on the specified map that represents a web session in elasticsearch.
     *
     * @param sourceAsMap the web session stored in elasticsearch.
     * @return a new record based on the specified map that represents a web session in elasticsearch.
     */
    public WebSession esDoc2WebSession(final Map<String, String> sourceAsMap) {
        return esDoc2WebSession(sourceAsMap, OUTPUT_RECORD_TYPE);
    }

    /**
     * Returns a new WebSession based on the specified map that represents a web session in elasticsearch.
     *
     * @param sourceAsMap the web session stored in elasticsearch.
     * @param recordType the recordType value for record.
     * @return a new WebSession based on the specified map that represents a web session in elasticsearch.
     */
    public WebSession esDoc2WebSession(final Map<String, String> sourceAsMap, String recordType) {
        final Record record = new StandardRecord(recordType);
        sourceAsMap.forEach((key, value) ->
        {
            if (sessionInternalFields.getIsSessionActiveField().equals(key)) {
                record.setField(key, FieldType.BOOLEAN, Boolean.valueOf(value));
            } else if (sessionInternalFields.getSessionDurationField().equals(key)
                    || sessionInternalFields.getEventsCounterField().equals(key)
                    || sessionInternalFields.getTimestampField().equals(key)
                    || sessionInternalFields.getSessionInactivityDurationField().equals(key)
                    || sessionInternalFields.getFirstEventEpochSecondsField().equals(key)
                    || sessionInternalFields.getLastEventEpochSecondsField().equals(key)
                    || FieldDictionary.RECORD_TIME.equals(key)) {
                record.setField(key, FieldType.LONG, Long.valueOf(value));
            } else {
                record.setField(key, FieldType.STRING, value);
            }
        });
        record.setId(record.getField(sessionInternalFields.getSessionIdField()).asString());
        return new WebSession(record, this.sessionInternalFields);
    }


    /**
     * return a new Event based on the specified map that represents a web event in elasticsearch.
     *
     * @param sourceAsMap the event stored in elasticsearch.
     * @return a new Event based on the specified map that represents a web event in elasticsearch.
     */
    public Event esDoc2Event(final Map<String, String> sourceAsMap) {
        return esDoc2Event(sourceAsMap, OUTPUT_RECORD_TYPE);
    }

    /**
     * return a new Event based on the specified map that represents a web event in elasticsearch.
     *
     * @param sourceAsMap the event stored in elasticsearch.
     * @param recordType the recordType value for record.
     * @return a new Event based on the specified map that represents a web event in elasticsearch.
     */
    public Event esDoc2Event(final Map<String, String> sourceAsMap, String recordType) {
        final Record record = new StandardRecord(recordType);
        sourceAsMap.forEach((key, value) ->
        {
            record.setField(key, FieldType.STRING, value);
        });
        //TODO P2 verify id is okay in tests, and verify if okay generally to just store as string.
        return new Event(record, this.eventsInternalFields);
    }

    /**
     * The pattern to detect parameter 'gclid' in URL.
     */
    private static final Pattern GCLID = Pattern.compile(".*[\\&\\?]gclid=\\s*([^\\&\\?]+)\\&?.*");

    /**
     * The pattern to detect parameter 'gclsrc' in URL.
     */
    private static final Pattern GCLSRC = Pattern.compile(".*[\\&\\?]gclsrc=\\s*([^\\&\\?]+)\\&?.*");



    /**
     * If {@code true} prints additional logs.
     */
    public boolean _DEBUG = false;

    /**
     * Facility to log debug.
     *
     * @param format the format of the String.
     * @param args the arguments.
     */
    public void debug(final String format, final Object... args)
    {
        if ( _DEBUG )
        {
            if ( args.length == 0 )
            {
                getLogger().debug(format);
            }
            else
            {
                getLogger().debug(String.format(format + "\n", args));
            }
        }
    }

    /**
     * Facility to log debug.
     *
     * @param format the format of the String.
     * @param args the arguments.
     */
    public void error(final String format, final Object... args)
    {
        if ( _DEBUG )
        {
            if ( args.length == 0 )
            {
                getLogger().error(format);
            }
            else
            {
                getLogger().error(String.format(format + "\n", args));
            }
        }
    }

    /**
     * Returns the conversion of a record to a map where all {@code null} values were removed.
     *
     * @param record the record to convert.
     *
     * @return the conversion of a record to a map where all {@code null} values were removed.
     */
    private static Map<String, Object> toMap(final Record record)
    {
        return toMap(record, false);
    }

    /**
     * Returns the conversion of a record to a map where all {@code null} values were removed.
     *
     * @param record the record to convert.
     * @param innerRecord if {@code true} special dictionnary fields are ignored; included otherwise.
     *
     * @return the conversion of a record to a map where all {@code null} values were removed.
     */
    private static Map<String, Object> toMap(final Record record,
                                             final boolean innerRecord)
    {
        try
        {
            final Map<String, Object> result = new HashMap<>();

            record.getFieldsEntrySet()
                  .stream()
                  .forEach(entry ->
                  {
                      if ( !innerRecord || (innerRecord && ! FieldDictionary.contains(entry.getKey())) )
                      {
                          Object value = entry.getValue().getRawValue();
                          if (value != null) {
                              switch(entry.getValue().getType())
                              {
                                  case RECORD:
                                      value = toMap((Record)value, true);
                                      break;
                                  case ARRAY:
                                      Collection collection;
                                      if ( value instanceof Collection )
                                      {
                                          collection = (Collection)value;
                                      }
                                      else
                                      {
                                          collection = Arrays.asList(value);
                                      }
                                      final List list = new ArrayList(collection.size());
                                      for(final Object item: collection)
                                      {
                                          if ( item instanceof Record )
                                          {
                                              list.add(toMap((Record)item, true));
                                          }
                                          else
                                          {
                                              list.add(item);
                                          }
                                      }
                                      value = list;
                                      break;
                                  default:
                              }
                              result.put(entry.getKey(), value);
                          }
                      }
                  });
            return result;
        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * A singleton for valid check.
     */
    public final static SessionCheckResult VALID = new SessionCheckResult()
    {
        @Override
        public boolean isValid() { return true; }

        @Override
        public String reason() { return null; }
    };


    private static class InvalidSessionCheckResult
                   implements SessionCheckResult
    {
        private final String reason;

        public InvalidSessionCheckResult(final String reason) { this.reason = reason; }

        @Override
        public boolean isValid() { return false; }

        @Override
        public String reason() { return this.reason; }
    }

    private final static SessionCheckResult DAY_OVERLAP = new InvalidSessionCheckResult("Day overlap");
    private final static SessionCheckResult SESSION_TIMEDOUT = new InvalidSessionCheckResult("Session timed-out");
    private final static SessionCheckResult SOURCE_OF_TRAFFIC = new InvalidSessionCheckResult("Source of traffic differed");

}

