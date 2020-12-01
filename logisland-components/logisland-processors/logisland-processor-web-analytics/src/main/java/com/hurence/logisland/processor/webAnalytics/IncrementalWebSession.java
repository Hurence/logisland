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
import com.hurence.logisland.record.*;
import com.hurence.logisland.service.cache.CacheService;
import com.hurence.logisland.service.datastore.*;
import com.hurence.logisland.service.elasticsearch.ElasticsearchClientService;
import com.hurence.logisland.validator.StandardValidators;

import java.time.*;
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

    public static final String PROP_ES_SESSION_INDEX_FIELD = "es.session.index.field";
    public static final String PROP_ES_MAPPING_EVENT_TO_SESSION_INDEX_NAME = "es.mapping.event.to.session.index.name";
    public static final String PROP_ES_SESSION_TYPE_NAME = "es.session.type.name";
    public static final String PROP_ES_EVENT_INDEX_PREFIX = "es.event.index.prefix";
    public static final String PROP_ES_EVENT_TYPE_NAME = "es.event.type.name";

    public static final String ES_MAPPING_EVENT_TO_SESSION_TYPE_NAME = "mapping";
    public static final String MAPPING_FIELD = "sessionId";

    /**
     * Extra fields - for convenience - avoiding to parse the human readable first and last timestamps.
     */
    public static final String _FIRST_EVENT_EPOCH_FIELD = "firstEventEpochSeconds";
    public static final String _LAST_EVENT_EPOCH_FIELD = "lastEventEpochSeconds";

    public static final PropertyDescriptor ELASTICSEARCH_CLIENT_SERVICE =
            new PropertyDescriptor.Builder()
                    .name("elasticsearch.client.service")
                    .description("The instance of the Controller Service to use for accessing Elasticsearch.")
                    .required(true)
                    .identifiesControllerService(ElasticsearchClientService.class)
                    .build();

    public static final PropertyDescriptor DEBUG =
            new PropertyDescriptor.Builder()
                    .name("debug")
                    .description("Enable debug. If enabled, debug information are logged.")
                    .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
                    .required(false)
                    .defaultValue("false")
                    .build();

    public static final PropertyDescriptor ES_SESSION_INDEX_FIELD =
            new PropertyDescriptor.Builder()
                    .name(PROP_ES_SESSION_INDEX_FIELD)
                    .description("Name of the field in the record defining the ES index containing the web session documents.")
                    .required(true)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .build();

    public static final PropertyDescriptor ES_SESSION_TYPE_NAME =
            new PropertyDescriptor.Builder()
                    .name(PROP_ES_SESSION_TYPE_NAME)
                    .description("Name of the ES type of web session documents.")
                    .required(true)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .build();

    public static final PropertyDescriptor ES_MAPPING_EVENT_TO_SESSION_INDEX_NAME =
            new PropertyDescriptor.Builder()
                    .name(PROP_ES_MAPPING_EVENT_TO_SESSION_INDEX_NAME)
                    .description("Name of the ES index containing the mapping of web session documents.")
                    .required(true)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .build();

    public static final PropertyDescriptor SESSION_INACTIVITY_TIMEOUT =
            new PropertyDescriptor.Builder()
                    .name("session.timeout")
                    .description("session timeout in sec")
                    .required(false)
                    .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
                    .defaultValue("1800")
                    .build();

    public static final PropertyDescriptor SESSION_ID_FIELD =
            new PropertyDescriptor.Builder()
                    .name("sessionid.field")
                    .description("the name of the field containing the session id => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("sessionId")
                    .build();

    public static final PropertyDescriptor ES_EVENT_INDEX_PREFIX =
            new PropertyDescriptor.Builder()
                    .name(PROP_ES_EVENT_INDEX_PREFIX)
                    .description("Prefix of the index containing the web event documents.")
                    .required(true)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .build();

    public static final PropertyDescriptor ES_EVENT_TYPE_NAME =
            new PropertyDescriptor.Builder()
                    .name(PROP_ES_EVENT_TYPE_NAME)
                    .description("Name of the ES type of web event documents.")
                    .required(true)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .build();

    public static final PropertyDescriptor TIMESTAMP_FIELD =
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

    protected static final String PROP_SOURCE_OF_TRAFFIC_SUFFIX = "source_of_traffic.suffix";
    protected static final String SOURCE_OF_TRAFFIC_SUFFIX_NAME = "source_of_traffic";
    public static final String DIRECT_TRAFFIC = "direct";

    public final String FLAT_SEPARATOR = "_";
    public static final PropertyDescriptor SOURCE_OF_TRAFFIC_PREFIX_FIELD =
            new PropertyDescriptor.Builder()
                 .name(PROP_SOURCE_OF_TRAFFIC_SUFFIX)
                 .description("Prefix for the source of the traffic related fields")
                 .required(false)
                 .defaultValue(SOURCE_OF_TRAFFIC_SUFFIX_NAME)
                 .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                 .build();

    /**
     * The properties of this processor.
     */
    private final static List<PropertyDescriptor> SUPPORTED_PROPERTY_DESCRIPTORS =
            Collections.unmodifiableList(Arrays.asList(DEBUG,
                                                       ES_SESSION_INDEX_FIELD,
                                                       ES_SESSION_TYPE_NAME,
                                                       ES_EVENT_INDEX_PREFIX,
                                                       ES_EVENT_TYPE_NAME,
                    ES_MAPPING_EVENT_TO_SESSION_INDEX_NAME,
                                                       SESSION_ID_FIELD,
                                                       TIMESTAMP_FIELD,
                                                       VISITED_PAGE_FIELD,
                                                       USER_ID_FIELD,
                                                       FIELDS_TO_RETURN,
                                                       FIRST_VISITED_PAGE_FIELD,
                                                       LAST_VISITED_PAGE_FIELD,
                                                       IS_SESSION_ACTIVE_FIELD,
                                                       SESSION_DURATION_FIELD,
                                                       SESSION_INACTIVITY_DURATION_FIELD,
                                                       SESSION_INACTIVITY_TIMEOUT,
                                                       EVENTS_COUNTER_FIELD,
                                                       FIRST_EVENT_DATETIME_FIELD,
                                                       LAST_EVENT_DATETIME_FIELD,
                                                       NEW_SESSION_REASON_FIELD,
                                                       TRANSACTION_IDS,
                                                       SOURCE_OF_TRAFFIC_PREFIX_FIELD,
                                                       // Service
                                                       ELASTICSEARCH_CLIENT_SERVICE));

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors()
    {
        return SUPPORTED_PROPERTY_DESCRIPTORS;
    }

    @Override
    public boolean hasControllerService()
    {
        return true;
    }

    public ElasticsearchClientService elasticsearchClientService;
    public CacheService<String/*sessionId*/, WebSession> cacheService;
    public long _SESSION_INACTIVITY_TIMEOUT;
    public String _ORIGINAL_SESSION_ID_FIELD = "originalSessionId";
    public String _SESSION_ID_FIELD;
    public String _TIMESTAMP_FIELD;
    public String _VISITED_PAGE_FIELD;
    public Collection<String> _FIELDS_TO_RETURN;
    public String _USERID_FIELD;
    public String _FIRST_VISITED_PAGE_FIELD;
    public String _LAST_VISITED_PAGE_FIELD;
    public String _IS_SESSION_ACTIVE_FIELD;
    public String _SESSION_DURATION_FIELD;
    public String _EVENTS_COUNTER_FIELD;
    public String _FIRST_EVENT_DATETIME_FIELD;
    public String _LAST_EVENT_DATETIME_FIELD;
    public String _SESSION_INACTIVITY_DURATION_FIELD;
    public String _NEW_SESSION_REASON_FIELD;
    public String _TRANSACTION_IDS;
    public String _SOT_SOURCE_FIELD;
    public String _SOT_CAMPAIGN_FIELD;
    public String _SOT_MEDIUM_FIELD;
    public String _SOT_CONTENT_FIELD;
    public String _SOT_KEYWORD_FIELD;
    public String _ES_SESSION_INDEX_FIELD;
    public String _ES_SESSION_TYPE_NAME;
    public String _ES_EVENT_INDEX_PREFIX;
    public String _ES_EVENT_TYPE_NAME;
    public String _ES_MAPPING_EVENT_TO_SESSION_INDEX_NAME;
    public Collection<SessionCheck> checker;

    @Override
    public void init(final ProcessContext context) throws InitializationException
    {
        super.init(context);
        this.elasticsearchClientService = PluginProxy.rewrap(context.getPropertyValue(ELASTICSEARCH_CLIENT_SERVICE)
                .asControllerService());
        if (elasticsearchClientService == null)
        {
            getLogger().error("Elasticsearch client service is not initialized!");
        }
        cacheService = PluginProxy.rewrap(context.getPropertyValue(CONFIG_CACHE_SERVICE).asControllerService());
        if (cacheService == null) {
            getLogger().error("Cache service is not initialized!");
        }
        this._SESSION_INACTIVITY_TIMEOUT = context.getPropertyValue(SESSION_INACTIVITY_TIMEOUT).asLong();
        this._SESSION_ID_FIELD = context.getPropertyValue(SESSION_ID_FIELD).asString();
        this._TIMESTAMP_FIELD = context.getPropertyValue(TIMESTAMP_FIELD).asString();
        this._VISITED_PAGE_FIELD = context.getPropertyValue(VISITED_PAGE_FIELD).asString();

        String fieldsToReturn = context.getPropertyValue(FIELDS_TO_RETURN).asString();
        if (fieldsToReturn != null && !fieldsToReturn.isEmpty()) {
            this._FIELDS_TO_RETURN = Arrays.asList(fieldsToReturn.split(","));
        } else {
            this._FIELDS_TO_RETURN = Collections.emptyList();
        }

        this._USERID_FIELD = context.getPropertyValue(USER_ID_FIELD).asString();
        this._FIRST_VISITED_PAGE_FIELD = context.getPropertyValue(FIRST_VISITED_PAGE_FIELD).asString();
        this._LAST_VISITED_PAGE_FIELD = context.getPropertyValue(LAST_VISITED_PAGE_FIELD).asString();
        this._IS_SESSION_ACTIVE_FIELD = context.getPropertyValue(IS_SESSION_ACTIVE_FIELD).asString();
        this._SESSION_DURATION_FIELD = context.getPropertyValue(SESSION_DURATION_FIELD).asString();
        this._EVENTS_COUNTER_FIELD = context.getPropertyValue(EVENTS_COUNTER_FIELD).asString();
        this._FIRST_EVENT_DATETIME_FIELD = context.getPropertyValue(FIRST_EVENT_DATETIME_FIELD).asString();
        this._LAST_EVENT_DATETIME_FIELD = context.getPropertyValue(LAST_EVENT_DATETIME_FIELD).asString();
        this._SESSION_INACTIVITY_DURATION_FIELD = context.getPropertyValue(SESSION_INACTIVITY_DURATION_FIELD)
                .asString();
        this._NEW_SESSION_REASON_FIELD = context.getPropertyValue(NEW_SESSION_REASON_FIELD).asString();
        this._TRANSACTION_IDS = context.getPropertyValue(TRANSACTION_IDS).asString();

        final String sotPrefix = context.getPropertyValue(SOURCE_OF_TRAFFIC_PREFIX_FIELD).asString() + FLAT_SEPARATOR;

        this._SOT_SOURCE_FIELD = sotPrefix + SOURCE_OF_TRAFFIC_FIELD_SOURCE;
        this._SOT_CAMPAIGN_FIELD = sotPrefix + SOURCE_OF_TRAFFIC_FIELD_CAMPAIGN;
        this._SOT_MEDIUM_FIELD = sotPrefix + SOURCE_OF_TRAFFIC_FIELD_MEDIUM;
        this._SOT_CONTENT_FIELD = sotPrefix + SOURCE_OF_TRAFFIC_FIELD_CONTENT;
        this._SOT_KEYWORD_FIELD = sotPrefix + SOURCE_OF_TRAFFIC_FIELD_KEYWORD;

        this._ES_SESSION_INDEX_FIELD = context.getPropertyValue(ES_SESSION_INDEX_FIELD).asString();
        Objects.requireNonNull(this._ES_SESSION_INDEX_FIELD, "Property required: " + ES_SESSION_INDEX_FIELD);
        this._ES_SESSION_TYPE_NAME = context.getPropertyValue(ES_SESSION_TYPE_NAME).asString();
        Objects.requireNonNull(this._ES_SESSION_TYPE_NAME, "Property required: " + ES_SESSION_TYPE_NAME);
        this._ES_EVENT_INDEX_PREFIX = context.getPropertyValue(ES_EVENT_INDEX_PREFIX).asString();
        Objects.requireNonNull(this._ES_EVENT_INDEX_PREFIX, "Property required: " + ES_EVENT_INDEX_PREFIX);
        this._ES_EVENT_TYPE_NAME = context.getPropertyValue(ES_EVENT_TYPE_NAME).asString();
        Objects.requireNonNull(this._ES_EVENT_TYPE_NAME, "Property required: " + ES_EVENT_TYPE_NAME);
        this._ES_MAPPING_EVENT_TO_SESSION_INDEX_NAME = context.getPropertyValue(ES_MAPPING_EVENT_TO_SESSION_INDEX_NAME).asString();
        Objects.requireNonNull(this._ES_MAPPING_EVENT_TO_SESSION_INDEX_NAME, "Property required: " + ES_MAPPING_EVENT_TO_SESSION_INDEX_NAME);

        this.checker = Arrays.asList(
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
                    boolean isValid = durationInSeconds <= this._SESSION_INACTIVITY_TIMEOUT;

                    if (_DEBUG && !isValid) {
                        debug("'Timeout exceeded' isValid=" + isValid + " seconds=" + durationInSeconds +
                                " timeout=" + this._SESSION_INACTIVITY_TIMEOUT + " session-id=" + session.getSessionId());
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
        Collection<Events> allEvents = handleRewindAndGetAllNeededEvents(records);
        final Collection<SessionsCalculator> calculatedSessions = this.processEvents(allEvents);
        saveEventsToEs(allEvents);
        final Collection<Record> flattenedSessions = calculatedSessions.stream()
                .flatMap(sessionsCalculator -> sessionsCalculator.getSessions().stream())
                .collect(Collectors.toList());
        debug("Processing done. Outcoming records size=%d ", flattenedSessions.size());
        return flattenedSessions;
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

    private Collection<Events> handleRewindAndGetAllNeededEvents(final Collection<Record> records) {
        final Collection<Events> groupOfEvents = toWebEvents(records);
        Map<String/*sessionId*/, Optional<WebSession>> lastSessionMapping = getMapping(groupOfEvents);
        final SplittedEvents splittedEvents = getSplittedEvents(groupOfEvents, lastSessionMapping);
        final Collection<Events> eventsFromPast = splittedEvents.getEventsfromPast();
        final Collection<Events> eventsInNominalMode = splittedEvents.getEventsInNominalMode();

        if (!eventsFromPast.isEmpty()) {
            deleteFuturSessions(eventsFromPast);
            Collection<Event> eventsFromEs = getNeededEventsFromEs(eventsFromPast);//TODO
            Map<String/*sessionId*/, List<Event>> eventsFromEsBySessionId = eventsFromEs
                    .stream()
                    .collect(Collectors.groupingBy(Event::getSessionId));
            //merge those events into the list
            for (Events events : eventsFromPast) {
                List<Event> eventsFromEsForSession = eventsFromEsBySessionId.get(events.getSessionId());
                events.addAll(eventsFromEsForSession);//TODO faire un test qui verifie que les events deja dans events sont prioritaire
            }
        }
        return Stream.concat(
                eventsFromPast.stream(),
                eventsInNominalMode.stream()
        ).collect(Collectors.toList());
    }

    private Collection<Event> getNeededEventsFromEs(Collection<Events> eventsFromPast) {
//        elasticsearchClientService.waitUntilCollectionIsReadyAndRefreshIfAnyPendingTasks(_ES, 60000L);
        //            TODO get needed events for current session
            /*
                Pour chaque events trouver les evènements de la session en cour nécessaire.
                Le plus simple si on a le nouvel sessionId (change tout les jours)
                C'est de requêter tous les events de la sessionId et timestamp <= firstEventTs(input events)
            */
        String[] indicesToRefresh = null;
        elasticsearchClientService.waitUntilCollectionIsReadyAndRefreshIfAnyPendingTasks(indicesToRefresh, 60000L);
        elasticsearchClientService.searchNumb
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
        return null;//TODO
    }

    private void deleteFuturSessions(Collection<Events> eventsFromPast) {
        /*
            Pour chaque events trouver le min, effacer toutes les sessions avec
            originalSessionId = sessionId && firstEventTs(session) > firstEventTs(input events)
           ==> du coup on a plus que les sessions plus ancienne ou la session actuelle dans es.
        */
        //TODO We could use a deleteByQuery here, but in 2.4 this is a plugin and may not be available.
        // Another solution is to use the Bulk api with delete query using id of documents.
        // We could add a method in ElasticSearchCLient interface isSupportingDeleteByQuery() to use it when available.
        QueryRecord queryRecord = new QueryRecord();
        queryRecord.setRefresh(false);
        for (Events events : eventsFromPast) {
            final String sessionIndexName = events.first().getValue(_ES_SESSION_INDEX_FIELD).toString();
            queryRecord.addCollection(sessionIndexName);
            final String sessionId = events.getSessionId();
            queryRecord.addTermQuery(new TermQueryRecord(_ORIGINAL_SESSION_ID_FIELD, sessionId));
            queryRecord.addRangeQuery(
                    new RangeQueryRecord(_TIMESTAMP_FIELD)
                    .setFrom(events.first().getTimeStampAsLong())
            );
        }
        elasticsearchClientService.deleteByQuery(queryRecord);
    }

    private SplittedEvents getSplittedEvents(Collection<Events> groupOfEvents, Map<String/*sessionId*/, Optional<WebSession>> lastSessionMapping) {
        final Collection<Events> eventsFromPast = new ArrayList<>();
        final Collection<Events> eventsOk = new ArrayList<>();
        for (Events events : groupOfEvents) {
            Optional<WebSession> lastSession = lastSessionMapping.get(events.getSessionId());
            if (lastSession.isPresent() && lastSession.get().getLastEvent().toInstant().toEpochMilli() > events.first().getTimeStampAsLong()) {//> ou >= ?
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
                        .map(record -> new Event(record, this))
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
     * The events' index suffix formatter.
     */
    private final DateTimeFormatter EVENT_SUFFIX_FORMATTER = DateTimeFormatter.ofPattern("yyyy.MM.dd",
            Locale.ENGLISH);

    /**
     * Returns the name of the event index corresponding to the specified date such as
     * ${event-index-name}.${event-suffix}.
     * Eg. openanalytics-webevents.2018.01.31
     *
     * @param date the timestamp of the event to store in the index.
     * @return the name of the event index corresponding to the specified date.
     */
    private String toEventIndexName(final ZonedDateTime date) {
        return _ES_EVENT_INDEX_PREFIX + "." + EVENT_SUFFIX_FORMATTER.format(date);
    }

    /**
     * Processes the provided events and returns their resulting sessions.
     * All sessions are retrieved from the cache or elasticsearch and then updated with the specified web events.
     * One serie of events may result into multiple sessions.
     *
     * @param webEvents the web events from a same session to process.
     * @return web sessions resulting of the processing of the web events.
     */
    private Collection<SessionsCalculator> processEvents(final Collection<Events> webEvents) {
        Map<String, WebSession> lastSessionMapping = getMapping(webEvents);//TODO
        // Applies all events to session documents and collect results.
        final Collection<SessionsCalculator> result =
                webEvents.stream()
                        .map(events -> new SessionsCalculator(this, events.getSessionId(),
                                lastSessionMapping.get(events.getSessionId())).processEvents(events))
                        .collect(Collectors.toList());

        return result;
    }

    private Map<String, Optional<WebSession>> getMapping(final Collection<Events> webEvents) {
        //First retrieve mapping of last sessions.
        // Eg sessionId -> sessionId#XX
        final MultiGetQueryRecordBuilder mgqrBuilder = new MultiGetQueryRecordBuilder();
        webEvents.forEach(events -> mgqrBuilder.add(_ES_MAPPING_EVENT_TO_SESSION_INDEX_NAME, ES_MAPPING_EVENT_TO_SESSION_TYPE_NAME,
                null, events.getSessionId()));

        List<MultiGetResponseRecord> esResponse = null;
        try {
            esResponse = elasticsearchClientService.multiGet(mgqrBuilder.build());
        } catch (final InvalidMultiGetQueryRecordException e) {
            // should never happen
            getLogger().error("error while executing multiGet elasticsearch", e);
        }

        // Documents have only one field "sessionId" that corresponds to last session.
        Map<String/*sessionId*/, String/*sessionId#<last>*/> _mappings = Collections.emptyMap();
        if (!esResponse.isEmpty()) {
            _mappings =
                    esResponse.stream()
                            .collect(Collectors.toMap(response -> response.getDocumentId(),
                                    response -> response.getRetrievedFields().get(MAPPING_FIELD)));
        }

        final Map<String/*sessionId*/, String/*sessionId#<last>*/> mappings = _mappings;

        // Retrieve all last sessionId from elasticsearch.
        final MultiGetQueryRecordBuilder sessionBuilder = new MultiGetQueryRecordBuilder();
        if (!mappings.isEmpty()) {
            webEvents.forEach(events ->
            {
                String sessionId = events.getSessionId();
                String mappedSessionId = mappings.get(sessionId); // last processed session id (#?)
                // Retrieve the name of the index that contains the websessions.
                // The chaining calls are on purpose as any NPE would mean something is wrong.
                final String sessionIndexName = events.first().getValue(_ES_SESSION_INDEX_FIELD).toString();
                elasticsearchClientService.waitUntilCollectionIsReadyAndRefreshIfAnyPendingTasks(sessionIndexName, 60000L);
                sessionBuilder.add(sessionIndexName, _ES_SESSION_TYPE_NAME,
                        null, mappedSessionId != null ? mappedSessionId : sessionId);
            });
        }

        esResponse = null;
        try {
            esResponse = elasticsearchClientService.multiGet(sessionBuilder.build());
        } catch (final InvalidMultiGetQueryRecordException e) {
            // should never happen
            getLogger().error("error while executing multiGet elasticsearch", e);
        }

        debug("Retrieved %d documents from elasticsearch.", esResponse.size());

        // Grouped all retrieved elasticsearch documents by their session identifier.
        final Map<String/*doc id without #*/,
                List<WebSession>/*session-id or session-id#0,...,session-id#N*/> sessionDocs =
                esResponse.isEmpty()
                        ? Collections.emptyMap()
                        : esResponse.stream()
                        .map(response -> new WebSession(esDoc2Record(response.getRetrievedFields()), this))
                        .collect(Collectors.groupingBy(record -> record.getSessionId()
                                .split(EXTRA_SESSION_DELIMITER)[0]));

        return null;//TODO
    }

    /**
     * Returns a new record based on the specified map that represents a web session in elasticsearch.
     *
     * @param sourceAsMap the web session stored in elasticsearch.
     * @return a new record based on the specified map that represents a web session in elasticsearch.
     */
    public Record esDoc2Record(final Map<String, String> sourceAsMap) {
        final Record record = new StandardRecord(OUTPUT_RECORD_TYPE);
        sourceAsMap.forEach((key, value) ->
        {
            if (_IS_SESSION_ACTIVE_FIELD.equals(key)) {
                // Boolean
                record.setField(key, FieldType.BOOLEAN, Boolean.valueOf(value));
            } else if (_SESSION_DURATION_FIELD.equals(key)
                    || _EVENTS_COUNTER_FIELD.equals(key)
                    || _TIMESTAMP_FIELD.equals(key)
                    || _SESSION_INACTIVITY_DURATION_FIELD.equals(key)
                    || _FIRST_EVENT_EPOCH_FIELD.equals(key)
                    || _LAST_EVENT_EPOCH_FIELD.equals(key)
                    || _SESSION_INACTIVITY_DURATION_FIELD.equals(key)
                    || "record_time".equals(key)) {
                // Long
                record.setField(key, FieldType.LONG, Long.valueOf(value));
            } else {
                // String
                record.setField(key, FieldType.STRING, value);
            }
        });

        record.setId(record.getField(_SESSION_ID_FIELD).asString());

        return record;
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

