package com.hurence.logisland.processor.webAnalytics;

import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.ProcessException;
import com.hurence.logisland.processor.elasticsearch.AbstractElasticsearchProcessor;
import com.hurence.logisland.record.*;
import com.hurence.logisland.service.elasticsearch.ElasticsearchClientService;
import com.hurence.logisland.service.elasticsearch.multiGet.InvalidMultiGetQueryRecordException;
import com.hurence.logisland.service.elasticsearch.multiGet.MultiGetQueryRecordBuilder;
import com.hurence.logisland.service.elasticsearch.multiGet.MultiGetResponseRecord;
import com.hurence.logisland.validator.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * This processor handles web-sessions.
 *
 * Web-events (user's clicks) trigger the creation or update of their associated web-sessions.
 * Web-sessions are identified by their session identifiers and stored in elasticsearch.
 *
 * Web-events are grouped by session id and applied chronologically. Before application the web-session is retrieved
 * from elasticsearch. If none exists yet, the web-session is created base on the web-event (first event timestamp,
 * ...). Then updates are applied (last event timestamp, session duration, ...). Finally the web-session is passed to
 * the next processor. The last processor must save the returned records in elasticsearch.
 *
 * WebEvent --> ThisProcessor -> ... -(write)-> ElasticsearchService
 *                    ^                                  |
 *                    +------------(read)----------------+
 *
 * This processor is asymmetric in two ways:
 * - web-sessions are retrieved from elasticsearch but not updated. Moreover the processor expects that returned
 * records representing web-sessions are stored later on.
 * - web-session handled by this processor are always active as triggered by a user action. An update by query script
 * in charge of tagging web-sessions as inactive is performed outside of the purpose of this processor.
 *
 * Script to run in order to flag web-sessions as inactive.
 * <pre>
 * curl -XPOST '${elasticsearch}/${index}/${type}/_update_by_query' -d
 * {
 *   "script": {
 *     "script": "ctx._source.sessionInactivityDuration=Math.min((long)(DateTime.now().getMillis()/1000) - Long.valueOf
 (ctx._source.lastEventEpochSeconds), 3600);ctx._source.is_sessionActive=ctx._source.sessionInactivityDuration < 3600;"
 *   },
 *   "query": {
 *     "term": {
 *       "is_sessionActive": true
 *     }
 *   }
 * }

 * </pre>
 */
public class IncrementalWebSession
        extends AbstractElasticsearchProcessor
{
    /**
     * The logger.
     */
    private static Logger LOG = LoggerFactory.getLogger(ConsolidateSession.class);

    /**
     * If {@code true} prints additional logs.
     */
    private boolean _DEBUG = false;

    /**
     * Facility to log debug.
     *
     * @param format the format of the String.
     * @param args the arguments.
     */
    private void debug(final String format, final Object... args)
    {
        if ( _DEBUG )
        {
            if ( args.length == 0 )
            {
                LOG.debug(format);
            }
            else
            {
                LOG.debug(String.format(format + "\n", args));
            }
        }
    }

    private static final String OUTPUT_RECORD_TYPE = "consolidate-session";

    private static final String PROP_ES_INDEX = "es.index";
    private static final String PROP_ES_BACKUP_INDEX = "es-backup.index";
    private static final String PROP_ES_TYPE = "es.type";
    private static final String PROP_FILTERING_FIELD = "field.name";

    private static final String _FIRST_EVENT_EPOCH_FIELD = "firstEventEpochSeconds";
    private static final String _LAST_EVENT_EPOCH_FIELD = "lastEventEpochSeconds";

    public static final PropertyDescriptor ELASTICSEARCH_CLIENT_SERVICE = new PropertyDescriptor.Builder()
                                                                                  .name("elasticsearch.client.service")
                                                                                  .description("The instance of the Controller Service to use for accessing Elasticsearch.")
                                                                                  .required(true)
                                                                                  .identifiesControllerService(ElasticsearchClientService.class)
                                                                                  .build();

    private static final PropertyDescriptor DEBUG = new PropertyDescriptor.Builder()
                                                            .name("debug")
                                                            .description("Enable debug. If enabled, debug information are logged.")
                                                            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
                                                            .required(false)
                                                            .defaultValue("false")
                                                            .build();

    static final PropertyDescriptor FILTERING_FIELD = new PropertyDescriptor.Builder()
                                                              .name(PROP_FILTERING_FIELD)
                                                              .description("The field to distinct records")
                                                              .required(true)
                                                              .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                                                              .defaultValue(FieldDictionary.RECORD_ID)
                                                              .build();

    static final PropertyDescriptor ES_INDEX_FIELD = new PropertyDescriptor.Builder()
                                                             .name(PROP_ES_INDEX)
                                                             .description("Name of the ES index containing the web session documents.")
                                                             .required(true)
                                                             .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                                                             .build();

    static final PropertyDescriptor ES_TYPE_FIELD = new PropertyDescriptor.Builder()
                                                            .name(PROP_ES_TYPE)
                                                            .description("Name of the ES type of web session documents.")
                                                            .required(false)
                                                            .defaultValue("default")
                                                            .build();

    static final PropertyDescriptor ES_BACKUP_INDEX_FIELD = new PropertyDescriptor.Builder()
                                                                    .name(PROP_ES_BACKUP_INDEX)
                                                                    .description("Name of the ES index containing the web session - backup - documents.")
                                                                    .required(true)
                                                                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                                                                    .build();

    static final PropertyDescriptor SESSION_INACTIVITY_TIMEOUT = new PropertyDescriptor.Builder()
                                                                         .name("session.timeout")
                                                                         .description("session timeout in sec")
                                                                         .required(false)
                                                                         .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
                                                                         .defaultValue("1800")
                                                                         .build();

    static final PropertyDescriptor SESSION_ID_FIELD = new PropertyDescriptor.Builder()
                                                               .name("sessionid.field")
                                                               .description("the name of the field containing the session id => will override default value if set")
                                                               .required(false)
                                                               .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                                                               .defaultValue("sessionId")
                                                               .build();

    static final PropertyDescriptor TIMESTAMP_FIELD = new PropertyDescriptor.Builder()
                                                              .name("timestamp.field")
                                                              .description("the name of the field containing the timestamp => will override default value if set")
                                                              .required(false)
                                                              .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                                                              .defaultValue("h2kTimestamp")
                                                              .build();

    static final PropertyDescriptor VISITED_PAGE_FIELD = new PropertyDescriptor.Builder()
                                                                 .name("visitedpage.field")
                                                                 .description("the name of the field containing the visited page => will override default value if set")
                                                                 .required(false)
                                                                 .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                                                                 .defaultValue("location")
                                                                 .build();


    static final PropertyDescriptor USER_ID_FIELD = new PropertyDescriptor.Builder()
                                                            .name("userid.field")
                                                            .description("the name of the field containing the userId => will override default value if set")
                                                            .required(false)
                                                            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                                                            .defaultValue("userId")
                                                            .build();

    static final PropertyDescriptor FIELDS_TO_RETURN = new PropertyDescriptor.Builder()
                                                               .name("fields.to.return")
                                                               .description("the list of fields to return")
                                                               .required(false)
                                                               .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
                                                               .build();

    static final PropertyDescriptor FIRST_VISITED_PAGE_FIELD = new PropertyDescriptor.Builder()
                                                                       .name("firstVisitedPage.out.field")
                                                                       .description("the name of the field containing the first visited page => will override default value if set")
                                                                       .required(false)
                                                                       .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                                                                       .defaultValue("firstVisitedPage")
                                                                       .build();

    static final PropertyDescriptor LAST_VISITED_PAGE_FIELD = new PropertyDescriptor.Builder()
                                                                      .name("lastVisitedPage.out.field")
                                                                      .description("the name of the field containing the last visited page => will override default value if set")
                                                                      .required(false)
                                                                      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                                                                      .defaultValue("lastVisitedPage")
                                                                      .build();

    static final PropertyDescriptor IS_SESSION_ACTIVE_FIELD = new PropertyDescriptor.Builder()
                                                                      .name("isSessionActive.out.field")
                                                                      .description("the name of the field stating whether the session is active or not => will override default value if set")
                                                                      .required(false)
                                                                      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                                                                      .defaultValue("is_sessionActive")
                                                                      .build();

    static final PropertyDescriptor SESSION_DURATION_FIELD = new PropertyDescriptor.Builder()
                                                                     .name("sessionDuration.out.field")
                                                                     .description("the name of the field containing the session duration => will override default value if set")
                                                                     .required(false)
                                                                     .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                                                                     .defaultValue("sessionDuration")
                                                                     .build();

    static final PropertyDescriptor SESSION_INACTIVITY_DURATION_FIELD = new PropertyDescriptor.Builder()
                                                                                .name("sessionInactivityDuration.out.field")
                                                                                .description("the name of the field containing the session inactivity duration => will override default value if set")
                                                                                .required(false)
                                                                                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                                                                                .defaultValue("sessionInactivityDuration")
                                                                                .build();

    static final PropertyDescriptor EVENTS_COUNTER_FIELD = new PropertyDescriptor.Builder()
                                                                   .name("eventsCounter.out.field")
                                                                   .description("the name of the field containing the session duration => will override default value if set")
                                                                   .required(false)
                                                                   .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                                                                   .defaultValue("eventsCounter")
                                                                   .build();

    static final PropertyDescriptor FIRST_EVENT_DATETIME_FIELD = new PropertyDescriptor.Builder()
                                                                         .name("firstEventDateTime.out.field")
                                                                         .description("the name of the field containing the date of the first event => will override default value if set")
                                                                         .required(false)
                                                                         .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                                                                         .defaultValue("firstEventDateTime")
                                                                         .build();

    static final PropertyDescriptor LAST_EVENT_DATETIME_FIELD = new PropertyDescriptor.Builder()
                                                                        .name("lastEventDateTime.out.field")
                                                                        .description("the name of the field containing the date of the last event => will override default value if set")
                                                                        .required(false)
                                                                        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                                                                        .defaultValue("lastEventDateTime")
                                                                        .build();

    /**
     * The legacy format used in Date.toString().
     */
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss zzz yyyy",
                                                                                     Locale.ENGLISH);

    /**
     * Returns the provided epoch timestamp formatted with the default formatter.
     *
     * @param epoch the timestamp in milliseconds.
     *
     * @return the provided epoch timestamp formatted with the default formatter.
     */
    static String toFormattedDate(final long epoch)
    {
        ZonedDateTime date = Instant.ofEpochMilli(epoch).atZone(ZoneId.systemDefault());
        String result = DATE_FORMAT.format(date);

        return result;
    }

    /**
     * The properties of this processor.
     */
    private final static List<PropertyDescriptor> SUPPORTED_PROPERTY_DESCRIPTORS =
                                           Collections.unmodifiableList(Arrays.asList(DEBUG,
                                                                                      FILTERING_FIELD,
                                                                                      ES_INDEX_FIELD,
                                                                                      ES_TYPE_FIELD,
                                                                                      ES_BACKUP_INDEX_FIELD,
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
                                                                                      // Service
                                                                                      ELASTICSEARCH_CLIENT_SERVICE));

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors()
    {
        return SUPPORTED_PROPERTY_DESCRIPTORS;
    }

    @Override
    public Collection<Record> process(final ProcessContext context,
                                      final Collection<Record> records)
            throws ProcessException
    {
        debug("Entering processor with %s ", records);
        return new Execution(context).process(records);
    }

    /**
     * Postfix of document identifier.
     */
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    /**
     * Returns a document identifier as sessionId-formattedDate.
     *
     * @param sessionId the session identifier.
     * @param date the date of the document.
     *
     * @return a document identifier.
     */
    static String toDocumentId(final String sessionId,
                               final LocalDate date)
    {
        return sessionId + "-" + date.format(FORMATTER);
    }

    /**
     * Return {@code true} if the provided field has a non-null value;  {@code false} otherwise.
     *
     * @param field the field to check.
     *
     * @return {@code true} if the provided field has a non-null value;  {@code false} otherwise.
     */
    private static boolean isFieldAssigned(final Field field)
    {
        return field!=null && field.getRawValue()!=null;
    }

    /**
     * A class that performs a unique execution.
     */
    private class Execution
    {
        private final ProcessContext context;
        private final Map<String, Session> sessions = new HashMap<>();

        /*
         * Field names.
         */
        private final long _SESSION_INACTIVITY_TIMEOUT;
        private final String _SESSION_ID_FIELD;
        private final String _TIMESTAMP_FIELD;
        private final String _VISITED_PAGE_FIELD;
        private final String _FIELDS_TO_RETURN;
        private final String _USERID_FIELD;
        private final String _FIRST_VISITED_PAGE_FIELD;
        private final String _LAST_VISITED_PAGE_FIELD;
        private final String _IS_SESSION_ACTIVE_FIELD;
        private final String _SESSION_DURATION_FIELD;
        private final String _EVENTS_COUNTER_FIELD;
        private final String _FIRST_EVENT_DATETIME_FIELD;
        private final String _LAST_EVENT_DATETIME_FIELD;
        private final String _SESSION_INACTIVITY_DURATION_FIELD;

        private final String _ES_BACKUP_INDEX_FIELD;
        private final String _ES_INDEX_FIELD;
        private final String _ES_TYPE_FIELD;

        private Execution(final ProcessContext context)
        {
            this.context = context;

            this._SESSION_INACTIVITY_TIMEOUT        = context.getPropertyValue(SESSION_INACTIVITY_TIMEOUT).asLong();
            this._SESSION_ID_FIELD                  = context.getPropertyValue(SESSION_ID_FIELD).asString();
            this._TIMESTAMP_FIELD                   = context.getPropertyValue(TIMESTAMP_FIELD).asString();
            this._VISITED_PAGE_FIELD                = context.getPropertyValue(VISITED_PAGE_FIELD).asString();
            this._FIELDS_TO_RETURN                  = context.getPropertyValue(FIELDS_TO_RETURN).asString();
            this._USERID_FIELD                      = context.getPropertyValue(USER_ID_FIELD).asString();
            this._FIRST_VISITED_PAGE_FIELD          = context.getPropertyValue(FIRST_VISITED_PAGE_FIELD).asString();
            this._LAST_VISITED_PAGE_FIELD           = context.getPropertyValue(LAST_VISITED_PAGE_FIELD).asString();
            this._IS_SESSION_ACTIVE_FIELD           = context.getPropertyValue(IS_SESSION_ACTIVE_FIELD).asString();
            this._SESSION_DURATION_FIELD            = context.getPropertyValue(SESSION_DURATION_FIELD).asString();
            this._EVENTS_COUNTER_FIELD              = context.getPropertyValue(EVENTS_COUNTER_FIELD).asString();
            this._FIRST_EVENT_DATETIME_FIELD        = context.getPropertyValue(FIRST_EVENT_DATETIME_FIELD).asString();
            this._LAST_EVENT_DATETIME_FIELD         = context.getPropertyValue(LAST_EVENT_DATETIME_FIELD).asString();
            this._SESSION_INACTIVITY_DURATION_FIELD = context.getPropertyValue(SESSION_INACTIVITY_DURATION_FIELD).asString();

            this._ES_INDEX_FIELD = context.getPropertyValue(ES_INDEX_FIELD).asString();
            this._ES_TYPE_FIELD = context.getPropertyValue(ES_TYPE_FIELD).asString();
            this._ES_BACKUP_INDEX_FIELD = context.getPropertyValue(ES_BACKUP_INDEX_FIELD).asString();

            _DEBUG = context.getPropertyValue(DEBUG).asBoolean();
        }

        public Collection<Record> process(final Collection<Record> records)
                throws ProcessException
        {
            debug("#1 records size=%d ", records.size());
            debug("#1 records=%s ", records);

            // Create webEvents from input records.
            // A webEvents contains all webEvent instances of its session id.
            final Set<WebEvents> webEvents =
                    records.stream()
                           // Remove record without session Id or timestamp.
                           .filter(record -> isFieldAssigned(record.getField(_SESSION_ID_FIELD))
                                          && isFieldAssigned(record.getField(_TIMESTAMP_FIELD)))
                           // Group records per session Id.
                           .collect(Collectors.groupingBy(record-> record.getField(_SESSION_ID_FIELD).asString()))
                           // Group all records with same session Id into WebEvents.
                           .entrySet()
                           .stream()
                           .map(entry ->
                                {
                                    final String sessionId = entry.getKey();
                                    final WebEvents result = new WebEvents(sessionId);
                                    this.sessions.put(sessionId, new Session(sessionId, result));
                                    entry.getValue().stream().forEach(record->result.add(new WebEvent(record)));
                                    return result;
                                })
                           .collect(Collectors.toSet());

            debug("#2 webEvents=%s ", webEvents);

            // Retrieve all documents from elasticsearch.
            final MultiGetQueryRecordBuilder mgqrBuilder = new MultiGetQueryRecordBuilder();
            final Map<String/*sessionId*/,
                      Collection<ElasticsearchWebSessionRequest>> esRequests = new HashMap<>(webEvents.size());

            webEvents.stream()
                     .flatMap(event ->
                              {
                                  // Retrieve which documents to retrieve from elasticsearch for that event.
                                  final Collection<ElasticsearchWebSessionRequest> requests = event.queryWebSessions();

                                  debug("ElasticsearchWebSessionRequest = %s", requests);

                                  // Save pair (session, esRequests).
                                  esRequests.put(event.sessionId, requests);

                                  return requests.stream();
                              })
                     .forEach(request ->
                              {
                                  debug("mgqrBuilder.add(%s, %s, null, %s)",
                                        request.index,
                                        _ES_TYPE_FIELD,
                                        request.docId);
                                  mgqrBuilder.add(request.index, _ES_TYPE_FIELD, null, request.docId);
                              });

            List<MultiGetResponseRecord> multiGetResponseRecords = null;
            try
            {
                multiGetResponseRecords = elasticsearchClientService.multiGet(mgqrBuilder.build());
            }
            catch (final InvalidMultiGetQueryRecordException e )
            {
                // should never happen
                e.printStackTrace();
            }

            debug("MultiGetResponseRecord.size()=%d", multiGetResponseRecords.size());

            if ( ! multiGetResponseRecords.isEmpty() )
            {
                multiGetResponseRecords.stream()
                                       .collect(Collectors.groupingBy(doc -> doc.getDocumentId()))
                                       .forEach((sessionId, esDocs)->
                                                {
                                                    final Session session = sessions.get(sessionId);
                                                    if ( session != null )
                                                    {
                                                        session.esDocs = esDocs;
                                                    }
                                                });
            }

            final Collection<Record> result = new ArrayList<>();

            for(final Map.Entry<String, Session> entry: sessions.entrySet())
            {
                // final String sessionId = entry.getKey();
                final Session session = entry.getValue();

                session.updateSession();
                result.addAll(session.updates);
            }

            debug("Processed updates=%s", result);

            return result;
        }

        /**
         * A class to represent a web-session request against elasticsearch with an index and a document identifier.
         */
        private class ElasticsearchWebSessionRequest
        {
            /**
             * The index of this request.
             */
            final String index;
            /**
             * The document identifier of this request.
             */
            final String docId;

            /**
             * Creates a new instance of this class with the provided parameters.
             *
             * @param index the index of this request.
             * @param docId the document identifier of this request.
             */
            private ElasticsearchWebSessionRequest(final String index,
                                                   final String docId)
            {
                this.index = index;
                this.docId = docId;
            }

            /**
             * An elasticsearch request that retrieves WebSession documents from the current index for the provided
             * session identifier.
             *
             * @param sessionId the session identifier.
             */
            public ElasticsearchWebSessionRequest(final String sessionId)
            {
                this(_ES_INDEX_FIELD, sessionId);
            }

            /**
             * An elasticsearch request that retrieves WebSession documents from the backup index for the provided
             * session identifier.
             *
             * @param sessionId the session identifier.
             * @param date the timestamp to compute prefix for the final session identifier.
             */
            public ElasticsearchWebSessionRequest(final String sessionId,
                                                  final LocalDate date)
            {
                this(_ES_BACKUP_INDEX_FIELD, toDocumentId(sessionId, date));
            }

            @Override
            public String toString()
            {
                return "ElasticsearchWebSessionRequest{index='" + index + "\', docId='" + docId + "\'}";
            }
        }

        /**
         * This class is just a container to tie all objects related to the update of a single session:
         * - the session identifier,
         * - the collection of user's events,
         * - the existing web sessions retrieved from back-end,
         * - the web sessions updated to pass to the next processor.
         */
        private class Session
        {
            /**
             * The session identifier.
             */
            private final String sessionId;
            /**
             * All web events associated to this session.
             */
            private final WebEvents webEvents;
            /**
             * All elasticsearch documents stored for this session.
             */
            private Collection<MultiGetResponseRecord> esDocs = Collections.emptyList();
            /**
             * The web session updates computed for this session.
             */
            private Collection<Record> updates = Collections.emptyList();

            /**
             * Constructs a new instance of this class with the provided parameter.
             *
             * @param sessionId the session of this object.
             */
            public Session(final String sessionId,
                           final WebEvents webEvents)
            {
                this.sessionId = sessionId;
                this.webEvents = webEvents;
            }

            /**
             * Applies the web events to the web sessions retrieved from the back-end.
             */
            public void updateSession()
            {
                if ( this.webEvents != null )
                {
                    this.updates = this.webEvents.apply(this.esDocs);

                    debug("this.updates=%s", this.updates);
                }
            }
        }

        /**
         * This class represents a complete sequence of web events of a single session.
         */
        private class WebEvents
        {
            /**
             * The session identifier.
             */
            private final String sessionId;
            /**
             * The set of web events grouped by day.
             */
            private final NavigableSet<EventDay> eventDays = new TreeSet<>();

            public WebEvents(final String sessionId)
            {
                this.sessionId = sessionId;
            }

            /**
             * Adds the provided event to this collection of events.
             *
             * @param webEvent the event to add to this collection.
             */
            public void add(final WebEvent webEvent)
            {
                final LocalDate eventId = webEvent.timestamp.toLocalDate();

                EventDay eventDay = null;
                for(final EventDay _eventDay : this.eventDays)
                {
                    if ( _eventDay.eventId.equals(eventId) )
                    {
                        eventDay = _eventDay;
                    }
                }
                if ( eventDay == null )
                {
                    eventDay = new EventDay(eventId);
                    this.eventDays.add(eventDay);
                }

                eventDay.add(webEvent);
            }

            /**
             * Returns the expected web session identifiers from back-end. In case none exists yet, this will return
             * the expected identifier but as non exists in the back-end, a new web session will be created finally.
             *
             * @return the expected web session identifiers from back-end.
             */
            public Collection<ElasticsearchWebSessionRequest> queryWebSessions()
            {
                final EventDay firstEventDay = this.eventDays.first();
                final LocalDate now = LocalDate.now();

                Collection<ElasticsearchWebSessionRequest> result;
                if ( firstEventDay.eventId.equals(now) )
                {
                    // Oldest eventDay is today.
                    result = Collections.singleton(new ElasticsearchWebSessionRequest(this.sessionId));
                }
                else
                {
                    result = new ArrayList<>(this.eventDays.size());

                    for(final EventDay eventDay : this.eventDays)
                    {
                        if ( ! eventDay.eventId.equals(now) )
                        {
                            result.add(new ElasticsearchWebSessionRequest(this.sessionId, eventDay.eventId));
                        }
                        else
                        {
                            result.add(new ElasticsearchWebSessionRequest(this.sessionId));
                        }
                    }
                }

                return result;
            }

            /**
             * Applies the events contained in this object to the oldest backup provided as argument.
             * Web events are applied per day and a new websession's backup is created for each day.
             * The updated web sessions are returned.
             *
             * @param backups all web sessions retrieved from back-end.
             *
             * @return all updated web sessions.
             */
            public Collection<Record> apply(final Collection<MultiGetResponseRecord> backups)
            {
                final Collection<Record> result = new ArrayList<>(backups.size());

                WebSession webSession;
                SortedSet<EventDay> eventDayToApply;

                if ( backups.isEmpty() )
                {
                    // The web session does not yet exists.
                    webSession = new WebSession(this.sessionId);
                    eventDayToApply = this.eventDays; // Apply all web events contained in eventDays.
                }
                else
                {
                    // The web session has backups.
                    final Map<String/*doc id*/, Map<String, String>/*sourceAsMap*/> docs =
                            backups.stream()
                                   .map(response -> new AbstractMap.SimpleEntry<>(response.getDocumentId(),
                                                                                  response.getRetrievedFields()))
                                   .collect(Collectors.toMap(Map.Entry::getKey,
                                                             Map.Entry::getValue));

                    // EventDay are chronologically sorted. Find oldest one with a valid backup.
                    final Iterator<EventDay> iterator = this.eventDays.iterator();
                    Map<String, String> backup = null; // sourceAsMap
                    EventDay eventDay = null;
                    while (iterator.hasNext() && backup == null)
                    {
                        eventDay = iterator.next();
                        backup = docs.get(toDocumentId(this.sessionId, eventDay.eventId));
                    }

                    if (backup == null)
                    {
                        // No backup with embedded timestamp. Take current web session.
                        backup = docs.get(this.sessionId);
                    }

                    if (backup == null)
                    {
                        // Paranoid++.
                        webSession = new WebSession(this.sessionId);
                    }
                    else
                    {
                        webSession = new WebSession(this.sessionId, backup);
                    }

                    eventDayToApply = this.eventDays.tailSet(eventDay);
                }


                // Apply next eventDays.
                for(final EventDay eventDay : eventDayToApply)
                {
                    debug("Applying %s to %s", eventDay, webSession);
                    eventDay.apply(webSession);
                    result.add(webSession.cloneRecord().setId(toDocumentId(this.sessionId, eventDay.eventId)));
                }
                // Also duplicate current web session to current index.
                result.add(webSession.cloneRecord());

                debug("WebEvents.apply=%s", result);

                return result;
            }

            @Override
            public String toString()
            {
                return "WebEvents{sessionId='" + this.sessionId + '\'' + ", eventDays=" + this.eventDays + '}';
            }

            /**
             * This class represents a set of web events of a single day. All events share the same session identifier
             * and are sorted on their timestamp.
             */
            private class EventDay
                    implements Comparable<EventDay>, Iterable<WebEvent>
            {
                /**
                 * The identifier - actually day - of this collection of events.
                 */
                private final LocalDate eventId;
                /**
                 * The user's events of this event day.
                 */
                private final NavigableSet<WebEvent> webEvents = new TreeSet<>();

                /**
                 * Creates a new instance of this class with the specified identifier representing the day that
                 * events must have as timestamp.
                 *
                 * @param eventId the event identifier that represents the day that all events should have as timestamp.
                 */
                public EventDay(final LocalDate eventId)
                {
                    this.eventId = eventId;
                }

                /**
                 * Adds the provided user's web event to this event day object. Note that no check is performed to
                 * make sure that the provided event is at the same day time as this object.
                 * @param webEvent
                 */
                public void add(final WebEvent webEvent)
                {
                    this.webEvents.add(webEvent);
                }

                /**
                 * Applies the incremental difference contained in the events of this object to the provided web
                 * session.
                 *
                 * @param webSession the web session to update with the events contained in this event day object.
                 */
                public void apply(final WebSession webSession)
                {
                    webEvents.forEach(event -> webSession.apply(event));
                }

                @Override
                public int compareTo(final EventDay eventDay)
                {
                    Objects.requireNonNull(eventDay);
                    return this.eventId.compareTo(eventDay.eventId);
                }

                @Override
                public Iterator<WebEvent> iterator()
                {
                    return this.webEvents.iterator();
                }

                @Override
                public String toString()
                {
                    return "EventDay{eventId='" + this.eventId + '\'' + ", webEvents=" + this.webEvents + '}';
                }
            }
        }

        /**
         * A web event representing a event received from the user.
         */
        private class WebEvent
                implements Comparable<WebEvent>
        {
            /**
             * The record representing the user's event.
             */
            private final Record record;
            /**
             * The timestamp to sort web event from.
             */
            private final ZonedDateTime timestamp;

            /**
             * Creates a new instance of this class with the associated parameter.
             *
             * @param record the record representing a user's web event.
             */
            public WebEvent(final Record record)
            {
                this.record = Objects.requireNonNull(record);

                final long epoch = this.record.getField(_TIMESTAMP_FIELD).asLong();
                final Instant instant = Instant.ofEpochMilli(Long.valueOf(epoch));
                this.timestamp = ZonedDateTime.ofInstant(instant, ZoneId.systemDefault());
            }

            /**
             * Returns the timestamp of this event.
             *
             * @return the timestamp of this event.
             */
            private ZonedDateTime getTimestamp()
            {
                return this.timestamp;
            }

            @Override
            public int compareTo(final WebEvent webEvent)
            {
                return this.getTimestamp().compareTo(webEvent.getTimestamp());
            }

            /**
             * Returns the field associated to the specified field's name.
             *
             * @param name the name of the field to return.
             *
             * @return the field associated to the specified field's name.
             */
            public Field getField(final String name)
            {
                return this.record.getField(name);
            }

            @Override
            public String toString()
            {
                return "WebEvent{timestamp=" + this.timestamp.toEpochSecond() + '}';
            }
        }

        /**
         * This class represents a web session that embeds a record. A web session can be created empty or created from
         * existing fields. The web session can be updated with event incrementally.
         */
        private class WebSession
        {
            /**
             * The record actually computed by this processor and returned at the end of the processing.
             */
            private final Record record;

            /**
             * Creates a new instance of this class with the associated parameter.
             *
             * @param sessionId the identifier of this web-session.
             */
            public WebSession(final String sessionId)
            {
                this.record = new StandardRecord(OUTPUT_RECORD_TYPE);
                this.record.setId(sessionId);
                this.record.setField(_SESSION_ID_FIELD, FieldType.STRING, sessionId);
            }

            /**
             *
             * @param sessionId
             * @param fields
             */
            public WebSession(final String sessionId,
                              final Map<String, String> fields)
            {
                this(sessionId);

                fields.forEach((key, value) ->
                               {
                                   if ( _IS_SESSION_ACTIVE_FIELD.equals(key) )
                                   {
                                       // Boolean
                                       this.record.setField(key, FieldType.BOOLEAN, Boolean.valueOf(value));
                                   }
                                   else if ( _SESSION_DURATION_FIELD.equals(key)
                                          || _EVENTS_COUNTER_FIELD.equals(key)
                                          || _TIMESTAMP_FIELD.equals(key)
                                          || _SESSION_INACTIVITY_DURATION_FIELD.equals(key)
                                          || _FIRST_EVENT_EPOCH_FIELD.equals(key)
                                          || _LAST_EVENT_EPOCH_FIELD.equals(key)
                                          || _SESSION_INACTIVITY_DURATION_FIELD.equals(key)
                                          || "record_time".equals(key))
                                   {
                                       // Long
                                       this.record.setField(key, FieldType.LONG, Long.valueOf(value));
                                   }
                                   else
                                   {
                                       // String
                                       this.record.setField(key, FieldType.STRING, value);
                                   }
                               });
            }

            /**
             * Applies incrementally the provided user's event to this web session.
             *
             * @param event the event to apply.
             */
            public void apply(final WebEvent event)
            {
                final Field eventTimestampField = event.getField(_TIMESTAMP_FIELD);
                final long eventTimestamp = eventTimestampField.asLong();

                // Sanity check.
                final Field lastEventField = this.record.getField(_LAST_EVENT_EPOCH_FIELD);
                if ( lastEventField != null )
                {
                    final long lastEvent = lastEventField.asLong();

                    if (lastEvent > 0 && eventTimestamp > 0 && eventTimestamp<lastEvent)
                    {
                        // The event is older that current web session; ignore.
                        return;
                    }
                }

                // EVENTS_COUNTER
                Field field = this.record.getField(_EVENTS_COUNTER_FIELD);
                long eventsCounter = field==null ? 0 : field.asLong();
                eventsCounter++;
                this.record.setField(_EVENTS_COUNTER_FIELD, FieldType.LONG, eventsCounter);

                // TIMESTAMP
                // Set the session create timestamp to the create timestamp of the first event on the session.
                long creationTimestamp;
                field = this.record.getField(_TIMESTAMP_FIELD);
                if ( !isFieldAssigned(field) )
                {
                    this.record.setField(_TIMESTAMP_FIELD, FieldType.LONG, eventTimestamp);
                    creationTimestamp = eventTimestamp;
                }
                else
                {
                    creationTimestamp = field.asLong();
                }

                final Field visitedPage = event.getField(_VISITED_PAGE_FIELD);
                // FIRST_VISITED_PAGE
                if ( !isFieldAssigned(this.record.getField(_FIRST_VISITED_PAGE_FIELD)) )
                {
                    this.record.setField(_FIRST_VISITED_PAGE_FIELD, FieldType.STRING, visitedPage.asString());
                }

                // LAST_VISITED_PAGE
                if ( isFieldAssigned(visitedPage) )
                {
                    this.record.setField(_LAST_VISITED_PAGE_FIELD, FieldType.STRING, visitedPage.asString());
                }

                // FIRST_EVENT_DATETIME
                if ( !isFieldAssigned(this.record.getField(_FIRST_EVENT_DATETIME_FIELD)) )
                {
                    this.record.setField(_FIRST_EVENT_DATETIME_FIELD, FieldType.STRING, toFormattedDate(eventTimestamp));
                    this.record.setField(_FIRST_EVENT_EPOCH_FIELD, FieldType.LONG, eventTimestamp/1000);
                }

                // LAST_EVENT_DATETIME
                if ( isFieldAssigned(eventTimestampField) )
                {
                    this.record.setField(_LAST_EVENT_DATETIME_FIELD, FieldType.STRING, toFormattedDate(eventTimestamp));
                    this.record.setField(_LAST_EVENT_EPOCH_FIELD, FieldType.LONG, eventTimestamp/1000);
                }

                // USERID
                // Add the userid record if available
                if ( !isFieldAssigned(this.record.getField(_USERID_FIELD)) && event.getField(_USERID_FIELD) != null )
                {
                    this.record.setField(_USERID_FIELD, FieldType.STRING, event.getField(_USERID_FIELD).asString());
                }

                LocalDateTime now = LocalDateTime.now();
                LocalDateTime eventLocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(eventTimestamp),
                                                                           ZoneId.systemDefault());

                // Compute session inactivity duration (in milliseconds)
                final long sessionInactivityDuration = Duration.between(eventLocalDateTime, now).getSeconds();

                if ( sessionInactivityDuration > _SESSION_INACTIVITY_TIMEOUT )
                {
                    // Mark the session as closed
                    this.record.setField(_IS_SESSION_ACTIVE_FIELD, FieldType.BOOLEAN, Boolean.FALSE);

                    // Max out the sessionInactivityDuration - only pertinent in case of topic rewind.
                    this.record.setField(_SESSION_INACTIVITY_DURATION_FIELD, FieldType.LONG, _SESSION_INACTIVITY_TIMEOUT);
                }
                else
                {
                    this.record.setField(_IS_SESSION_ACTIVE_FIELD, FieldType.BOOLEAN, Boolean.TRUE);
                }

                final long sessionDuration = Duration.between(Instant.ofEpochMilli(creationTimestamp),
                                                              Instant.ofEpochMilli(eventTimestamp)).getSeconds();
                if ( sessionDuration > 0 )
                {
                    this.record.setField(_SESSION_DURATION_FIELD, FieldType.LONG, sessionDuration);
                }

                if ( (_FIELDS_TO_RETURN != null) && (!_FIELDS_TO_RETURN.isEmpty()) )
                {
                    for(final String fieldnameToAdd: _FIELDS_TO_RETURN.split(","))
                    {
                        field = event.getField(fieldnameToAdd);
                        if ( isFieldAssigned(field) )
                        {
                            this.record.setField(field.getName(), FieldType.STRING, field.asString());
                        }
                    }
                }

                if ( ! record.isValid() )
                {
                    record.getFieldsEntrySet().forEach(entry ->
                                                       {
                                                           final Field f = entry.getValue();
                                                           debug("INVALID field type=%s, class=%s",
                                                                 f.getType(),
                                                                 f.getRawValue().getClass());
                                                       });
                }
            }

            /**
             * Returns a copy of the inner record.
             *
             * @return a copy of the inner record.
             */
            public Record cloneRecord()
            {
                final Record result = new StandardRecord();
                this.record.getFieldsEntrySet()
                           .forEach(entry ->
                                    {
                                        if ( entry.getValue() != null )
                                        {
                                            result.setField(entry.getValue());
                                        }
                                    });

                return new StandardRecord(this.record);
            }

            @Override
            public String toString()
            {
                return "WebSession{record=" + record + '}';
            }
        }
    }
}
