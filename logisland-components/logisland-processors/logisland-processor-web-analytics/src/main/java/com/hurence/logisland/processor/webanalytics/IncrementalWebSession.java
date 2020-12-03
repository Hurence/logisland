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
package com.hurence.logisland.processor.webanalytics;

import com.hurence.logisland.annotation.documentation.*;
import com.hurence.logisland.classloading.PluginProxy;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.ProcessException;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.service.elasticsearch.ElasticsearchClientService;
import com.hurence.logisland.service.datastore.InvalidMultiGetQueryRecordException;
import com.hurence.logisland.service.datastore.MultiGetQueryRecordBuilder;
import com.hurence.logisland.service.datastore.MultiGetResponseRecord;
import com.hurence.logisland.validator.StandardValidators;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
    private static final String EXTRA_SESSION_DELIMITER = "#";

    /**
     * The type of the output record.
     */
    private static final String OUTPUT_RECORD_TYPE = "consolidate-session";

    private static final String PROP_ES_SESSION_INDEX_FIELD = "es.session.index.field";
    private static final String PROP_ES_MAPPING_EVENT_TO_SESSION_INDEX_NAME = "es.mapping.event.to.session.index.name";
    private static final String PROP_ES_SESSION_TYPE_NAME = "es.session.type.name";
    private static final String PROP_ES_EVENT_INDEX_PREFIX = "es.event.index.prefix";
    private static final String PROP_ES_EVENT_TYPE_NAME = "es.event.type.name";

    private static final String ES_MAPPING_EVENT_TO_SESSION_TYPE_NAME = "mapping";
    private static final String MAPPING_FIELD = "sessionId";

    /**
     * Extra fields - for convenience - avoiding to parse the human readable first and last timestamps.
     */
    private static final String _FIRST_EVENT_EPOCH_FIELD = "firstEventEpochSeconds";
    private static final String _LAST_EVENT_EPOCH_FIELD = "lastEventEpochSeconds";

    public static final PropertyDescriptor ELASTICSEARCH_CLIENT_SERVICE =
            new PropertyDescriptor.Builder()
                    .name("elasticsearch.client.service")
                    .description("The instance of the Controller Service to use for accessing Elasticsearch.")
                    .required(true)
                    .identifiesControllerService(ElasticsearchClientService.class)
                    .build();

    private static final PropertyDescriptor DEBUG =
            new PropertyDescriptor.Builder()
                    .name("debug")
                    .description("Enable debug. If enabled, debug information are logged.")
                    .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
                    .required(false)
                    .defaultValue("false")
                    .build();

    static final PropertyDescriptor ES_SESSION_INDEX_FIELD =
            new PropertyDescriptor.Builder()
                    .name(PROP_ES_SESSION_INDEX_FIELD)
                    .description("Name of the field in the record defining the ES index containing the web session documents.")
                    .required(true)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .build();

    static final PropertyDescriptor ES_SESSION_TYPE_NAME =
            new PropertyDescriptor.Builder()
                    .name(PROP_ES_SESSION_TYPE_NAME)
                    .description("Name of the ES type of web session documents.")
                    .required(true)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .build();

    static final PropertyDescriptor ES_MAPPING_EVENT_TO_SESSION_INDEX_NAME =
            new PropertyDescriptor.Builder()
                    .name(PROP_ES_MAPPING_EVENT_TO_SESSION_INDEX_NAME)
                    .description("Name of the ES index containing the mapping of web session documents.")
                    .required(true)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .build();

    static final PropertyDescriptor SESSION_INACTIVITY_TIMEOUT =
            new PropertyDescriptor.Builder()
                    .name("session.timeout")
                    .description("session timeout in sec")
                    .required(false)
                    .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
                    .defaultValue("1800")
                    .build();

    static final PropertyDescriptor SESSION_ID_FIELD =
            new PropertyDescriptor.Builder()
                    .name("sessionid.field")
                    .description("the name of the field containing the session id => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("sessionId")
                    .build();

    static final PropertyDescriptor ES_EVENT_INDEX_PREFIX =
            new PropertyDescriptor.Builder()
                    .name(PROP_ES_EVENT_INDEX_PREFIX)
                    .description("Prefix of the index containing the web event documents.")
                    .required(true)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .build();

    static final PropertyDescriptor ES_EVENT_TYPE_NAME =
            new PropertyDescriptor.Builder()
                    .name(PROP_ES_EVENT_TYPE_NAME)
                    .description("Name of the ES type of web event documents.")
                    .required(true)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .build();

    static final PropertyDescriptor TIMESTAMP_FIELD =
            new PropertyDescriptor.Builder()
                    .name("timestamp.field")
                    .description("the name of the field containing the timestamp => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("h2kTimestamp")
                    .build();

    static final PropertyDescriptor VISITED_PAGE_FIELD =
            new PropertyDescriptor.Builder()
                    .name("visitedpage.field")
                    .description("the name of the field containing the visited page => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("location")
                    .build();

    static final PropertyDescriptor USER_ID_FIELD =
            new PropertyDescriptor.Builder()
                    .name("userid.field")
                    .description("the name of the field containing the userId => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("userId")
                    .build();

    static final PropertyDescriptor FIELDS_TO_RETURN =
            new PropertyDescriptor.Builder()
                    .name("fields.to.return")
                    .description("the list of fields to return")
                    .required(false)
                    .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
                    .build();

    static final PropertyDescriptor FIRST_VISITED_PAGE_FIELD =
            new PropertyDescriptor.Builder()
                    .name("firstVisitedPage.out.field")
                    .description("the name of the field containing the first visited page => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("firstVisitedPage")
                    .build();

    static final PropertyDescriptor LAST_VISITED_PAGE_FIELD =
            new PropertyDescriptor.Builder()
                    .name("lastVisitedPage.out.field")
                    .description("the name of the field containing the last visited page => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("lastVisitedPage")
                    .build();

    static final PropertyDescriptor IS_SESSION_ACTIVE_FIELD =
            new PropertyDescriptor.Builder()
                    .name("isSessionActive.out.field")
                    .description("the name of the field stating whether the session is active or not => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("is_sessionActive")
                    .build();

    static final PropertyDescriptor SESSION_DURATION_FIELD =
            new PropertyDescriptor.Builder()
                    .name("sessionDuration.out.field")
                    .description("the name of the field containing the session duration => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("sessionDuration")
                    .build();

    static final PropertyDescriptor SESSION_INACTIVITY_DURATION_FIELD =
            new PropertyDescriptor.Builder()
                    .name("sessionInactivityDuration.out.field")
                    .description("the name of the field containing the session inactivity duration => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("sessionInactivityDuration")
                    .build();

    static final PropertyDescriptor EVENTS_COUNTER_FIELD =
            new PropertyDescriptor.Builder()
                    .name("eventsCounter.out.field")
                    .description("the name of the field containing the session duration => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("eventsCounter")
                    .build();

    static final PropertyDescriptor FIRST_EVENT_DATETIME_FIELD =
            new PropertyDescriptor.Builder()
                    .name("firstEventDateTime.out.field")
                    .description("the name of the field containing the date of the first event => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("firstEventDateTime")
                    .build();

    static final PropertyDescriptor LAST_EVENT_DATETIME_FIELD =
            new PropertyDescriptor.Builder()
                    .name("lastEventDateTime.out.field")
                    .description("the name of the field containing the date of the last event => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("lastEventDateTime")
                    .build();

    static final PropertyDescriptor NEW_SESSION_REASON_FIELD =
            new PropertyDescriptor.Builder()
                    .name("newSessionReason.out.field")
                    .description("the name of the field containing the reason why a new session was created => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("reasonForNewSession")
                    .build();

    static final PropertyDescriptor TRANSACTION_IDS =
            new PropertyDescriptor.Builder()
                    .name("transactionIds.out.field")
                    .description("the name of the field containing all transactionIds => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("transactionIds")
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
    private static final String DIRECT_TRAFFIC = "direct";

    private final String FLAT_SEPARATOR = "_";
    private static final PropertyDescriptor SOURCE_OF_TRAFFIC_PREFIX_FIELD =
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

    /**
     * The elasticsearch service.
     */
    private ElasticsearchClientService elasticsearchClientService;

    @Override
    public boolean hasControllerService()
    {
        return true;
    }

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
    }

    @Override
    public Collection<Record> process(final ProcessContext context,
                                      final Collection<Record> records)
        throws ProcessException
    {
//        debug("Entering processor with %s ", records);
        return new Execution(context).process(records);
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
     * The legacy format used in Date.toString().
     */
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss zzz yyyy",
                                                                                     Locale.ENGLISH);

    /**
     * Returns the epoch timestamp corresponding to the specified value parsed with the default formatter.
     *
     * @param string the value to parse with the default formatter.
     *
     * @return the epoch timestamp corresponding to the specified value parsed with the default formatter.
     */
    static long toEpoch(final String string)
    {
        return LocalDateTime.parse(string, DATE_FORMAT)
                            .atZone(ZoneId.systemDefault())
                            .toInstant()
                            .toEpochMilli();
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
                getLogger().debug(format);
            }
            else
            {
                getLogger().debug(String.format(format + "\n", args));
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
     * This interface defines the result of a check a of session against an event.
     * If the result is valid then the reason is empty; otherwise the reason contains a description of why the check
     * is not valid.
     */
    interface SessionCheckResult
    {
        /**
         * Returns {@code true} is the event is applicable to the session incrementally, {@code false} otherwise.
         * If {@code false} is returned then a new session must be created from the provided event and the provided
         * session closed.
         *
         * @return {@code true} is the event is applicable to the session, {@code false} otherwise.
         */
        boolean isValid();

        /**
         * The reason why the check is not valid.
         *
         * @return the reason why the check is not valid.
         */
        String reason();
    }

    /**
     * A singleton for valid check.
     */
    private final static SessionCheckResult VALID = new SessionCheckResult()
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

    /**
     * This interface defines rules to test whether an event can be applied to a session or not. In case it can not
     * be applied then a new session must be created.
     */
    interface SessionCheck
    {
        /**
         * Returns {@code true} is the event is applicable to the session incrementally, {@code false} otherwise.
         * If {@code false} is returned then a new session must be created from the provided event and the provided
         * session close.
         *
         * @param session the session to apply the event onto.
         * @param event   the event to apply to the session.
         * @return {@code true} is the event is applicable to the session, {@code false} otherwise.
         */
        SessionCheckResult isValid(Execution.WebSession session, Execution.WebEvent event);
    }

    /**
     * A class that performs a unique execution.
     */
    private class Execution
    {
        /*
         * Field names.
         */
        private final long _SESSION_INACTIVITY_TIMEOUT;
        private final String _SESSION_ID_FIELD;
        private final String _TIMESTAMP_FIELD;
        private final String _VISITED_PAGE_FIELD;
        private final Collection<String> _FIELDS_TO_RETURN;
        private final String _USERID_FIELD;
        private final String _FIRST_VISITED_PAGE_FIELD;
        private final String _LAST_VISITED_PAGE_FIELD;
        private final String _IS_SESSION_ACTIVE_FIELD;
        private final String _SESSION_DURATION_FIELD;
        private final String _EVENTS_COUNTER_FIELD;
        private final String _FIRST_EVENT_DATETIME_FIELD;
        private final String _LAST_EVENT_DATETIME_FIELD;
        private final String _SESSION_INACTIVITY_DURATION_FIELD;
        private final String _NEW_SESSION_REASON_FIELD;
        private final String _TRANSACTION_IDS;
        private final String _SOT_SOURCE_FIELD;
        private final String _SOT_CAMPAIGN_FIELD;
        private final String _SOT_MEDIUM_FIELD;
        private final String _SOT_CONTENT_FIELD;
        private final String _SOT_KEYWORD_FIELD;

        private final String _ES_SESSION_INDEX_FIELD;
        private final String _ES_SESSION_TYPE_NAME;
        private final String _ES_EVENT_INDEX_PREFIX;
        private final String _ES_EVENT_TYPE_NAME;
        private final String _ES_MAPPING_EVENT_TO_SESSION_INDEX_NAME;

        private final Collection<SessionCheck> checker;

        private Execution(final ProcessContext context)
        {
            this._SESSION_INACTIVITY_TIMEOUT = context.getPropertyValue(SESSION_INACTIVITY_TIMEOUT).asLong();
            this._SESSION_ID_FIELD = context.getPropertyValue(SESSION_ID_FIELD).asString();
            this._TIMESTAMP_FIELD = context.getPropertyValue(TIMESTAMP_FIELD).asString();
            this._VISITED_PAGE_FIELD = context.getPropertyValue(VISITED_PAGE_FIELD).asString();

            String fieldsToReturn = context.getPropertyValue(FIELDS_TO_RETURN).asString();
            if ( fieldsToReturn != null && !fieldsToReturn.isEmpty() )
            {
                this._FIELDS_TO_RETURN = Arrays.asList(fieldsToReturn.split(","));
            }
            else
            {
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

                    if ( _DEBUG && !isValid )
                    {
                        debug("'Day overlap' isValid=" + isValid + " session-id=" + session.getSessionId());
                    }

                    return isValid? VALID : DAY_OVERLAP;
                },

                // Timeout exceeded
                (session, event) ->
                {
                    final long durationInSeconds = Duration.between(session.getLastEvent(), event.getTimestamp())
                                                           .getSeconds();
                    boolean isValid = durationInSeconds <= this._SESSION_INACTIVITY_TIMEOUT;

                    if ( _DEBUG && !isValid )
                    {
                        debug("'Timeout exceeded' isValid=" + isValid + " seconds=" + durationInSeconds +
                              " timeout=" + this._SESSION_INACTIVITY_TIMEOUT + " session-id=" + session.getSessionId());
                    }

                    return isValid? VALID : SESSION_TIMEDOUT;
                },

                // One Campaign Per Session—Each visit to your site from a different campaign—organic or paid—triggers a
                // new session, regardless of the actual time elapsed in the current session.
                (session, event) ->
                {
                    boolean isValid = Objects.equals(event.getValue(_SOT_SOURCE_FIELD), DIRECT_TRAFFIC) ||
                                      Objects.deepEquals(session.getSourceOfTraffic(), event.getSourceOfTraffic());

                    if ( _DEBUG && !isValid )
                    {
                        debug("'Fields of traffic' isValid=" + isValid + " session-id=" + session.getSessionId());
                    }

                    return isValid? VALID : SOURCE_OF_TRAFFIC;
                });
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
         *
         * @return the name of the event index corresponding to the specified date.
         */
        private String toEventIndexName(final ZonedDateTime date)
        {
            return _ES_EVENT_INDEX_PREFIX + "." + EVENT_SUFFIX_FORMATTER.format(date);
        }

        /**
         * Processes the incoming records and returns their result.
         *
         * @param records the records to process.
         *
         * @return the result of the processing of the incoming records.
         *
         * @throws ProcessException if something went wrong.
         */
        public Collection<Record> process(final Collection<Record> records)
            throws ProcessException
        {
//            debug("\n\n--------------------------------------------------------------------");
//            debug("Starting processing on %s '%s' %s. Incoming records size=%d ",
//                  ManagementFactory.getRuntimeMXBean().getName(),
//                  Thread.currentThread().getName(),
//                  Thread.currentThread().getId(),
//                  records.size());
//            records.forEach(record -> debug(record.getId()));

            // Convert records to web-events grouped by session-id. Indeed each instance of Events contains a list of
            // sorted web-event grouped by session identifiers.
            final Collection<Events> events = toWebEvents(records);

            //
            final Collection<Sessions> rewriters = this.processEvents(events);

            // Store all events to elasticsearch through a bulk processor.
            events.stream()
                  .flatMap(event -> event.stream())
                  .forEach(event ->
                  {
                      final Map<String, Object> map = toMap(event.cloneRecord());

                      elasticsearchClientService.bulkPut(toEventIndexName(event.getTimestamp()),
                              _ES_EVENT_TYPE_NAME,
                                                         map,
                                                         Optional.of((String)map.get(FieldDictionary.RECORD_ID)));
                  });
            elasticsearchClientService.bulkFlush();

            // Convert all created sessions to records.
            final Collection<Record> result = rewriters.stream()
                                                       .flatMap(rewriter -> rewriter.getSessions().stream())
                                                       .collect(Collectors.toList());

            // Save last sessionId in mapping index.
            // <sessionId> -> <sessionId>#?
            rewriters.stream()
                     .forEach(sessions ->
                elasticsearchClientService.bulkPut(_ES_MAPPING_EVENT_TO_SESSION_INDEX_NAME, ES_MAPPING_EVENT_TO_SESSION_TYPE_NAME,
                                                   Collections.singletonMap(MAPPING_FIELD, sessions.getLastSessionId()),
                                                   Optional.of(sessions.getSessionId())));



            debug("Processing done. Outcoming records size=%d ", result.size());

            return result;
        }

        /**
         * Returns a concatenation of the form ${utmSource}:${utmMedium}:${utmCampaign}:${utmTerm}:${utmContent} with
         * the provided parameters.
         *
         * @param utmSource the utm source
         * @param utmMedium the medium source
         * @param utmCampaign the campaign source
         * @param utmTerm the utm term
         * @param utmContent the utm content
         *
         * @return a concatenation of the form ${utmSource}:${utmMedium}:${utmCampaign}:${utmTerm}:${utmContent}
         *         with the provided parameters.
         */
        private String concatFieldsOfTraffic(final String utmSource,
                                             final String utmMedium,
                                             final String utmCampaign,
                                             final String utmTerm,
                                             final String utmContent)
        {
            return new StringBuilder().append(utmSource==null?"":utmSource).append(':')
                                      .append(utmMedium==null?"":utmMedium).append(':')
                                      .append(utmCampaign==null?"":utmCampaign).append(':')
                                      .append(utmTerm==null?"":utmTerm).append(':')
                                      .append(utmContent==null?"":utmContent).toString();
        }

        /**
         * Returns the provided records as a collection of Events instances.
         * Provided records are grouped by session identifier and their remaining collection wrapped in Events
         * instances.
         *
         * @param records a collection of records representing web-events.
         *
         * @return the provided records as a collection of Events instances.
         */
        private Collection<Events> toWebEvents(final Collection<Record> records)
        {
            // Create webEvents from input records.
            // A webEvents contains all webEvent instances of its session id.
            final Collection<Events> result =
                    records.stream()
                           // Remove record without session Id or timestamp.
                           .filter(record -> isFieldAssigned(record.getField(_SESSION_ID_FIELD))
                                          && isFieldAssigned(record.getField(_TIMESTAMP_FIELD)))
                           // Create web-event from record.
                           .map(WebEvent::new)
                           // Group records per session Id.
                           .collect(Collectors.groupingBy(WebEvent::getSessionId))
                           // Ignore keys (sessionId) and stream over list of associated events.
                           .values()
                           .stream()
                           // Wrapped grouped web-events of sessionId in WebEvents.
                           .map(Events::new)
                           .collect(Collectors.toList());

            return result;
        }

        /**
         * Processes the provided events and returns their resulting sessions.
         * All sessions are retrieved from elasticsearch and then updated with the specified web events.
         *
         * @param webEvents the web events to process.
         *
         * @return web sessions resulting of the processing of the web events.
         */
        private Collection<Sessions> processEvents(final Collection<Events> webEvents)
        {
            // First retrieve mapping of last sessions.
            // Eg sessionId -> sessionId#XX
            final MultiGetQueryRecordBuilder mgqrBuilder = new MultiGetQueryRecordBuilder();
            webEvents.forEach(events -> mgqrBuilder.add(_ES_MAPPING_EVENT_TO_SESSION_INDEX_NAME, ES_MAPPING_EVENT_TO_SESSION_TYPE_NAME,
                                                        null, events.getSessionId()));

            List<MultiGetResponseRecord> esResponse = null;
            try
            {
                esResponse = elasticsearchClientService.multiGet(mgqrBuilder.build());
            }
            catch (final InvalidMultiGetQueryRecordException e)
            {
                // should never happen
                getLogger().error("error while executing multiGet elasticsearch", e);
            }

            // Documents have only one field "sessionId" that corresponds to last session.
            Map<String/*sessionId*/, String/*sessionId#<last>*/> _mappings = Collections.emptyMap();
            if ( ! esResponse.isEmpty() )
            {
                _mappings =
                     esResponse.stream()
                               .collect(Collectors.toMap(response -> response.getDocumentId(),
                                                         response -> response.getRetrievedFields().get(MAPPING_FIELD)));
            }

            final Map<String/*sessionId*/, String/*sessionId#<last>*/> mappings = _mappings;

            // Retrieve all last sessionId from elasticsearch.
            final MultiGetQueryRecordBuilder sessionBuilder = new MultiGetQueryRecordBuilder();
            if ( ! mappings.isEmpty() )
            {
                webEvents.forEach(events ->
                {
                    String sessionId = events.getSessionId();
                    String mappedSessionId = mappings.get(sessionId); // last processed session id (#?)
                    // Retrieve the name of the index that contains the websessions.
                    // The chaining calls are on purpose as any NPE would mean something is wrong.
                    final String sessionIndexName = events.first().getValue(_ES_SESSION_INDEX_FIELD).toString();
                    sessionBuilder.add(sessionIndexName, _ES_SESSION_TYPE_NAME,
                                       null, mappedSessionId!=null?mappedSessionId:sessionId);
                });
            }

            esResponse = null;
            try
            {
                esResponse = elasticsearchClientService.multiGet(sessionBuilder.build());
            }
            catch (final InvalidMultiGetQueryRecordException e)
            {
                // should never happen
                getLogger().error("error while executing multiGet elasticsearch", e);
            }

            debug("Retrieved %d documents from elasticsearch.", esResponse.size());

            // Grouped all retrieved elasticsearch documents by their session identifier.
            final Map<String/*doc id without #*/,
                      List<WebSession>/*session-id or session-id#0,...,session-id#N*/> sessionDocs  =
                esResponse.isEmpty()
                        ? Collections.emptyMap()
                        : esResponse.stream()
                                    .map(response -> new WebSession(esDoc2Record(response.getRetrievedFields())))
                                    .collect(Collectors.groupingBy(record-> record.getSessionId()
                                                                                  .split(EXTRA_SESSION_DELIMITER)[0]));

            // Applies all events to session documents and collect results.
            final Collection<Sessions> result =
                    webEvents.stream()
                             .map(events -> new Sessions(events.getSessionId(),
                                                         sessionDocs.get(events.getSessionId())).processEvents(events))
                             .collect(Collectors.toList());

            return result;
        }

        /**
         * Returns a new record based on the specified map that represents a web session in elasticsearch.
         *
         * @param sourceAsMap the web session stored in elasticsearch.
         *
         * @return a new record based on the specified map that represents a web session in elasticsearch.
         */
        public Record esDoc2Record(final Map<String, String> sourceAsMap)
        {
            final Record record = new StandardRecord(OUTPUT_RECORD_TYPE);
            sourceAsMap.forEach((key, value) ->
                                {
                                    if ( _IS_SESSION_ACTIVE_FIELD.equals(key) )
                                    {
                                        // Boolean
                                        record.setField(key, FieldType.BOOLEAN, Boolean.valueOf(value));
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
                                        record.setField(key, FieldType.LONG, Long.valueOf(value));
                                    }
                                    else
                                    {
                                        // String
                                        record.setField(key, FieldType.STRING, value);
                                    }
                                });

            record.setId(record.getField(_SESSION_ID_FIELD).asString());

            return record;
        }

        /**
         * Returns {@code true} if the specified web-event checked against the provided web-session is valid;
         * {@code false} otherwise.
         * In case the returned value is {@code false} then a new session must be created.
         *
         * @param webSession the web-session to check the web-event against.
         * @param webEvent the web-event to validate.
         *
         * @return {@code true} if the specified web-event checked against the provided web-session is valid;
         *         {@code false} otherwise.
         */
        private SessionCheckResult isEventApplicable(final WebSession webSession,
                                                     final WebEvent webEvent)
        {
            SessionCheckResult result = VALID;
            for (final SessionCheck check : checker)
            {
                result = check.isValid(webSession, webEvent);
                if (!result.isValid())
                {
                    break;
                }
            }

            return result;
        }

        /**
         * This class represents one or more sessions resulting of the processing of web events.
         */
        private class Sessions
        {
            private final String sessionId;

            // Last processed web session retrieved from datastore.
            private final WebSession lastSession;

            // The resulting sessions from the processed web events.
            // MAKE SURE LAST SESSION IS AT LAST POSITION!!!
            private final List<WebSession> processedSessions = new ArrayList<>();
            private long eventCount;

            /**
             * Create a new instance of this class.
             * The provided web sessions are sorted chronologically and only the last one is used as starting point to
             * process all web events.
             *
             * @param storedSessions the sessions stored in the datastore.
             */
            public Sessions(final String sessionId,
                            final Collection<WebSession> storedSessions)
            {
                this.sessionId = sessionId;

                this.lastSession = storedSessions==null ? null : Collections.max(storedSessions);

                if ( _DEBUG )
                {
                    debug("storedSessions=" +
                          (storedSessions == null ? null :
                                                    storedSessions.stream()
                                                                  .map(webSession -> webSession.record.getId())
                                                                  .collect(Collectors.joining(" "))) +
                          ", last=" + (lastSession == null ? null : lastSession.record.getId()));
                }
            }

            /**
             * Returns the session identifier of this session.
             *
             * @return the session identifier of this session.
             */
            public String getSessionId()
            {
                return this.sessionId;
            }

            /**
             * Processes the provided events against the first session (if any).
             *
             * @param events the events to process.
             *
             * @return this object for convenience.
             */
            public Sessions processEvents(final Events events)
            {
                debug("Applying %d events to session '%s'", events.size(), events.getSessionId());

                if ( this.lastSession != null )
                {
                    // One or more sessions were already stored in datastore.
                    final Iterator<WebEvent> eventIterator = events.iterator();

                    WebEvent event = null;
                    boolean outsideTimeWindow = false;
                    // Skip all events that have their timestamp in the range of the [first, last] timestamps of the
                    // web session. This happens in case the kafka topic was re-read from earliest than the last
                    // processed messages.
                    while (eventIterator.hasNext())
                    {
                        event = eventIterator.next();
                        outsideTimeWindow = !lastSession.containsTimestamp(event.timestamp);
                        if (outsideTimeWindow)
                        {
                            break;
                        }
                    }

                    if (outsideTimeWindow)
                    {
                        // Event iterator points to first event outside of session's time window.
                        // Recreates a list from the first event outside of the time window included.
                        final Events nextEvents = new Events(events.tailSet(event));

                        final String sessionIdOfCurrentSession = this.lastSession.getSessionId();
                        nextEvents.forEach(toRename -> toRename.rename(sessionIdOfCurrentSession));

                        // Resume from first session.
                        this.processEvents(lastSession, nextEvents);
                    }
                }
                else
                {
                    // No web session yet exists for this session identifier. Create a new one.
                    this.processEvents(null, events);
                }

                return this;
            }

            /**
             * Processes the provided events against the provided session (can be {@code null}).
             *
             * @param session the session to update. If {@code null} is provided a new session is created.
             * @param events the events to process.
             *
             * @return this object for convenience.
             */
            private void processEvents(WebSession session,
                                       final Events events)
            {
                if (events.isEmpty())
                {
                    // No event. Paranoid.
                    return;
                }

                final Iterator<WebEvent> iterator = events.iterator();
                debug("Processing event sessionId="+events.getSessionId() + " eventCount="+eventCount);

                if (session == null)
                {
                    // No web-session yet in datastore.
                    WebEvent event = iterator.next();
                    eventCount++;
                    session = new WebSession(event);
                    session.add(event);
                }

                this.processedSessions.add(session);

                while (iterator.hasNext())
                {
                    final WebEvent event = iterator.next();
                    eventCount++;

                    final SessionCheckResult isSessionValid = isEventApplicable(session, event);

                    if ( isSessionValid.isValid() )
                    {
                        // No invalid check found.
                        session.add(event);
                    }
                    else
                    {
                        // Invalid check found:
                        // 1. keep current web-session untouched (and save it)
                        // 2. create a new web-session from the current web-event and rename/increase session-id.
                        final String[] oldSessionId = event.getSessionId().split(EXTRA_SESSION_DELIMITER);
                        final int index = (oldSessionId.length == 1) ? 2 // only one web session so far => create 2nd one
                                                  : Integer.valueOf(oldSessionId[1]) + 1; // +1 on web session
                        final String newSessionId = oldSessionId[0] + EXTRA_SESSION_DELIMITER + index;
                        final Collection<WebEvent> renamedEvents = events.tailSet(event);
                        // Rewrite all remaining web-events with new session identifier.
                        renamedEvents.forEach(toRename -> toRename.rename(newSessionId));
                        // Mark event that triggered the new sessions with the reason.
                        event.record.setField(_NEW_SESSION_REASON_FIELD, FieldType.STRING, isSessionValid.reason());

                        final Events nextEvents = new Events(renamedEvents);

                        this.processEvents(null/*force new web-session*/, nextEvents);
                        break;
                    }
                }
            }

            /**
             * Returns the processed sessions as records.
             *
             * @return the processed sessions as records.
             */
            public Collection<Record> getSessions()
            {
                return processedSessions.stream()
                                        .map(item -> item.record)
                                        .collect(Collectors.toSet());
            }

            /**
             * Returns the last sessionId (#?) of this session container.
             *
             * @return the last sessionId (#?) of this session container.
             */
            public String getLastSessionId()
            {
                String result = this.sessionId;

                if ( !this.processedSessions.isEmpty() )
                {
                    result = this.processedSessions.get(this.processedSessions.size()-1).getSessionId();
                }
                else {
                    getLogger().error("Invalid state: session container for '" + this.sessionId + "' is empty. " +
                              "At least one session is expected");
                }

                return result;
            }
        }

        /**
         * This class represents a collection of events and is provided for convenience.
         */
        private class Events
                implements SortedSet<WebEvent>
        {
            private final SortedSet<WebEvent> set;

            public Events(Collection<WebEvent> events)
            {
                this.set = new TreeSet<>(events);
            }

            public String getSessionId()
            {
                return this.first().getSessionId();
            }

            @Override
            public Comparator<? super WebEvent> comparator()
            {
                return this.set.comparator();
            }

            @Override
            public SortedSet<WebEvent> subSet(WebEvent fromElement, WebEvent toElement)
            {
                return this.set.subSet(fromElement, toElement);
            }

            @Override
            public SortedSet<WebEvent> headSet(WebEvent toElement)
            {
                return this.set.headSet(toElement);
            }

            @Override
            public SortedSet<WebEvent> tailSet(WebEvent fromElement)
            {
                return this.set.tailSet(fromElement);
            }

            @Override
            public WebEvent first()
            {
                return this.set.first();
            }

            @Override
            public WebEvent last()
            {
                return this.set.last();
            }

            @Override
            public int size()
            {
                return this.set.size();
            }

            @Override
            public boolean isEmpty()
            {
                return this.set.isEmpty();
            }

            @Override
            public boolean contains(Object o)
            {
                return this.set.contains(o);
            }

            @Override
            public Iterator<WebEvent> iterator()
            {
                return this.set.iterator();
            }

            @Override
            public Object[] toArray()
            {
                return this.set.toArray();
            }

            @Override
            public <T> T[] toArray(T[] a)
            {
                return this.set.toArray(a);
            }

            @Override
            public boolean add(WebEvent t)
            {
                return this.set.add(t);
            }

            @Override
            public boolean remove(Object o)
            {
                return this.set.remove(o);
            }

            @Override
            public boolean containsAll(Collection<?> c)
            {
                return this.set.containsAll(c);
            }

            @Override
            public boolean addAll(Collection<? extends WebEvent> c)
            {
                return this.set.addAll(c);
            }

            @Override
            public boolean retainAll(Collection<?> c)
            {
                return this.set.retainAll(c);
            }

            @Override
            public boolean removeAll(Collection<?> c)
            {
                return this.set.removeAll(c);
            }

            @Override
            public void clear()
            {
                this.set.clear();
            }
        }

        /**
         * This class represents a session which can be created from a given web-event or an existing session
         * represented by a record.
         */
        private class WebSession
                extends RecordItem
                implements Comparable<WebSession>
        {
            /**
             * Creates a new instance of this class with:
             * - the session identifier set from the web event's session identifier
             * - the first and last timestamps set from the web event's timestamp.
             *
             * @param webEvent the web event to fetch information from.
             */
            public WebSession(final WebEvent webEvent)
            {
                super(new StandardRecord(OUTPUT_RECORD_TYPE));
                this.record.setId(webEvent.getSessionId());
                this.record.setField(_SESSION_ID_FIELD, FieldType.STRING, webEvent.getSessionId());

                final long eventTimestamp = webEvent.record.getField(_TIMESTAMP_FIELD).asLong();
                this.setFirstEvent(eventTimestamp);
                this.setLastEvent(eventTimestamp);

                if ( (_FIELDS_TO_RETURN != null) && (!_FIELDS_TO_RETURN.isEmpty()) )
                {
                    for(final String fieldnameToAdd: _FIELDS_TO_RETURN)
                    {
                        final Field field = webEvent.record.getField(fieldnameToAdd);
                        if ( isFieldAssigned(field) )
                        {
                            record.setField(field); // Field immutable.
                        }
                    }
                }
            }

            /**
             * Creates a new instance of this class that wraps the provided record.
             *
             * @param saveSession the embedded record.
             */
            public WebSession(final Record saveSession)
            {
                super(saveSession);
            }

            /**
             * Adds the specified event to this sessions by updating fields such as lastVisitedPage.
             *
             * @param event the event to apply.
             */
            public void add(final WebEvent event)
            {
                // Handle case where web-event is older that first event of session.
                // In case there are few events older than the current web-session, all those events must
                // be taken into account despite the fact that setting the timestamp of the first event
                // will 'hide' the next ones.

                final Field eventTimestampField = event.record.getField(_TIMESTAMP_FIELD);
                final long eventTimestamp = eventTimestampField.asLong();

                // Sanity check.
                final Field lastEventField = record.getField(_LAST_EVENT_EPOCH_FIELD);
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
                Field field = record.getField(_EVENTS_COUNTER_FIELD);
                long eventsCounter = field==null ? 0 : field.asLong();
                eventsCounter++;
                record.setField(_EVENTS_COUNTER_FIELD, FieldType.LONG, eventsCounter);

                // TIMESTAMP
                // Set the session create timestamp to the create timestamp of the first event on the session.
                long creationTimestamp;
                field = record.getField(_TIMESTAMP_FIELD);
                if ( !isFieldAssigned(field) )
                {
                    record.setField(_TIMESTAMP_FIELD, FieldType.LONG, eventTimestamp);
                    creationTimestamp = eventTimestamp;
                }
                else
                {
                    creationTimestamp = field.asLong();
                }

                final Field visitedPage = event.record.getField(_VISITED_PAGE_FIELD);
                // FIRST_VISITED_PAGE
                if ( !isFieldAssigned(record.getField(_FIRST_VISITED_PAGE_FIELD)) )
                {
                    record.setField(_FIRST_VISITED_PAGE_FIELD, FieldType.STRING, visitedPage.asString());
                }

                // LAST_VISITED_PAGE
                if ( isFieldAssigned(visitedPage) )
                {
                    record.setField(_LAST_VISITED_PAGE_FIELD, FieldType.STRING, visitedPage.asString());
                }

                // FIRST_EVENT_DATETIME
                if ( !isFieldAssigned(record.getField(_FIRST_EVENT_DATETIME_FIELD)) )
                {
                    this.setFirstEvent(eventTimestamp);
                }

                // LAST_EVENT_DATETIME
                if ( isFieldAssigned(eventTimestampField) )
                {
                    this.setLastEvent(eventTimestamp);
                }

                // USERID
                // Add the userid record if available
                final Field userIdField = record.getField(_USERID_FIELD);
                if ( (!isFieldAssigned(userIdField) || "undefined".equalsIgnoreCase(userIdField.asString()))
                  && isFieldAssigned(event.record.getField(_USERID_FIELD)) )
                {
                    final String userId = event.record.getField(_USERID_FIELD).asString();
                    if ( userId != null )
                    {
                        record.setField(_USERID_FIELD, FieldType.STRING, userId);
                    }
                }

                LocalDateTime now = LocalDateTime.now();
                LocalDateTime eventLocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(eventTimestamp),
                                                                           ZoneId.systemDefault());

                // Compute session inactivity duration (in milliseconds)
                final long sessionInactivityDuration = Duration.between(eventLocalDateTime, now).getSeconds();

                if ( sessionInactivityDuration > _SESSION_INACTIVITY_TIMEOUT )
                {
                    // Mark the session as closed
                    record.setField(_IS_SESSION_ACTIVE_FIELD, FieldType.BOOLEAN, Boolean.FALSE);

                    // Max out the sessionInactivityDuration - only pertinent in case of topic rewind.
                    record.setField(_SESSION_INACTIVITY_DURATION_FIELD, FieldType.LONG, _SESSION_INACTIVITY_TIMEOUT);
                }
                else
                {
                    record.setField(_IS_SESSION_ACTIVE_FIELD, FieldType.BOOLEAN, Boolean.TRUE);
                }

                final long sessionDuration = Duration.between(Instant.ofEpochMilli(creationTimestamp),
                                                              Instant.ofEpochMilli(eventTimestamp)).getSeconds();
                if ( sessionDuration > 0 )
                {
                    record.setField(_SESSION_DURATION_FIELD, FieldType.LONG, sessionDuration);
                }

                // Extra
                final Field transactionIdField = event.record.getField("transactionId");
                if ( isFieldAssigned(transactionIdField)
                  && (!"undefined".equalsIgnoreCase(transactionIdField.asString()))
                  && (!transactionIdField.asString().isEmpty()) )
                {
                    final Field transactionIdsField = this.record.getField("transactionIds");
                    Collection<String> transactionIds;
                    if ( !isFieldAssigned(transactionIdField) )
                    {
                        transactionIds = (Collection<String>)transactionIdsField.getRawValue();
                    }
                    else
                    {
                        transactionIds = new ArrayList<>();
                        this.record.setField(_TRANSACTION_IDS, FieldType.ARRAY, transactionIds);
                    }
                    transactionIds.add(transactionIdField.asString());
                }

                if ( ! record.isValid() )
                {
                    record.getFieldsEntrySet().forEach(entry ->
                    {
                        final Field f = entry.getValue();
                        debug("INVALID field type=%s, class=%s", f.getType(), f.getRawValue().getClass());
                    });
                }
            }

            /**
             * Returns {@code true} if the specified timestamp is enclosed within the first and last timestamp of this
             * session; {@code false} otherwise.
             *
             * @param timestamp the timestamp to check against this session.
             *
             * @return {@code true} if the specified timestamp is enclosed within the first and last timestamp of this
             *         session; {@code false} otherwise.
             */
            public boolean containsTimestamp(final ZonedDateTime timestamp)
            {
                return this.getFirstEvent().compareTo(timestamp) <= 0 && timestamp.compareTo(this.getLastEvent()) <= 0;
            }

            @Override
            public int compareTo(final WebSession session)
            {
                if ( this.getLastEvent().compareTo(session.getFirstEvent()) < 0)
                {
                    return -1;
                }
                else if ( session.getLastEvent().compareTo(this.getFirstEvent()) < 0)
                {
                    return 1;
                }
                else
                {
                    throw new IllegalStateException("Two sessions can no share same timestamp:" + this.toString()
                    +" vs " + session.toString());
                }
            }

            public ZonedDateTime getFirstEvent()
            {
                final Field field = record.getField(_FIRST_EVENT_EPOCH_FIELD);
                if ( field == null )
                {
                    // Fallback by parsing the equivalent human readable field.
                    return fromEpoch(toEpoch(record.getField(_FIRST_EVENT_DATETIME_FIELD).asString()));
                }
                return fromEpoch(field.asLong()*1000);
            }

            private void setFirstEvent(final long eventTimestamp)
            {
                this.record.setField(_FIRST_EVENT_DATETIME_FIELD, FieldType.STRING, toFormattedDate(eventTimestamp));
                this.record.setField(_FIRST_EVENT_EPOCH_FIELD, FieldType.LONG, eventTimestamp/1000);
            }

            private void setLastEvent(final long eventTimestamp)
            {
                this.record.setField(_LAST_EVENT_DATETIME_FIELD, FieldType.STRING, toFormattedDate(eventTimestamp));
                this.record.setField(_LAST_EVENT_EPOCH_FIELD, FieldType.LONG, eventTimestamp/1000);
            }

            public ZonedDateTime getLastEvent()
            {
                final Field field = record.getField(_LAST_EVENT_EPOCH_FIELD);
                if ( field == null )
                {
                    // Fallback by parsing the equivalent human readable field.
                    return fromEpoch(toEpoch(record.getField(_LAST_EVENT_DATETIME_FIELD).asString()));
                }
                return fromEpoch(field.asLong()*1000);
            }

            public String getSourceOfTraffic()
            {
                return concatFieldsOfTraffic((String)this.getValue(_SOT_SOURCE_FIELD),
                                             (String)this.getValue(_SOT_MEDIUM_FIELD),
                                             (String)this.getValue(_SOT_CAMPAIGN_FIELD),
                                             (String)this.getValue(_SOT_KEYWORD_FIELD),
                                             (String)this.getValue(_SOT_CONTENT_FIELD));
            }

            @Override
            public String toString()
            {
                return "WebSession{" + record.getField(_FIRST_EVENT_DATETIME_FIELD).asString() +
                       "-" + record.getField(_LAST_EVENT_DATETIME_FIELD).asString() + "}";
            }
        }

        /**
         * This class represents a web event that can be optionally renamed if a new session should have
         * been created.
         */
        private class WebEvent
                extends RecordItem
                implements Comparable<WebEvent>
        {
            private String sessionId;

            /**
             * The timestamp to sort web event from.
             */
            private final ZonedDateTime timestamp;

            public WebEvent(final Record record)
            {
                super(record);
                this.timestamp = this.fromEpoch(record.getField(_TIMESTAMP_FIELD).asLong());
            }

            @Override
            public int compareTo(final WebEvent webEvent)
            {
                return this.timestamp.compareTo(webEvent.getTimestamp());
            }

            /**
             * Returns the timestamp of this event.
             *
             * @return the timestamp of this event.
             */
            public ZonedDateTime getTimestamp()
            {
                return this.timestamp;
            }

            public String getVisitedPage()
            {
                return record.getField(_VISITED_PAGE_FIELD).asString();
            }

            @Override
            public String getSessionId()
            {
                return this.sessionId==null?super.getSessionId():this.sessionId;
            }

            public void rename(final String sessionId)
            {
                debug("Rename " + this.record.getId() + " from " + super.getSessionId() + " to " + sessionId);
                this.sessionId = sessionId;
                this.record.setField("originalSessionId", FieldType.STRING, super.getSessionId());
            }

            public String getSourceOfTraffic()
            {
                return concatFieldsOfTraffic((String)this.getValue(_SOT_SOURCE_FIELD),
                                             (String)this.getValue(_SOT_MEDIUM_FIELD),
                                             (String)this.getValue(_SOT_CAMPAIGN_FIELD),
                                             (String)this.getValue(_SOT_KEYWORD_FIELD),
                                             (String)this.getValue(_SOT_CONTENT_FIELD));
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

                result.setField(_SESSION_ID_FIELD, FieldType.STRING, this.getSessionId());

                return result;
            }

            @Override
            public String toString()
            {
                return "WebEvent{sessionId='" + this.getSessionId() + "', timestamp=" + timestamp + '}';
            }
        }

        /**
         * This class is a basic WebItem that wraps an inner record.
         */
        private class RecordItem
        {
            /**
             * The record actually computed by this processor and returned at the end of the processing.
             */
            final Record record;

            /**
             * Creates a new instance of this class with the associated parameter.
             *
             * @param record the wrapped record.
             */
            public RecordItem(final Record record)
            {
                this.record = record;
            }

            public String getSessionId()
            {
                return (String) this.getValue(_SESSION_ID_FIELD);
            }

            /**
             * Returns the value of the specified field name. {@code null} is returned if the field does not exists or
             * if the value of the field is {@code null}.
             *
             * @param fieldname the name of the field to retrieve.
             *
             * @return the value of the specified field name.
             */
            public Object getValue(final String fieldname)
            {
                final Field field = this.record.getField(fieldname);
                return field==null ? null : field.getRawValue();
            }

            /**
             * Returns a ZonedDateTime corresponding to the provided epoch parameter with the system default timezone.
             *
             * @param epoch the time to convert.
             *
             * @return a ZonedDateTime corresponding to the provided epoch parameter with the system default timezone.
             */
            ZonedDateTime fromEpoch(final long epoch)
            {
                return ZonedDateTime.ofInstant(Instant.ofEpochMilli(Long.valueOf(epoch)), ZoneId.systemDefault());
            }
        }
    }
}

