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

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.ExtraDetailFile;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.*;
import com.hurence.logisland.record.*;
import com.hurence.logisland.validator.StandardValidators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Consolidate session processor
 */
@Tags({"analytics", "web", "session"})
@CapabilityDescription(
        value = "The ConsolidateSession processor is the Logisland entry point to get and process events from the Web Analytics."
                + "As an example here is an incoming event from the Web Analytics:\n\n"
                + "\"fields\": ["
                + "{ \"name\": \"timestamp\",              \"type\": \"long\" },"
                + "{ \"name\": \"remoteHost\",             \"type\": \"string\"},"
                + "{ \"name\": \"record_type\",            \"type\": [\"null\", \"string\"], \"default\": null },"
                + "{ \"name\": \"record_id\",              \"type\": [\"null\", \"string\"], \"default\": null },"
                + "{ \"name\": \"location\",               \"type\": [\"null\", \"string\"], \"default\": null },"
                + "{ \"name\": \"hitType\",                \"type\": [\"null\", \"string\"], \"default\": null },"
                + "{ \"name\": \"eventCategory\",          \"type\": [\"null\", \"string\"], \"default\": null },"
                + "{ \"name\": \"eventAction\",            \"type\": [\"null\", \"string\"], \"default\": null },"
                + "{ \"name\": \"eventLabel\",             \"type\": [\"null\", \"string\"], \"default\": null },"
                + "{ \"name\": \"localPath\",              \"type\": [\"null\", \"string\"], \"default\": null },"
                + "{ \"name\": \"q\",                      \"type\": [\"null\", \"string\"], \"default\": null },"
                + "{ \"name\": \"n\",                      \"type\": [\"null\", \"int\"],    \"default\": null },"
                + "{ \"name\": \"referer\",                \"type\": [\"null\", \"string\"], \"default\": null },"
                + "{ \"name\": \"viewportPixelWidth\",     \"type\": [\"null\", \"int\"],    \"default\": null },"
                + "{ \"name\": \"viewportPixelHeight\",    \"type\": [\"null\", \"int\"],    \"default\": null },"
                + "{ \"name\": \"screenPixelWidth\",       \"type\": [\"null\", \"int\"],    \"default\": null },"
                + "{ \"name\": \"screenPixelHeight\",      \"type\": [\"null\", \"int\"],    \"default\": null },"
                + "{ \"name\": \"partyId\",                \"type\": [\"null\", \"string\"], \"default\": null },"
                + "{ \"name\": \"sessionId\",              \"type\": [\"null\", \"string\"], \"default\": null },"
                + "{ \"name\": \"pageViewId\",             \"type\": [\"null\", \"string\"], \"default\": null },"
                + "{ \"name\": \"is_newSession\",          \"type\": [\"null\", \"boolean\"],\"default\": null },"
                + "{ \"name\": \"userAgentString\",        \"type\": [\"null\", \"string\"], \"default\": null },"
                + "{ \"name\": \"pageType\",               \"type\": [\"null\", \"string\"], \"default\": null },"
                + "{ \"name\": \"UserId\",                 \"type\": [\"null\", \"string\"], \"default\": null },"
                + "{ \"name\": \"B2Bunit\",                \"type\": [\"null\", \"string\"], \"default\": null },"
                + "{ \"name\": \"pointOfService\",         \"type\": [\"null\", \"string\"], \"default\": null },"
                + "{ \"name\": \"companyID\",              \"type\": [\"null\", \"string\"], \"default\": null },"
                + "{ \"name\": \"GroupCode\",              \"type\": [\"null\", \"string\"], \"default\": null },"
                + "{ \"name\": \"userRoles\",              \"type\": [\"null\", \"string\"], \"default\": null },"
                + "{ \"name\": \"is_PunchOut\",            \"type\": [\"null\", \"string\"], \"default\": null }"
                + "]"
                + "The ConsolidateSession processor groups the records by sessions and compute the duration between now and the "
                + "last received event. If the distance from the last event is beyond a given threshold (by default 30mn), then the "
                + "session is considered closed."
                + "The ConsolidateSession is building an aggregated session object for each active session."
                + "This aggregated object includes:"
                + " - The actual session duration."
                + " - A boolean representing wether the session is considered active or closed."
                + "   Note: it is possible to ressurect a session if for instance an event arrives after a session has been marked closed."
                + " - User related infos: userId, B2Bunit code, groupCode, userRoles, companyId"
                + " - First visited page: URL"
                + " - Last visited page: URL"
                + " The properties to configure the processor are:"
                + " - sessionid.field:          Property name containing the session identifier (default: sessionId)."
                + " - timestamp.field:          Property name containing the timestamp of the event (default: timestamp)."
                + " - session.timeout:          Timeframe of inactivity (in seconds) after which a session is considered closed (default: 30mn)."
                + " - visitedpage.field:        Property name containing the page visited by the customer (default: location)."
                + " - fields.to.return:         List of fields to return in the aggregated object. (default: N/A)"
)
@ExtraDetailFile("./details/ConsolidateSession-Detail.rst")
public class ConsolidateSession extends AbstractProcessor {

    private static Logger logger = LoggerFactory.getLogger(ConsolidateSession.class);

    private boolean debug = false;
    
    private static final String KEY_DEBUG = "debug";
    private static final String OUTPUT_RECORD_TYPE = "consolidate-session";
    
    public static final PropertyDescriptor DEBUG = new PropertyDescriptor.Builder()
            .name(KEY_DEBUG)
            .description("Enable debug. If enabled, the original JSON string is embedded in the record_value field of the record.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(false)
            .build();

    public static final PropertyDescriptor SESSION_INACTIVITY_TIMEOUT = new PropertyDescriptor.Builder()
            .name("session.timeout")
            .description("session timeout in sec")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1800")
            .build();

    public static final PropertyDescriptor SESSION_ID_FIELD = new PropertyDescriptor.Builder()
            .name("sessionid.field")
            .description("the name of the field containing the session id => will override default value if set")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("sessionId")
            .build();

    public static final PropertyDescriptor TIMESTAMP_FIELD = new PropertyDescriptor.Builder()
            .name("timestamp.field")
            .description("the name of the field containing the timestamp => will override default value if set")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("h2kTimestamp")
            .build();

    public static final PropertyDescriptor VISITED_PAGE_FIELD = new PropertyDescriptor.Builder()
            .name("visitedpage.field")
            .description("the name of the field containing the visited page => will override default value if set")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("location")
            .build();


    public static final PropertyDescriptor USERID_FIELD = new PropertyDescriptor.Builder()
            .name("userid.field")
            .description("the name of the field containing the userId => will override default value if set")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("userId")
            .build();

    public static final PropertyDescriptor FIELDS_TO_RETURN = new PropertyDescriptor.Builder()
            .name("fields.to.return")
            .description("the list of fields to return")
            .required(false)
            .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
            .build();

    public static final PropertyDescriptor FIRST_VISITED_PAGE_FIELD = new PropertyDescriptor.Builder()
            .name("firstVisitedPage.out.field")
            .description("the name of the field containing the first visited page => will override default value if set")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("firstVisitedPage")
            .build();

    public static final PropertyDescriptor LAST_VISITED_PAGE_FIELD = new PropertyDescriptor.Builder()
            .name("lastVisitedPage.out.field")
            .description("the name of the field containing the last visited page => will override default value if set")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("lastVisitedPage")
            .build();

    public static final PropertyDescriptor IS_SESSION_ACTIVE_FIELD = new PropertyDescriptor.Builder()
            .name("isSessionActive.out.field")
            .description("the name of the field stating whether the session is active or not => will override default value if set")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("is_sessionActive")
            .build();

    public static final PropertyDescriptor SESSION_DURATION_FIELD = new PropertyDescriptor.Builder()
            .name("sessionDuration.out.field")
            .description("the name of the field containing the session duration => will override default value if set")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("sessionDuration")
            .build();

    public static final PropertyDescriptor SESSION_INACTIVITY_DURATION_FIELD = new PropertyDescriptor.Builder()
            .name("sessionInactivityDuration.out.field")
            .description("the name of the field containing the session inactivity duration => will override default value if set")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("sessionInactivityDuration")
            .build();

    public static final PropertyDescriptor EVENTS_COUNTER_FIELD = new PropertyDescriptor.Builder()
            .name("eventsCounter.out.field")
            .description("the name of the field containing the session duration => will override default value if set")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("eventsCounter")
            .build();

    public static final PropertyDescriptor FIRST_EVENT_DATETIME_FIELD = new PropertyDescriptor.Builder()
            .name("firstEventDateTime.out.field")
            .description("the name of the field containing the date of the first event => will override default value if set")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("firstEventDateTime")
            .build();

    public static final PropertyDescriptor LAST_EVENT_DATETIME_FIELD = new PropertyDescriptor.Builder()
            .name("lastEventDateTime.out.field")
            .description("the name of the field containing the date of the last event => will override default value if set")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("lastEventDateTime")
            .build();

    @Override
    public void init(final ProcessContext context)
    {
        super.init(context);
        logger.debug("Initializing Consolidate Session Processor");
    }
    
    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(DEBUG);
        descriptors.add(SESSION_INACTIVITY_TIMEOUT);
        descriptors.add(SESSION_ID_FIELD);
        descriptors.add(TIMESTAMP_FIELD);
        descriptors.add(VISITED_PAGE_FIELD);
        descriptors.add(USERID_FIELD);
        descriptors.add(FIELDS_TO_RETURN);
        descriptors.add(FIRST_VISITED_PAGE_FIELD);
        descriptors.add(LAST_VISITED_PAGE_FIELD);
        descriptors.add(IS_SESSION_ACTIVE_FIELD);
        descriptors.add(SESSION_DURATION_FIELD);
        descriptors.add(EVENTS_COUNTER_FIELD);
        descriptors.add(FIRST_EVENT_DATETIME_FIELD);
        descriptors.add(LAST_EVENT_DATETIME_FIELD);
        descriptors.add(SESSION_INACTIVITY_DURATION_FIELD);

        return Collections.unmodifiableList(descriptors);
    }
  
    @Override
    /**
     * The ConsolidateSession processor groups the records by sessions and compute the duration between now and the
     last received event. If the distance from the last event is beyond a given threshold (by default 30mn), then the
     session is considered closed.
     The ConsolidateSession is building an aggregated session object for each active session.
     This aggregated object includes:
        - The actual session duration.
        - A boolean representing wether the session is considered active or closed.
            Note: it is possible to ressurect a session if for instance an event arrives after a session has been marked closed.
        - User related infos: userId, B2Bunit code, groupCode, userRoles, companyId
        - First visited page: URL
        - Last visited page: URL
     */
    public Collection<Record> process(ProcessContext context, Collection<Record> records)
    {
        if (debug)
        {
            logger.debug("Consolidate Session Processor records input: " + records);
        }
        long session_inactivity_timeout         = context.getPropertyValue(SESSION_INACTIVITY_TIMEOUT).asLong();
        String sessionid_field                  = context.getPropertyValue(SESSION_ID_FIELD).asString();
        String timestamp_field                  = context.getPropertyValue(TIMESTAMP_FIELD).asString();
        String visitedpage_field                = context.getPropertyValue(VISITED_PAGE_FIELD).asString();
        String fields_to_return                 = context.getPropertyValue(FIELDS_TO_RETURN).asString();
        String userid_field                     = context.getPropertyValue(USERID_FIELD).asString();
        String firstVisitedPage_field           = context.getPropertyValue(FIRST_VISITED_PAGE_FIELD).asString();
        String lastVisitedPage_field            = context.getPropertyValue(LAST_VISITED_PAGE_FIELD).asString();
        String isSessionActive_field            = context.getPropertyValue(IS_SESSION_ACTIVE_FIELD).asString();
        String sessionDuration_field            = context.getPropertyValue(SESSION_DURATION_FIELD).asString();
        String eventsCounter_field              = context.getPropertyValue(EVENTS_COUNTER_FIELD).asString();
        String firstEventDateTime_field         = context.getPropertyValue(FIRST_EVENT_DATETIME_FIELD).asString();
        String lastEventDateTime_field          = context.getPropertyValue(LAST_EVENT_DATETIME_FIELD).asString();
        String sessionInactivityDuration_field  = context.getPropertyValue(SESSION_INACTIVITY_DURATION_FIELD).asString();
        String[] fields_to_add                  = null;

        if ((fields_to_return != null) && ( ! fields_to_return.isEmpty())) {
            fields_to_add = fields_to_return.split(",");
        }

        Map<String, List<Record>> groupedBySessions = records
                .stream()
                .filter(e->(e.getField(sessionid_field)!=null))
                .collect(Collectors.groupingBy((Record e )-> e.getField(sessionid_field).asString()));

        LinkedList<Record> consolidatedSessions = new LinkedList<Record>();

        // Return a record containing the user info (i-e at least the userId)
        //groupedBySessions.forEach(
        //        (s, l)-> l.stream()
        //                .sorted(Comparator.comparing((Record e)-> e.getField(timestamp_field).toString()))
        //);

        //HashMap<String, List<Record>> sortedMapSessions = new HashMap<String, List<Record>>();
        //for (String sessionKey : groupedBySessions.keySet())
        //{
        //    List<Record> sortedRecords =
        //    groupedBySessions
        //            .get(sessionKey)
        //            .stream()
        //            .sorted(Comparator.comparing((Record e)-> e.getField(timestamp_field).toString()))
        //            .collect(Collectors.toList());
        //    sortedMapSessions.put(sessionKey, sortedRecords);
        //}
        //
       for (String sessionKey : groupedBySessions.keySet())
        {
            try {
                StandardRecord consolidatedSession = new StandardRecord(OUTPUT_RECORD_TYPE);

                // Grab record being the first event of the session
                Record firstRecord = groupedBySessions.get(sessionKey)
                        .stream()
                        .filter(e -> e.getField(timestamp_field) != null)
                        .min(Comparator.comparingLong((Record e) -> e.getField(timestamp_field).asLong()))
                        .get();

                // Grab record being the latest event of the session
                Record latestRecord = groupedBySessions.get(sessionKey)
                        .stream()
                        .filter(e -> e.getField(timestamp_field) != null)
                        .max(Comparator.comparingLong((Record e) -> e.getField(timestamp_field).asLong()))
                        .get();

                if ((firstRecord == null) || (latestRecord == null)){
                    // Malformed record
                    // Ignore the current session
                    continue;
                }
                // Count the number of events for this session
                long count = groupedBySessions.get(sessionKey)
                        .stream()
                        .count();

                // Grab record containing the userid (if any)
                Optional<Record> optionalUseridRecord = groupedBySessions.get(sessionKey)
                        .stream()
                        .filter(p -> ((p.getField(userid_field) != null) &&
                                (p.getField(userid_field).asString() != null) &&
                                (! p.getField(userid_field).asString().isEmpty())))
                        .findFirst();

                Record useridRecord = null;
                if (optionalUseridRecord.isPresent()) {
                    useridRecord = optionalUseridRecord.get();
                }

                // Compute session inactivity duration (in milliseconds)
                long now = System.currentTimeMillis();
                long sessionInactivityDuration = (now - latestRecord.getField(timestamp_field).asLong()) / 1000;
                if (sessionInactivityDuration > session_inactivity_timeout)
                {
                    // Mark the session as closed
                    consolidatedSession.setField(isSessionActive_field, FieldType.BOOLEAN, false);
                    // Max out the sessionInactivityDuration
                    sessionInactivityDuration = session_inactivity_timeout;
                } else {
                    consolidatedSession.setField(isSessionActive_field, FieldType.BOOLEAN, true);
                }

                long sessionDuration = (latestRecord.getField(timestamp_field).asLong() - firstRecord.getField(timestamp_field).asLong()) / 1000;
                if (sessionDuration > 0) {
                    consolidatedSession.setField(sessionDuration_field, FieldType.LONG, sessionDuration);
                }
                if (sessionInactivityDuration > 0) {
                    consolidatedSession.setField(
                            sessionInactivityDuration_field,
                            FieldType.LONG,
                            sessionInactivityDuration);
                }

                // Grab the firstEventDateTime and the lastEventDateTime
                Date firstEventDateTime = new Date(firstRecord.getField(timestamp_field).asLong());
                Date lastEventDateTime = new Date(latestRecord.getField(timestamp_field).asLong());
                if (firstEventDateTime != null) {
                    consolidatedSession.setField(firstEventDateTime_field, FieldType.STRING, firstEventDateTime.toString());
                }
                if (lastEventDateTime != null) {
                    consolidatedSession.setField(lastEventDateTime_field, FieldType.STRING, lastEventDateTime.toString());
                }

                // Grab and set the first and last visited page
                String firstVisitedPage = firstRecord.getField(visitedpage_field).asString();
                String lastVisitedPage = latestRecord.getField(visitedpage_field).asString();
                if (firstVisitedPage != null) {
                    consolidatedSession.setField(firstVisitedPage_field, FieldType.STRING, firstVisitedPage);
                }
                if (lastVisitedPage != null) {
                    consolidatedSession.setField(lastVisitedPage_field, FieldType.STRING, lastVisitedPage);
                }

                // Set the events counter
                consolidatedSession.setField(eventsCounter_field, FieldType.LONG, count);

                // Add the userid record if available
                if (useridRecord != null) {
                    consolidatedSession.setField(userid_field, FieldType.STRING, useridRecord.getField(userid_field).asString());
                }

                // Add additional fields from the first record in the stream containing it.
                if ((fields_to_add != null) && (fields_to_add.length > 0)) {
                    for (String field : fields_to_add)
                    {
                        Optional<Record> optionalRecord = groupedBySessions.get(sessionKey)
                                .stream()
                                .filter(p -> (
                                        (p.getField(field) != null) &&
                                        (p.getField(field).asString() != null) &&
                                        (! p.getField(field).asString().isEmpty()))
                                 )
                                .findFirst();

                        if (optionalRecord.isPresent())
                        {
                            Record recordContainingField = optionalRecord.get();
                            String value = recordContainingField.getField(field).asString();
                            if ((value != null) && (!value.isEmpty())) {
                                consolidatedSession.setField(field, FieldType.STRING, value);
                            }
                        }
                    }
                }

                // Set the sessionid into the consolidated session record
                consolidatedSession.setField(sessionid_field, FieldType.STRING, sessionKey);

                // Set the session create timestamp to the create timestamp of the firts event on the session.
                consolidatedSession.setField(timestamp_field, FieldType.LONG, firstRecord.getField(timestamp_field).asLong());

                // Set the record id
                consolidatedSession.setStringField(FieldDictionary.RECORD_ID, sessionKey);

                consolidatedSessions.add(consolidatedSession);
            }
            catch (Exception x){
                logger.warn("issue while trying to consolidate session events {} :  {}",
                        context.getPropertyValue(SESSION_ID_FIELD).asString(),
                        x.toString());
            }
        }

        if (debug)
        {
            logger.debug("Consolidate Session Processor records output: " + consolidatedSessions);
        }
        return consolidatedSessions;
    }
    
    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {

        logger.debug("property {} value changed from {} to {}", descriptor.getName(), oldValue, newValue);
        
        /**
         * Handle the debug property
         */
        if (descriptor.getName().equals(KEY_DEBUG))
        {
          if (newValue != null)
          {
              if (newValue.equalsIgnoreCase("true"))
              {
                  debug = true;
              }
          } else
          {
              debug = false;
          }
        }
    }   
}
