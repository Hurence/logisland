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
package com.hurence.logisland.connect.source.timed;

import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.text.SimpleDateFormat;
import java.util.*;

import org.springframework.scheduling.support.CronSequenceGenerator;


/**
 * {@link SourceTask} for {@link ClockSourceConnector}
 *
 * @author amarziali, jarnou
 *
 * The ClockSourceTask is a kafka connect service controller designed to
 * generate a/(a set of) record(s) on a regular basis.
 * It can be either on a rate (in milliseconds) or on a cron basis (cron expression).
 *
 * Note: If both rate and cron are specified in the configuration of the controller,
 * the cron takes precedence over the rate.
 *
 * It is possible to add a field in the produced record containing a value
 * (number of seconds since 1970) corresponding to the time at which the record has been
 * produced and another field containing the date in a human-readable format in a
 * specified timezone (CET being the default)
 *
 * By the way:
 * It is also possible in addition to the standard record produced to generate 2 additional records.
 * One is containing the data (snaphot, date) for the ongoing 'clock', and another one for the
 * previous one.
 * The use-case for these 2 additional records is the update of a table of snapshots for instance.
 * The idea is to have a list of all snapshots, as well as the ongoing and the previous one.
 *
 */
public class ClockSourceTask extends SourceTask {

    private Time time;
    private long rate;
    private String cronExpr;
    private String recordIdField;
    private String recordIdValue;
    private String snapshotField;
    private String tsidField;
    private String dateField;
    private CronSequenceGenerator cronSeqGen = null;
    private boolean useCron = false;
    private boolean useSnapshot = false;
    private boolean useTSID = false;
    private boolean useDate = false;
    private boolean hasOngoingRecord = false;
    private boolean hasPreviousRecord = false;
    private long recordSnapshot = -1; // Uniquely identifies a poll/retrieval of the data from a src
    private Schema finalSchema = null;
    private long previousRecordSnapshot = -1;
    private static long TSID_DEFAULT = -1;
    private String dateFormat;
    private String dateTimezone;



    @Override
    public void start(Map<String, String> props) {
        this.time = new SystemTime();
        rate = Long.parseLong(props.get(ClockSourceConnector.RATE));
        cronExpr = props.get(ClockSourceConnector.POLL_CRON_SCHEDULER_CONFIG);
        recordIdField = props.get(ClockSourceConnector.RECORD_ID_FIELD_CONFIG);
        snapshotField = props.get(ClockSourceConnector.SNAPSHOT_FIELD_CONFIG);
        tsidField = props.get(ClockSourceConnector.TSID_FIELD_CONFIG);
        dateField = props.get(ClockSourceConnector.DATE_FIELD_CONFIG);
        recordIdValue = props.get(ClockSourceConnector.CURRENT_RECORD_ID_VALUE_CONFIG);
        dateFormat = props.get(ClockSourceConnector.DATE_FORMAT_CONFIG);
        dateTimezone = props.get(ClockSourceConnector.DATE_TIMEZONE_CONFIG);

        // Check if cron should be used && Generate a cron object once for further use
        if ((cronExpr != null) && (cronExpr.isEmpty() != true)) {
            useCron = CronSequenceGenerator.isValidExpression(cronExpr);
        }

        if (useCron) {
            cronSeqGen = new CronSequenceGenerator(cronExpr);
        }
        useSnapshot = (snapshotField != null) ? true : false;
        useTSID = (tsidField != null) ? true : false;
        useDate = (dateField != null) ? true : false;
        hasOngoingRecord = new Boolean(props.get(ClockSourceConnector.HAS_ONGOING_RECORD_CONFIG));
        hasPreviousRecord = new Boolean(props.get(ClockSourceConnector.HAS_PREVIOUS_RECORD_CONFIG));

        // Build the schema if not created yet
        if (finalSchema == null) {
            SchemaBuilder newSchema = SchemaBuilder.struct();
            newSchema.field(recordIdField, Schema.STRING_SCHEMA);
            if (useSnapshot) {
                newSchema.field(snapshotField, Schema.INT64_SCHEMA);
            }
            if (useTSID) {
                newSchema.field(tsidField, Schema.INT64_SCHEMA);
            }
            if (useDate) {
                newSchema.field(dateField, Schema.STRING_SCHEMA);
            }
            finalSchema = newSchema.build();
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        final long untilNext;
        if (useCron) {
            Date nextTriggerDate = cronSeqGen.next(new Date(time.milliseconds()));
            long nextTriggerDateInMs = nextTriggerDate.getTime();
            untilNext = nextTriggerDateInMs - time.milliseconds();
            if (useSnapshot) {
                recordSnapshot = nextTriggerDateInMs ;
            }
            time.sleep(untilNext);
        }
        else {
            if (useSnapshot){
                recordSnapshot = (time.milliseconds()+rate) ;
            }
            Thread.sleep(rate);
        }

        Struct recordVal = new Struct(finalSchema);
        recordVal.put(recordIdField, recordIdValue);
        if (useSnapshot) {
            recordVal.put(snapshotField, recordSnapshot);
            if (useDate){
                String jdate = secToString(recordSnapshot, dateFormat, dateTimezone);
                recordVal.put(dateField, jdate);
            }
        }
        if (useTSID) {
            recordVal.put(tsidField, recordSnapshot);
        }

        SourceRecord sr = new SourceRecord(
                null,
                null,
                "",
                finalSchema,
                recordVal);

        if ( ! hasOngoingRecord  && ! hasPreviousRecord ) {
            return Collections.singletonList(sr);
        }
        else {
            List<SourceRecord> listRecords = new LinkedList<>();
            listRecords.add(sr);

            if (useSnapshot) {
                // Build ongoing record (if requested)
                if (hasOngoingRecord){
                    Struct orVal = new Struct(finalSchema);
                    orVal.put(recordIdField, "ongoing");
                    if (useSnapshot) {
                        orVal.put(snapshotField, recordSnapshot);
                        if (useDate){
                            String jdate = secToString(recordSnapshot, dateFormat, dateTimezone);
                            orVal.put(dateField, jdate);
                        }
                    }
                    if (useTSID) {
                        orVal.put(tsidField, TSID_DEFAULT);
                    }

                    SourceRecord or = new SourceRecord(
                            null,
                            null,
                            "",
                            finalSchema,
                            orVal);
                    listRecords.add(or);
                }

                // Build previous record (if requested)
                if (hasPreviousRecord && previousRecordSnapshot > 0) {
                    Struct prVal = new Struct(finalSchema);
                    prVal.put(recordIdField, "previous");
                    if (useSnapshot) {
                        prVal.put(snapshotField, previousRecordSnapshot);
                        if (useDate){
                            String jdate = secToString(previousRecordSnapshot, dateFormat, dateTimezone);
                            prVal.put(dateField, jdate);
                        }
                    }
                    if (useTSID) {
                        prVal.put(tsidField, TSID_DEFAULT);
                    }

                    SourceRecord pr = new SourceRecord(null,
                            null,
                            "",
                            finalSchema,
                            prVal);
                    listRecords.add(pr);
                }
                previousRecordSnapshot = recordSnapshot;
            }

            return listRecords;
        }
    }

    @Override
    public void stop() {

    }

    @Override
    public String version() {
        return "1.0";
    }

    /*
     * Return the timeInSec in a Human Readable
     * format (dateFormat) using the timezone given in parameter.
     */
    private String secToString(long timeInSec, String dateFormat, String timezone){
        //convert seconds to milliseconds
        Date date = new Date(timeInSec);
        // format of the date
        SimpleDateFormat jdf = new SimpleDateFormat(dateFormat);
        jdf.setTimeZone(TimeZone.getTimeZone(timezone));
        String jdate = jdf.format(date);
        return jdate;
    }
}
