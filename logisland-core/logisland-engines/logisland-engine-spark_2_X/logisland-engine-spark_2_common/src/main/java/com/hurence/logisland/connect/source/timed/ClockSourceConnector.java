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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A connector that emits an empty record at fixed rate waking up the processing pipeline.
 *
 * @author amarziali
 */
public class ClockSourceConnector extends SourceConnector {

    public static final String RATE = "rate";
    public static final String POLL_CRON_SCHEDULER_CONFIG = "poll.cron";
    public static final String SNAPSHOT_FIELD_CONFIG = "snapshot.field";
    public static final String TSID_FIELD_CONFIG = "tsid.field";
    public static final String DATE_FIELD_CONFIG = "date.field";
    public static final String DATE_FORMAT_CONFIG = "date.format";
    public static final String RECORD_ID_FIELD_CONFIG = "record.id.field";
    public static final String HAS_ONGOING_RECORD_CONFIG = "has.ongoing.record";
    public static final String HAS_PREVIOUS_RECORD_CONFIG = "has.previous.record";
    public static final String CURRENT_RECORD_ID_VALUE_CONFIG = "current.record.id.value";

    public static final String POLL_CRON_SCHEDULER_DEFAULT = null;
    public static final String SNAPSHOT_FIELD_DEFAULT = "record_snapshot";
    public static final String TSID_FIELD_DEFAULT = "ts_id";
    public static final String DATE_FIELD_DEFAULT = null;
    public static final String DATE_FORMAT_DEFAULT = "yyyy-MM-dd HH:mm:ss z";
    public static final String RECORD_ID_FIELD_DEFAULT = "id";
    public static final boolean HAS_ONGOING_RECORD_DEFAULT = false;
    public static final boolean HAS_PREVIOUS_RECORD_DEFAULT = false;
    public static final String CURRENT_RECORD_ID_VALUE_DEFAULT = "fsv";


    private static final ConfigDef CONFIG = new ConfigDef()
            .define(RATE, ConfigDef.Type.LONG, null, ConfigDef.Importance.HIGH, "The clock rate in milliseconds")
            .define(POLL_CRON_SCHEDULER_CONFIG, ConfigDef.Type.STRING, POLL_CRON_SCHEDULER_DEFAULT, ConfigDef.Importance.HIGH, "The cron expression")
            .define(SNAPSHOT_FIELD_CONFIG, ConfigDef.Type.STRING, SNAPSHOT_FIELD_DEFAULT, ConfigDef.Importance.HIGH, "Name of the field containing the snapshot id")
            .define(TSID_FIELD_CONFIG, ConfigDef.Type.STRING, TSID_FIELD_DEFAULT, ConfigDef.Importance.HIGH, "Name of the field containing the ordering column")
            .define(DATE_FIELD_CONFIG, ConfigDef.Type.STRING, DATE_FIELD_DEFAULT, ConfigDef.Importance.HIGH, "Name of the field containing the date in human readable format")
            .define(DATE_FORMAT_CONFIG, ConfigDef.Type.STRING, DATE_FORMAT_DEFAULT, ConfigDef.Importance.HIGH, "Format to use to display date in human readable-format")
            .define(RECORD_ID_FIELD_CONFIG, ConfigDef.Type.STRING, RECORD_ID_FIELD_DEFAULT, ConfigDef.Importance.HIGH, "Name of the field containing the id of the record")
            .define(HAS_ONGOING_RECORD_CONFIG, ConfigDef.Type.BOOLEAN, HAS_ONGOING_RECORD_DEFAULT, ConfigDef.Importance.HIGH, "If set to true, it will produce an additional record with ongoing snapshot details")
            .define(HAS_PREVIOUS_RECORD_CONFIG, ConfigDef.Type.BOOLEAN, HAS_PREVIOUS_RECORD_DEFAULT, ConfigDef.Importance.HIGH, "If set to true, it will produce an additional record with previous snapshot details")
            .define(CURRENT_RECORD_ID_VALUE_CONFIG, ConfigDef.Type.STRING, CURRENT_RECORD_ID_VALUE_DEFAULT, ConfigDef.Importance.HIGH, "Specifies the id value of the record");

    private long rate;
    private String recordIdField;
    private String currentRecordIdValue;
    private String recordSnapshotField;
    private String cronExprValue;
    private String tsidField;
    private String dateField;
    private boolean hasOngoingRecordDefault;
    private boolean hasPreviousRecordDefault;
    private String formatDateValue;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        rate = (Long) CONFIG.parse(props).get(RATE);
        recordIdField = (String) CONFIG.parse(props).get(RECORD_ID_FIELD_CONFIG);
        recordSnapshotField = (String) CONFIG.parse(props).get(SNAPSHOT_FIELD_CONFIG);
        cronExprValue = (String) CONFIG.parse(props).get(POLL_CRON_SCHEDULER_CONFIG);
        tsidField = (String) CONFIG.parse(props).get(TSID_FIELD_CONFIG);
        dateField = (String) CONFIG.parse(props).get(DATE_FIELD_CONFIG);
        hasOngoingRecordDefault = (boolean) CONFIG.parse(props).get(HAS_ONGOING_RECORD_CONFIG);
        hasPreviousRecordDefault = (boolean) CONFIG.parse(props).get(HAS_PREVIOUS_RECORD_CONFIG);
        currentRecordIdValue = (String) CONFIG.parse(props).get(CURRENT_RECORD_ID_VALUE_CONFIG);
        formatDateValue = (String) CONFIG.parse(props).get(DATE_FORMAT_CONFIG);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ClockSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        Map<String, String> mapConfig = new HashMap<>();
        mapConfig.put(RATE, Long.toString(rate));
        mapConfig.put(RECORD_ID_FIELD_CONFIG, recordIdField);
        mapConfig.put(CURRENT_RECORD_ID_VALUE_CONFIG, currentRecordIdValue);
        mapConfig.put(SNAPSHOT_FIELD_CONFIG, recordSnapshotField);
        mapConfig.put(POLL_CRON_SCHEDULER_CONFIG, cronExprValue);
        mapConfig.put(TSID_FIELD_CONFIG, tsidField);
        mapConfig.put(DATE_FIELD_CONFIG, dateField);
        mapConfig.put(HAS_ONGOING_RECORD_CONFIG, Boolean.toString(hasOngoingRecordDefault));
        mapConfig.put(HAS_PREVIOUS_RECORD_CONFIG, Boolean.toString(hasPreviousRecordDefault));
        mapConfig.put(DATE_FORMAT_CONFIG, formatDateValue);
        return Collections.singletonList(mapConfig);
        //return Collections.singletonList(Collections.singletonMap(RATE, Long.toString(rate)));
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {

        return CONFIG;
    }
}
