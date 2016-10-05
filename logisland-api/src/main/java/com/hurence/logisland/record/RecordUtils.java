package com.hurence.logisland.record;


public final class RecordUtils {


    public static final String KV_RECORD_KEY_FIELD = "record_key";
    public static final String KV_RECORD_VALUE_FIELD = "record_value";

    public static Record getKeyValueRecord(String key, String value) {
        final Record record = new StandardRecord("kv_record");
        record.setStringField(KV_RECORD_KEY_FIELD, key);
        record.setStringField(KV_RECORD_VALUE_FIELD, value);
        return record;
    }
}
