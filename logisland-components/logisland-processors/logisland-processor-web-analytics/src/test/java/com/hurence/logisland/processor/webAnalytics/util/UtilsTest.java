package com.hurence.logisland.processor.webAnalytics.util;

import com.hurence.logisland.processor.webAnalytics.modele.Event;
import com.hurence.logisland.processor.webAnalytics.modele.TestMappings;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class UtilsTest {

    @Test
    public void testClone() {
        Event event0 = new Event(
                new WebEvent("0", "session","user", 0L, "url"),
                TestMappings.eventsInternalFields
        );
        Record record = event0.cloneRecord();
        Utils.toMap(record, false);
    }

    @Test
    public void test() {
        Record record = new StandardRecord();
        record.setStringField("string", "value");
        record.setBooleanField("bool", true);
        record.setBooleanField("bool2", false);
        record.setDoubleField("double", 0.2d);
        record.setLongField("long", 45L);
        record.setIntField("int", 5);
        record.setFloatField("float", 5.6f);
        record.setBytesField("bytes", new Byte[]{1, 2});
        record.setArrayField("array", Arrays.asList("1", "2"));
        record.setDateTimeField("date", new Date(0));
//        record.setRecordField("record", "value");
//        record.setObjectField("object", "value");
        Map<String, Object> mapFromRecord = Utils.toMap(record, false);
        Map<String, Object> mapExpected = new HashMap<>();
        //{date=Thu Jan 01 01:00:00 CET 1970, bool=true, string=value, double=0.2, float=5.6, int=5, long=45, record_type=generic, bool2=false, record_id=47fbd588-f70c-4a85-a2bc-1c690cb88e60, array=[1, 2], bytes=[Ljava.lang.Byte;@2d6e8792, record_time=1607619730231}
        Assert.assertEquals(mapExpected, mapFromRecord);
    }

    @Test
    public void test2() {
        Record record = new StandardRecord();
        record.setStringField("string", "value");
        record.setBooleanField("bool", true);
        record.setBooleanField("bool2", false);
        record.setDoubleField("double", 0.2d);
        record.setLongField("long", 45L);
        record.setIntField("int", 5);
        record.setFloatField("float", 5.6f);
        record.setBytesField("bytes", new Byte[]{1, 2});
        record.setArrayField("array", Arrays.asList("1", "2"));
        record.setDateTimeField("date", new Date(0));
//        record.setRecordField("record", "value");
//        record.setObjectField("object", "value");
        Map<String, Object> mapFromRecord = Utils.toMap(record, true);
        Map<String, Object> mapExpected = new HashMap<>();
//        {date=Thu Jan 01 01:00:00 CET 1970, bool2=false, bool=true, string=value, array=[1, 2], bytes=[Ljava.lang.Byte;@22f71333, double=0.2, float=5.6, int=5, long=45}
        Assert.assertEquals(mapExpected, mapFromRecord);
    }
}

