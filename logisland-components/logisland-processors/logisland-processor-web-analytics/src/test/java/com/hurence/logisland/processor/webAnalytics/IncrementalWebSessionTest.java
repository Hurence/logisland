package com.hurence.logisland.processor.webAnalytics;

import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static com.hurence.logisland.processor.webAnalytics.util.ElasticsearchServiceUtil.EVENT_INDEX_PREFIX;
import static com.hurence.logisland.processor.webAnalytics.util.ElasticsearchServiceUtil.SESSION_INDEX_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class IncrementalWebSessionTest {


    @Test
    public void testOneEventOnlyPerSession()
    {
        IncrementalWebSession proc = new IncrementalWebSession();
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.assertNotValid();

        runner.setProperty(IncrementalWebSession.ES_EVENT_INDEX_PREFIX_CONF, EVENT_INDEX_PREFIX);
        runner.setProperty(IncrementalWebSession.ES_SESSION_INDEX_PREFIX_CONF, SESSION_INDEX_PREFIX);
        runner.setProperty(IncrementalWebSession.ES_SESSION_INDEX_SUFFIX_FORMATTER_CONF, "yyyy.MM");
        runner.setProperty(IncrementalWebSession.ES_EVENT_INDEX_SUFFIX_FORMATTER_CONF, "yyyy.MM.dd");
        runner.setProperty(IncrementalWebSession.ES_SESSION_TYPE_NAME_CONF, "sessions");
        runner.setProperty(IncrementalWebSession.ES_EVENT_TYPE_NAME_CONF, "event");
        runner.setProperty(IncrementalWebSession.CONFIG_CACHE_SERVICE, "lruCache");
        runner.setProperty(IncrementalWebSession.ELASTICSEARCH_CLIENT_SERVICE_CONF, "elasticsearchClient");
        runner.assertValid();

        runner.removeProperty(IncrementalWebSession.ES_EVENT_INDEX_PREFIX_CONF);
        runner.assertNotValid();
        runner.setProperty(IncrementalWebSession.ES_EVENT_INDEX_PREFIX_CONF, EVENT_INDEX_PREFIX);
        runner.removeProperty(IncrementalWebSession.ES_SESSION_INDEX_PREFIX_CONF);
        runner.assertNotValid();
        runner.setProperty(IncrementalWebSession.ES_SESSION_INDEX_PREFIX_CONF, SESSION_INDEX_PREFIX);
        runner.removeProperty(IncrementalWebSession.ES_SESSION_INDEX_SUFFIX_FORMATTER_CONF);
        runner.assertNotValid();
        runner.setProperty(IncrementalWebSession.ES_SESSION_INDEX_SUFFIX_FORMATTER_CONF, "yyyy.MM");
        runner.removeProperty(IncrementalWebSession.ES_EVENT_INDEX_SUFFIX_FORMATTER_CONF);
        runner.assertNotValid();
        runner.setProperty(IncrementalWebSession.ES_EVENT_INDEX_SUFFIX_FORMATTER_CONF, "yyyy.MM.dd");
        runner.removeProperty(IncrementalWebSession.ES_SESSION_TYPE_NAME_CONF);
        runner.assertNotValid();
        runner.setProperty(IncrementalWebSession.ES_SESSION_TYPE_NAME_CONF, "sessions");
        runner.removeProperty(IncrementalWebSession.ES_EVENT_TYPE_NAME_CONF);
        runner.assertNotValid();
        runner.setProperty(IncrementalWebSession.ES_EVENT_TYPE_NAME_CONF, "event");
        runner.removeProperty(IncrementalWebSession.CONFIG_CACHE_SERVICE);
        runner.assertNotValid();
        runner.setProperty(IncrementalWebSession.CONFIG_CACHE_SERVICE, "lruCache");
        runner.removeProperty(IncrementalWebSession.ELASTICSEARCH_CLIENT_SERVICE_CONF);
        runner.assertNotValid();
        runner.setProperty(IncrementalWebSession.ELASTICSEARCH_CLIENT_SERVICE_CONF, "elasticsearchClient");
        runner.assertValid();

        runner.setProperty(IncrementalWebSession.ZONEID_CONF, "aba");
        runner.assertNotValid();
        runner.setProperty(IncrementalWebSession.ZONEID_CONF, "Canada/Yukon");
        runner.assertValid();
        runner.setProperty(IncrementalWebSession.ZONEID_CONF, "CaNAda/YuKON");
        runner.assertNotValid();
        runner.setProperty(IncrementalWebSession.ZONEID_CONF, "UTC");
        runner.assertValid();
        runner.setProperty(IncrementalWebSession.ZONEID_CONF, "Japan");
        runner.assertValid();


    }



    @Test
    public void testChoosingTheZoneId() {
        //        1575158400000    1/12/2019 à 0:00:00   "1/12/2019 à 1:00:00 +0100"  in LOCAL english
        //        1577836799000    31/12/2019 à 23:59:59 "1/1/2020 à 0:59:59 +0100"  in LOCAL english
        //        1577836800000    1/1/2020 à 0:00:00    "1/1/2020 à 1:00:00 +0100" in LOCAL english
        //        1577840399000    1/1/2020 à 0:59:59    "1/1/2020 à 1:59:59 +0100" in LOCAL english
        //        1580515199000    31/1/2020 à 23:59:59  "1/2/2020 à 0:59:59 +0100" in LOCAL english
        //        1580515200000    1/2/2020 à 0:00:00    "1/2/2020 à 1:00:00 +0100" in LOCAL english
        //        1583020799000    29/2/2020 à 23:59:59  "1/3/2020 à 0:59:59 +0100" in LOCAL english
        final long time1 = 1575158400000L;
        final long time2 = 1577836799000L;
        final long time3 = 1577836800000L;
        final long time4 = 1577840399000L;
        final long time5 = 1580515199000L;
        final long time6 = 1580515200000L;
        final long time7 = 1583020799000L;
        IncrementalWebSession proc = new IncrementalWebSession();
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(IncrementalWebSession.ES_EVENT_INDEX_PREFIX_CONF, EVENT_INDEX_PREFIX);
        runner.setProperty(IncrementalWebSession.ES_SESSION_INDEX_PREFIX_CONF, SESSION_INDEX_PREFIX);
        runner.setProperty(IncrementalWebSession.ES_SESSION_INDEX_SUFFIX_FORMATTER_CONF, "yyyy.MM.dd HH:mm:ss Z");
        runner.setProperty(IncrementalWebSession.ES_EVENT_INDEX_SUFFIX_FORMATTER_CONF, "yyyy.MM.dd HH:mm:ss Z");
        runner.setProperty(IncrementalWebSession.ES_SESSION_TYPE_NAME_CONF, "sessions");
        runner.setProperty(IncrementalWebSession.ES_EVENT_TYPE_NAME_CONF, "event");
        runner.setProperty(IncrementalWebSession.CONFIG_CACHE_SERVICE, "lruCache");
        runner.setProperty(IncrementalWebSession.ELASTICSEARCH_CLIENT_SERVICE_CONF, "elasticsearchClient");
        runner.assertValid();
        runner.run();
        // 1/12/2019 à 1:00:00 +0100
        assertEquals(EVENT_INDEX_PREFIX+   "2019.12.01 01:00:00 +0100", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time1)));
        assertEquals(SESSION_INDEX_PREFIX +"2019.12.01 01:00:00 +0100", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time1)));
        //"1/1/2020 à 0:59:59 +0100"
        assertEquals(EVENT_INDEX_PREFIX+   "2020.01.01 00:59:59 +0100", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time2)));
        assertEquals(SESSION_INDEX_PREFIX +"2020.01.01 00:59:59 +0100", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time2)));
        //1/1/2020 à 1:00:00 +0100
        assertEquals(EVENT_INDEX_PREFIX+   "2020.01.01 01:00:00 +0100", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time3)));
        assertEquals(SESSION_INDEX_PREFIX +"2020.01.01 01:00:00 +0100", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time3)));
        //1/1/2020 à 1:59:59 +0100
        assertEquals(EVENT_INDEX_PREFIX+   "2020.01.01 01:59:59 +0100", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time4)));
        assertEquals(SESSION_INDEX_PREFIX +"2020.01.01 01:59:59 +0100", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time4)));
        //1/2/2020 à 0:59:59 +0100
        assertEquals(EVENT_INDEX_PREFIX+   "2020.02.01 00:59:59 +0100", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time5)));
        assertEquals(SESSION_INDEX_PREFIX +"2020.02.01 00:59:59 +0100", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time5)));
        //"1/2/2020 à 1:00:00 +0100"
        assertEquals(EVENT_INDEX_PREFIX+   "2020.02.01 01:00:00 +0100", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time6)));
        assertEquals(SESSION_INDEX_PREFIX +"2020.02.01 01:00:00 +0100", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time6)));
        //1/3/2020 à 0:59:59 +0100
        assertEquals(EVENT_INDEX_PREFIX+   "2020.03.01 00:59:59 +0100", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time7)));
        assertEquals(SESSION_INDEX_PREFIX +"2020.03.01 00:59:59 +0100", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time7)));


        runner.setProperty(IncrementalWebSession.PROP_ES_INDEX_SUFFIX_TIMEZONE, "Japan");
        runner.run();
        // 1/12/2019 à 1:00:00 +0100
        assertEquals(EVENT_INDEX_PREFIX+   "2019.12.01 09:00:00 +0900", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time1)));
        assertEquals(SESSION_INDEX_PREFIX +"2019.12.01 09:00:00 +0900", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time1)));
        //"1/1/2020 à 0:59:59 +0100"
        assertEquals(EVENT_INDEX_PREFIX+   "2020.01.01 08:59:59 +0900", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time2)));
        assertEquals(SESSION_INDEX_PREFIX +"2020.01.01 08:59:59 +0900", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time2)));
        //1/1/2020 à 1:00:00 +0100
        assertEquals(EVENT_INDEX_PREFIX+   "2020.01.01 09:00:00 +0900", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time3)));
        assertEquals(SESSION_INDEX_PREFIX +"2020.01.01 09:00:00 +0900", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time3)));
        //1/1/2020 à 1:59:59 +0100
        assertEquals(EVENT_INDEX_PREFIX+   "2020.01.01 09:59:59 +0900", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time4)));
        assertEquals(SESSION_INDEX_PREFIX +"2020.01.01 09:59:59 +0900", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time4)));
        //1/2/2020 à 0:59:59 +0100
        assertEquals(EVENT_INDEX_PREFIX+   "2020.02.01 08:59:59 +0900", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time5)));
        assertEquals(SESSION_INDEX_PREFIX +"2020.02.01 08:59:59 +0900", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time5)));
        //"1/2/2020 à 1:00:00 +0100"
        assertEquals(EVENT_INDEX_PREFIX+   "2020.02.01 09:00:00 +0900", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time6)));
        assertEquals(SESSION_INDEX_PREFIX +"2020.02.01 09:00:00 +0900", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time6)));
        //1/3/2020 à 0:59:59 +0100
        assertEquals(EVENT_INDEX_PREFIX+   "2020.03.01 08:59:59 +0900", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time7)));
        assertEquals(SESSION_INDEX_PREFIX +"2020.03.01 08:59:59 +0900", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time7)));

        runner.setProperty(IncrementalWebSession.PROP_ES_INDEX_SUFFIX_TIMEZONE,  "Canada/Yukon");
        runner.run();
        // 1/12/2019 à 1:00:00 +0100
        assertEquals(EVENT_INDEX_PREFIX+   "2019.11.30 16:00:00 -0800", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time1)));
        assertEquals(SESSION_INDEX_PREFIX +"2019.11.30 16:00:00 -0800", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time1)));
        //"1/1/2020 à 0:59:59 +0100"
        assertEquals(EVENT_INDEX_PREFIX+   "2019.12.31 15:59:59 -0800", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time2)));
        assertEquals(SESSION_INDEX_PREFIX +"2019.12.31 15:59:59 -0800", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time2)));
        //1/1/2020 à 1:00:00 +0100
        assertEquals(EVENT_INDEX_PREFIX+   "2019.12.31 16:00:00 -0800", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time3)));
        assertEquals(SESSION_INDEX_PREFIX +"2019.12.31 16:00:00 -0800", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time3)));
        //1/1/2020 à 1:59:59 +0100
        assertEquals(EVENT_INDEX_PREFIX+   "2019.12.31 16:59:59 -0800", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time4)));
        assertEquals(SESSION_INDEX_PREFIX +"2019.12.31 16:59:59 -0800", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time4)));
        //1/2/2020 à 0:59:59 +0100
        assertEquals(EVENT_INDEX_PREFIX+   "2020.01.31 15:59:59 -0800", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time5)));
        assertEquals(SESSION_INDEX_PREFIX +"2020.01.31 15:59:59 -0800", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time5)));
        //"1/2/2020 à 1:00:00 +0100"
        assertEquals(EVENT_INDEX_PREFIX+   "2020.01.31 16:00:00 -0800", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time6)));
        assertEquals(SESSION_INDEX_PREFIX +"2020.01.31 16:00:00 -0800", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time6)));
        //1/3/2020 à 0:59:59 +0100
        assertEquals(EVENT_INDEX_PREFIX+   "2020.02.29 15:59:59 -0800", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time7)));
        assertEquals(SESSION_INDEX_PREFIX +"2020.02.29 15:59:59 -0800", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time7)));

        runner.setProperty(IncrementalWebSession.PROP_ES_INDEX_SUFFIX_TIMEZONE,  "UTC");
        runner.run();
        // 1/12/2019 à 1:00:00 +0100
        assertEquals(EVENT_INDEX_PREFIX+   "2019.12.01 00:00:00 +0000", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time1)));
        assertEquals(SESSION_INDEX_PREFIX +"2019.12.01 00:00:00 +0000", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time1)));
        //"1/1/2020 à 0:59:59 +0100"
        assertEquals(EVENT_INDEX_PREFIX+   "2019.12.31 23:59:59 +0000", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time2)));
        assertEquals(SESSION_INDEX_PREFIX +"2019.12.31 23:59:59 +0000", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time2)));
        //1/1/2020 à 1:00:00 +0100
        assertEquals(EVENT_INDEX_PREFIX+   "2020.01.01 00:00:00 +0000", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time3)));
        assertEquals(SESSION_INDEX_PREFIX +"2020.01.01 00:00:00 +0000", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time3)));
        //1/1/2020 à 1:59:59 +0100
        assertEquals(EVENT_INDEX_PREFIX+   "2020.01.01 00:59:59 +0000", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time4)));
        assertEquals(SESSION_INDEX_PREFIX +"2020.01.01 00:59:59 +0000", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time4)));
        //1/2/2020 à 0:59:59 +0100
        assertEquals(EVENT_INDEX_PREFIX+   "2020.01.31 23:59:59 +0000", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time5)));
        assertEquals(SESSION_INDEX_PREFIX +"2020.01.31 23:59:59 +0000", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time5)));
        //"1/2/2020 à 1:00:00 +0100"
        assertEquals(EVENT_INDEX_PREFIX+   "2020.02.01 00:00:00 +0000", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time6)));
        assertEquals(SESSION_INDEX_PREFIX +"2020.02.01 00:00:00 +0000", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time6)));
        //1/3/2020 à 0:59:59 +0100
        assertEquals(EVENT_INDEX_PREFIX+   "2020.02.29 23:59:59 +0000", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time7)));
        assertEquals(SESSION_INDEX_PREFIX +"2020.02.29 23:59:59 +0000", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time7)));
    }

    private ZonedDateTime GetZonedDateTimeFromEpochMili(long time1) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(time1), ZoneId.systemDefault());
    }
}
