package com.hurence.logisland.parser.syslog

import java.util.Calendar

import com.hurence.logisland.logisland.parser.base.BaseLogParserTest
import com.hurence.logisland.event.Event
import org.joda.time.DateTimeZone
import org.joda.time.format.DateTimeFormat

/**
  * Created by gregoire on 13/04/16.
  */
class SyslogParserTest extends BaseLogParserTest {

    "A system log" should "be parsed" in {
        val logEntryLines = List(
            "<27>Apr 13 09:35:45 gregoire-W350SKQ-W370SK thermald[942]: thd_trip_cdev_state_reset index 9:rapl_controller"
        )

        val parser = new SyslogParser
        val events = logEntryLines flatMap (log => parser.parse(log))

        events.length should be(1)
        testASyslogEvent(
            events.head,
            priority = "27",
            version = None,
            date = "Apr 13 09:35:45",
            host = "gregoire-W350SKQ-W370SK",
            body = "thermald[942]: thd_trip_cdev_state_reset index 9:rapl_controller"
        )
        println(events.head)
    }


    it should "handle dates as well" in {
        val logEntryLines = List(
       "<30>Apr 20 13:53:02 sd-84186 chef-client: [2016-04-20T13:53:02+02:00] INFO: Processing template[/etc/security/limits.d/root_limits.conf] action create (ulimit::default line 16)")


        val DTF3_SYSLOG_MSG_RFC3164_0 = DateTimeFormat.forPattern("MMM d HH:mm:ss").withZone(DateTimeZone.UTC).withDefaultYear(Calendar.getInstance().get(Calendar.YEAR))
        val timestamp = println(DTF3_SYSLOG_MSG_RFC3164_0.parseDateTime("Apr 20 13:53:02").toDate)

        val parser = new SyslogParser
        val events = logEntryLines flatMap (log => parser.parse(log))

        events.length should be(1)
        testASyslogEvent(
            events.head,
            priority = "30",
            version = None,
            date = "Apr 20 13:53:02",
            host = "sd-84186",
            body = "chef-client: [2016-04-20T13:53:02+02:00] INFO: Processing template[/etc/security/limits.d/root_limits.conf] action create (ulimit::default line 16)"
        )
        println(events.head)



    }

    private def testASyslogEvent(syslogEvent: Event,
                                 priority: String,
                                 version: Option[Int],
                                 date: String,
                                 host: String,
                                 body: String) = {
        syslogEvent.getType should be("syslog")
        testAnEventField(syslogEvent.get("priority"), "priority", "string", priority)
        if (version.isDefined) {
            testAnEventField(syslogEvent.get("version"), "version", "int", version.get.asInstanceOf[Object])
        }
        testAnEventField(syslogEvent.get("date"), "date", "string", date)
        testAnEventField(syslogEvent.get("host"), "host", "string", host)
        testAnEventField(syslogEvent.get("body"), "body", "string", body)
    }
}
