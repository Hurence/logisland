package com.hurence.logisland.plugin.syslog

import base.BaseLogParserTest
import com.hurence.logisland.event.Event

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
