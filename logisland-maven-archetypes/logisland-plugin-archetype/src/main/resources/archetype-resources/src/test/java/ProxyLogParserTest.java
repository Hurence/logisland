package ${package};


import com.hurence.logisland.event.Event;
import com.hurence.logisland.log.LogParser;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;


public class ProxyLogParserTest {


    private static String[] flows = {
            "Thu Jan 02 08:43:39 CET 2014	GET	10.118.32.164	193.251.214.117	http	webmail.laposte.net	80	/webmail/fr_FR/Images/Images-20130905100226/Images/RightJauge.gif	724	409	false	false",
            "Thu Jan 02 08:43:40 CET 2014	GET	10.118.32.164	193.251.214.117	http	webmail.laposte.net	80	/webmail/fr_FR/Images/Images-20130905100226/Images/fondJauge.gif	723	402	false	false",
            "Thu Jan 02 08:43:42 CET 2014	GET	10.118.32.164	193.252.23.209	http	static1.lecloud.wanadoo.fr	80	/home/fr_FR/20131202100641/img/sprite-icons.pn	495	92518	false	false",
            "Thu Jan 02 08:43:43 CET 2014	GET	10.118.32.164	173.194.66.94	https	www.google.fr	443	/complete/search	736	812	false	false",
            "Thu Jan 02 08:43:45 CET 2014	GET	10.118.32.164	193.251.214.117	http	webmail.laposte.net	80	/webmail/fr_FR/Images/Images-20130905100226/Images/digiposte/archiver-btn.png	736	2179	false	false",
            "Thu Jan 02 08:43:49 CET 2014	GET	10.118.32.164	193.251.214.117	http	webmail.laposte.net	80	/webmail/fr_FR/Images/Images-20130905100226/Images/picto_trash.gif	725	544	false	false"};


    @Test(timeout = 10000)
    public void ParsingBasicTest() throws IOException {
        LogParser parser = new ProxyLogParser();


        Event[] parsedEvents = parser.parse(flows[0]);
        assertTrue(parsedEvents.length == 1);
        assertTrue(parsedEvents[0].get("timestamp").getType().equals("long"));
        assertTrue(parsedEvents[0].get("ipTarget").getValue().equals("193.251.214.117"));
    }
}
