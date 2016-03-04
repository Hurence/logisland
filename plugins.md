---
layout: page
title: Plugins
permalink: /plugins/
---

How to extend LogIsland ?

### Dependencies

    

### Maven setup
Create a folder for your `super-plugin` project :

	mkdir -p super-plugin/lib
	mkdir -p src/main/java/com/hurence/logisland

First you need to build log-island and to get the pom and jars availables for your projet

    git clone https://github.com/Hurence/log-island.git 
    cd log-island
    sbt package 
    cp target/logisland-core-0.9.2.jar super-plugin/lib


> log-island jar dependencies are not yet released on maven central (comming soon) so youll need to build them and copy the artefact somewhere in your project (lib folder for instance) in order to use it in your maven project

```
├── lib
│   └── logisland-core-0.9.2.jar
├── pom.xml
├── src
│   ├── main
│   │   ├── java
│   │   │   └── com
│   │   │       └── hurence
│   │   │           └── logisland
│   │   │               └── MyLogParser.java
│   │   └── resources
│   └── test
│       └── java
```

Edit your `pom.xml` as follows

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.hurence.logisland</groupId>
    <artifactId>super-plugin</artifactId>
    <version>1.0-SNAPSHOT</version>

    <dependencies>
        <dependency>
            <groupId>com.hurence.logisland</groupId>
            <artifactId>logisland-core</artifactId>
            <version>0.9.2</version>
            <scope>system</scope>
            <systemPath>${project.basedir}/lib/logisland-core-0.9.2.jar</systemPath>
        </dependency>
    </dependencies>
</project>
```


Write your a custom LogParser for your super-plugin in `/src/main/java/com/hurence/logisland/MyLogParser.java`

Our parser will analyze some Proxy Log String in the following form :

	"Thu Jan 02 08:43:39 CET 2014	GET	10.118.32.164	193.251.214.117	http	webmail.laposte.net	80	/webmail/fr_FR/Images/Images-20130905100226/Images/RightJauge.gif	724	409	false	false"



```java
package com.hurence.logisland;

import com.hurence.logisland.event.Event;
import com.hurence.logisland.log.LogParser;
import com.hurence.logisland.log.LogParserException;

import java.text.SimpleDateFormat;

/**
 * NetworkFlow(
 * timestamp: Long,
 * method: String,
 * ipSource: String,
 * ipTarget: String,
 * urlScheme: String,
 * urlHost: String,
 * urlPort: String,
 * urlPath: String,
 * requestSize: Int,
 * responseSize: Int,
 * isOutsideOfficeHours: Boolean,
 * isHostBlacklisted: Boolean,
 * tags: String)
 */
public class ProxyLogParser implements LogParser {

    /**
     * take a line of csv and convert it to a NetworkFlow
     *
     * @param s
     * @return
     */
    public Event[] parse(String s) throws LogParserException {


        Event event = new Event();

        try {
            String[] records = s.split("\t");

            try {
                SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy");
                event.put("timestamp", "long", sdf.parse(records[0]).getTime());
            } catch (Exception e) {
                event.put("parsing_error", e.getMessage());
            }

            event.put("method", "string", records[1]);
            event.put("ipSource", "string", records[2]);
            event.put("ipTarget", "string", records[3]);
            event.put("urlScheme", "string", records[4]);
            event.put("urlHost", "string", records[5]);
            event.put("urlPort", "string", records[6]);
            event.put("urlPath", "string", records[7]);

            try {
                event.put("requestSize", "int", Integer.parseInt(records[8]));
            } catch (Exception e) {
                event.put("parsing_error", e.getMessage());
            }
            try {
                event.put("responseSize", "int", Integer.parseInt(records[9]));
            } catch (Exception e) {
                event.put("parsing_error", e.getMessage());
            }
            try {
                event.put("isOutsideOfficeHours", "bool", Boolean.parseBoolean(records[10]));
            } catch (Exception e) {
                event.put("parsing_error", e.getMessage());
            }
            try {
                event.put("isHostBlacklisted", "bool", Boolean.parseBoolean(records[11]));
            } catch (Exception e) {
                event.put("parsing_error", e.getMessage());
            }


            if (records.length == 13) {
                String tags = records[12].replaceAll("\"", "").replaceAll("\\[", "").replaceAll("\\]", "");
                event.put("tags", "string", tags);
            }


        }catch (Exception e) {
            event.put("parsing_error", e.getMessage());
        }

        Event[] result = new Event[1];
        result[0] = event;

        return result;
    }

}
```


which can be tested (not really deeply ...) with a small unit test

```java
package com.hurence.logisland;


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
```


