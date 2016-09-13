/*
 Copyright 2016 Hurence

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */
package com.hurence.logisland.parser.apache

import java.text.SimpleDateFormat
import java.util
import java.util.Collections
import java.util.regex.Pattern

import com.hurence.logisland.components.PropertyDescriptor
import com.hurence.logisland.event.Event
import com.hurence.logisland.log.AbstractLogParser
import com.hurence.logisland.processor.ProcessContext
import com.hurence.logisland.validators.StandardValidators
import org.slf4j.LoggerFactory


/**
  * Parse an Apache log file with Regular Expressions
  */
class ApacheLogParser extends AbstractLogParser {

    val simplePattern = Pattern.compile("^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+)")
    val combinedPattern = Pattern.compile("^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"")

    val requestPattern = Pattern.compile("(\\S*) (\\S*) (\\S*)")
    private val sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z")
    private val logger = LoggerFactory.getLogger(classOf[ApacheLogParser])


    val EVENT_TYPE = new PropertyDescriptor.Builder()
        .name("event.type")
        .description("the type of event")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue("apache_log").build

    val KEY_REGEX = new PropertyDescriptor.Builder()
        .name("key.regex")
        .description("the regex to match for the message key")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue("").build

    val KEY_FIELDS = new PropertyDescriptor.Builder()
        .name("key.fields")
        .description("a comma separated list of fields corresponding to matching groups for the message key")
        .required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue("").build

    override def getSupportedPropertyDescriptors: util.List[PropertyDescriptor] = {
        val descriptors = new util.ArrayList[PropertyDescriptor]
        descriptors.add(AbstractLogParser.ERROR_TOPICS)
        descriptors.add(AbstractLogParser.INPUT_TOPICS)
        descriptors.add(AbstractLogParser.OUTPUT_TOPICS)
        descriptors.add(AbstractLogParser.OUTPUT_SCHEMA)

        descriptors.add(EVENT_TYPE)
        descriptors.add(KEY_REGEX)
        descriptors.add(KEY_FIELDS)
        Collections.unmodifiableList(descriptors)
    }

    override def parse(context: ProcessContext, key: String, value: String): util.Collection[Event] = {
        val keyFields = context.getProperty(KEY_FIELDS).getValue.split(",")
        val keyRegexString = context.getProperty(KEY_REGEX).getValue
        val keyRegex = Pattern.compile(keyRegexString)

        val event = new Event(context.getProperty(EVENT_TYPE).getValue)
        event.put("raw_content", "string", value)

        // match the key
        if (key != null) {
            val keyMatcher = keyRegex.matcher(key)
            if (keyMatcher.matches) {
                var i = 0
                while (i < keyMatcher.groupCount + 1 && i < keyFields.length) {
                    try {
                        val content = keyMatcher.group(i)
                        if (content != null) {
                            event.put(keyFields(i), "string", keyMatcher.group(i + 1).replaceAll("\"", ""))
                        }

                    } catch {
                        case t: Throwable => logger.info("no match for key regex {}", keyRegexString)
                    } finally {
                        i += 1
                    }
                }
            }
        }


        // match the value
        val matcher = combinedPattern.matcher(value)
        val simpleMatcher = simplePattern.matcher(value)
        if (matcher.matches()) {
            event.put("dest_ip", "string", matcher.group(1))
            event.put("user", "string", matcher.group(3))
            event.put("event_time", "long", sdf.parse(matcher.group(4)).getTime)
            event.put("http_request", "string", matcher.group(5))
            event.put("status", "string", matcher.group(6))
            event.put("bytes_out", "int", matcher.group(7).toInt)
            event.put("referer", "string", matcher.group(8))
            event.put("user_agent", "string", matcher.group(9))
        } else if (simpleMatcher.matches()) {
            event.put("dest_ip", "string", simpleMatcher.group(1))
            event.put("user", "string", simpleMatcher.group(3))
            event.put("event_time", "long", sdf.parse(simpleMatcher.group(4)).getTime)
            event.put("http_request", "string", simpleMatcher.group(5))
            event.put("status", "string", simpleMatcher.group(6))
            event.put("bytes_out", "int", simpleMatcher.group(7).toInt)
        } else {
            event.put("error", "string", "bad log entry (or problem with RE?)")
        }

        // add some further processing
        try {
            if (event.get("http_request") != null) {
                val requestMatcher = requestPattern.matcher(event.get("http_request").getValue.toString)
                if (requestMatcher.matches()) {
                    event.put("http_method", "string", requestMatcher.group(1))
                    event.put("http_url", "string", requestMatcher.group(2))
                    event.put("http_protocol", "string", requestMatcher.group(3))
                }
            }
        }
        Collections.singletonList(event)
    }

    override def getIdentifier: String = null
}




