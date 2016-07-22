package com.hurence.logisland.processor.parser;

import com.hurence.logisland.components.PropertyDescriptor;
import com.hurence.logisland.event.Event;
import com.hurence.logisland.log.AbstractLogParser;
import com.hurence.logisland.log.LogParserException;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.validators.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by tom on 21/07/16.
 */
public class SplitText extends AbstractLogParser {

    private static Logger logger = LoggerFactory.getLogger(SplitText.class);


    public static final PropertyDescriptor REGEX = new PropertyDescriptor.Builder()
            .name("regex")
            .description("the regex to match")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor FIELDS = new PropertyDescriptor.Builder()
            .name("fields")
            .description("the list of fields corresponding to groups")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(ERROR_TOPICS);
        descriptors.add(INPUT_TOPICS);
        descriptors.add(OUTPUT_TOPICS);
        descriptors.add(OUTPUT_SCHEMA);
        descriptors.add(REGEX);
        descriptors.add(FIELDS);

        return Collections.unmodifiableList(descriptors);
    }

    @Override
    public Collection<Event> parse(ProcessContext context, String lines) throws LogParserException {



        final String regexString = context.getProperty(REGEX).getValue();
        final Pattern regex = Pattern.compile(regexString);


        try {


            Matcher matcher = regex.matcher(lines);



/*
            Assert.assertTrue(String.format("'%s' did not match the regex '%s'", line, usrBackendRegex.toString()), matcher.matches());
            Assert.assertEquals(7, matcher.groupCount());
            Assert.assertEquals("2016-07-12 02:00:00.000  INFO   [schedule-tasks-9              ] --- c.l.m.b.s.i.SchedulingTaskConfiguration  - SESSION[NONE] - USERID[NONE] - Starting internal job 'class com.lotsys.motors.backend.manager.jobs.OLTPGameModelCleanCacheManager'", matcher.group(0));
            Assert.assertEquals("2016-07-12 02:00:00.000", matcher.group(1));
            Assert.assertEquals("INFO", matcher.group(2));
            Assert.assertEquals("schedule-tasks-9              ", matcher.group(3));
            Assert.assertEquals("c.l.m.b.s.i.SchedulingTaskConfiguration", matcher.group(4));
            Assert.assertEquals("NONE", matcher.group(5));
            Assert.assertEquals("NONE", matcher.group(6));
            Assert.assertEquals("Starting internal job 'class com.lotsys.motors.backend.manager.jobs.OLTPGameModelCleanCacheManager'", matcher.group(7));*/

        } catch (Exception e) {
            logger.warn("issue while matching regex {} on string {}", regexString, lines);
        }



        return Collections.emptyList();
    }

    @Override
    public String getIdentifier() {
        return null;
    }



}
