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
public class SplitTextMultiline extends AbstractLogParser {

    static final long serialVersionUID = -1L;

    private static Logger logger = LoggerFactory.getLogger(SplitTextMultiline.class);


    public static final PropertyDescriptor REGEX = new PropertyDescriptor.Builder()
            .name("regex")
            .description("the regex to match")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor FIELDS = new PropertyDescriptor.Builder()
            .name("fields")
            .description("a comma separated list of fields corresponding to matching groups")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor EVENT_TYPE = new PropertyDescriptor.Builder()
            .name("event.type")
            .description("the type of event")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private String buffer = "";



    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(ERROR_TOPICS);
        descriptors.add(INPUT_TOPICS);
        descriptors.add(OUTPUT_TOPICS);
        descriptors.add(OUTPUT_SCHEMA);
        descriptors.add(REGEX);
        descriptors.add(FIELDS);
        descriptors.add(EVENT_TYPE);

        return Collections.unmodifiableList(descriptors);
    }

    @Override
    public Collection<Event> parse(ProcessContext context, String key, String lines) throws LogParserException {

        final String[] fields = context.getProperty(FIELDS).getValue().split(",");
        final String regexString = context.getProperty(REGEX).getValue();
        final String eventType = context.getProperty(EVENT_TYPE).getValue();
        final Pattern regex = Pattern.compile(regexString, Pattern.DOTALL);

        List<Event> events = new ArrayList<>();

        buffer += lines;

        try {
            Matcher matcher = regex.matcher(buffer);
            if(matcher.find()){
                Event event = new Event(eventType);
                for (int i = 0; i < matcher.groupCount() +1  && i < fields.length; i++) {
                    String content = matcher.group(i);
                    if(content != null){
                        event.put(fields[i], "string", matcher.group(i).replaceAll("\"",""));
                    }

                }
                events.add(event);
                buffer = "";
            }else {
                logger.warn("no match");
            }
        } catch (Exception e) {
            logger.warn("issue while matching regex {} on string {} exception {}", regexString, lines, e.getMessage());
        }


        return events;
    }

    @Override
    public String getIdentifier() {
        return null;
    }


}
