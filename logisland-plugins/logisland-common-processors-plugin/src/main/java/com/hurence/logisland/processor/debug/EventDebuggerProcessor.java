package com.hurence.logisland.processor.debug;

import com.hurence.logisland.components.AllowableValue;
import com.hurence.logisland.components.PropertyDescriptor;
import com.hurence.logisland.event.Event;
import com.hurence.logisland.event.EventField;
import com.hurence.logisland.processor.AbstractEventProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.serializer.EventJsonSerializer;
import com.hurence.logisland.serializer.EventSerializer;
import com.hurence.logisland.serializer.EventStringSerializer;
import com.hurence.logisland.validators.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;


public class EventDebuggerProcessor extends AbstractEventProcessor {

    private static Logger logger = LoggerFactory.getLogger(EventDebuggerProcessor.class);


    public static final AllowableValue JSON = new AllowableValue("json", "Json serialization",
            "serialize events as json blocs");

    public static final AllowableValue STRING = new AllowableValue("string", "String serialization",
            "serialize events as toString() blocs");

    public static final PropertyDescriptor SERIALIZER = new PropertyDescriptor.Builder()
            .name("event.serializer")
            .description("the way to serialize event")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(JSON.getValue())
            .allowableValues(JSON, STRING)
            .build();




    @Override
    public Collection<Event> process(final ProcessContext context, final Collection<Event> collection) {
        if (collection.size() != 0) {
            EventSerializer serializer = null;
            if(context.getProperty(SERIALIZER).getValue().equals(JSON.getValue())){
                serializer = new EventJsonSerializer();
            }else{
                serializer = new EventStringSerializer();
            }


            final EventSerializer finalSerializer = serializer;
            collection.stream().forEach(event -> {



                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    finalSerializer.serialize(baos, event);
                    try {
                        baos.close();
                    } catch (IOException e) {
                        logger.debug("error {} ", e.getCause());
                    }

                logger.info(new String(baos.toByteArray()));


            });
        }


        return Collections.emptyList();
    }


    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.emptyList();
    }

    @Override
    public String getIdentifier() {
        return null;
    }
}
