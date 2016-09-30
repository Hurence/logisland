package com.hurence.logisland.processor;

import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.ComponentContext;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.serializer.JsonSerializer;
import com.hurence.logisland.serializer.RecordSerializer;
import com.hurence.logisland.serializer.StringSerializer;
import com.hurence.logisland.validator.StandardPropertyValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;


public class RecordDebugger extends AbstractProcessor {


    static final long serialVersionUID = 3097445175348597100L;

    private static Logger logger = LoggerFactory.getLogger(RecordDebugger.class);


    public static final AllowableValue JSON = new AllowableValue("json", "Json serialization",
            "serialize events as json blocs");

    public static final AllowableValue STRING = new AllowableValue("string", "String serialization",
            "serialize events as toString() blocs");

    public static final PropertyDescriptor SERIALIZER = new PropertyDescriptor.Builder()
            .name("event.serializer")
            .description("the way to serialize event")
            .required(true)
            .addValidator(StandardPropertyValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(JSON.getValue())
            .allowableValues(JSON, STRING)
            .build();


    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(SERIALIZER);

        return Collections.unmodifiableList(descriptors);
    }


    @Override
    public Collection<Record> process(final ComponentContext context, final Collection<Record> collection) {
        if (collection.size() != 0) {
            RecordSerializer serializer = null;
            if(context.getProperty(SERIALIZER).getRawValue().equals(JSON.getValue())){
                serializer = new JsonSerializer();
            }else{
                serializer = new StringSerializer();
            }


            final RecordSerializer finalSerializer = serializer;
            collection.forEach(event -> {


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



}
