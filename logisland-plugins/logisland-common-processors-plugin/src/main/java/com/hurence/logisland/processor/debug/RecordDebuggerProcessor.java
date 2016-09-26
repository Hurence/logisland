package com.hurence.logisland.processor.debug;

import com.hurence.logisland.components.AllowableValue;
import com.hurence.logisland.components.PropertyDescriptor;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.processor.AbstractRecordProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.serializer.JsonRecordSerializer;
import com.hurence.logisland.serializer.RecordSerializer;
import com.hurence.logisland.serializer.StringRecordSerializer;
import com.hurence.logisland.validators.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;


public class RecordDebuggerProcessor extends AbstractRecordProcessor {


    static final long serialVersionUID = 3097445175348597100L;

    private static Logger logger = LoggerFactory.getLogger(RecordDebuggerProcessor.class);


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
    public Collection<Record> process(final ProcessContext context, final Collection<Record> collection) {
        if (collection.size() != 0) {
            RecordSerializer serializer = null;
            if(context.getProperty(SERIALIZER).getRawValue().equals(JSON.getValue())){
                serializer = new JsonRecordSerializer();
            }else{
                serializer = new StringRecordSerializer();
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


    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.emptyList();
    }

    @Override
    public String getIdentifier() {
        return null;
    }
}
