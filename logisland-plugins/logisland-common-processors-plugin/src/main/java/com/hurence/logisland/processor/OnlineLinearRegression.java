package com.hurence.logisland.processor;

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.*;
import com.hurence.logisland.service.cache.CacheService;
import com.hurence.logisland.validator.StandardValidators;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@Tags({"record", "linear-regression", "prediction"})
@CapabilityDescription("Perform a training and prediction on records. Each record must have two mondatory fields: timestamp, value." +
        "All the records must be related to the same measurement Id.")
public class OnlineLinearRegression  extends AbstractProcessor {

    private static final Logger logger = LoggerFactory.getLogger(OnlineLinearRegression.class);

    protected CacheService cacheClientService;

    public static final PropertyDescriptor CACHE_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("cache.client.service")
            .description("The instance of the Controller Service to use for accessing the cache.")
            .required(true)
            .identifiesControllerService(CacheService.class)
            .build();

    public static final PropertyDescriptor RECORD_TYPE = new PropertyDescriptor.Builder()
            .name("record.type")
            .description("default type of record")
            .required(false)
            .defaultValue("record")
            .build();

    public static final PropertyDescriptor TRAINING_HISTORY_SIZE = new PropertyDescriptor.Builder()
            .name("training.history.size")
            .description("History size of for the training in terms of number of points.")
            .required(true)
            .defaultValue("true")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor PREDICTION_HORIZON_SIZE = new PropertyDescriptor.Builder()
            .name("prediction.horizon.size")
            .description("Predction horizon : number of seconds in the futur to add to the present timestamp.")
            .required(true)
            .defaultValue("true")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor KEEP_RAW_CONTENT = new PropertyDescriptor.Builder()
            .name("keep.raw.content")
            .description("do we add the initial raw content ?")
            .required(false)
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(CACHE_CLIENT_SERVICE);
        descriptors.add(RECORD_TYPE);
        descriptors.add(TRAINING_HISTORY_SIZE);
        descriptors.add(PREDICTION_HORIZON_SIZE);
        descriptors.add(KEEP_RAW_CONTENT);
        return Collections.unmodifiableList(descriptors);
    }

    @Override
    public void init(ProcessContext context) {
        cacheClientService = context.getPropertyValue(CACHE_CLIENT_SERVICE).asControllerService(CacheService.class);
        if (cacheClientService == null) {
            throw new IllegalArgumentException(CACHE_CLIENT_SERVICE.getName() + " does not resolve to any valid Cache service. " +
                    "Please check your configuration.");
        }
    }

    @Override
    public boolean hasControllerService() {
        return true;
    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {

        final String eventType = context.getPropertyValue(RECORD_TYPE).asString();
        final Long trainingHistorySize = context.getPropertyValue(TRAINING_HISTORY_SIZE).asLong();
        final int predictionHorizonSize = context.getPropertyValue(PREDICTION_HORIZON_SIZE).asInteger();
        final boolean keepRawContent = context.getPropertyValue(KEEP_RAW_CONTENT).asBoolean();

        /**
         * Perform the training and send the result:
         * model: slope, intercept
         * predicted value
         * prediction timestamp
         */
        List<Record> outputRecords = new ArrayList<>();
        StandardRecord outputRecord = new StandardRecord(eventType);

        // creating regression object, passing true to have intercept term
        SimpleRegression simpleRegression = new SimpleRegression(true);
        if(records.size() != 0) {

            long currentTimestamp = (new Date()).getTime() / 1000;

            Record record =  records.iterator().next();
            String metricId = record.getField("metricId").asString();
            String metricName = record.getField("metricName").asString();
            String groupId = record.getField("groupId").asString();
            int normalCheckWindow = record.getField("normalCheckWindow").asInteger();
            long pastTimestamp = currentTimestamp - (normalCheckWindow * trainingHistorySize) ;

            Record lastTrainingTimestampRecord = (Record) cacheClientService.get("#"+metricId);
            if(lastTrainingTimestampRecord.getField(FieldDictionary.RECORD_TRAINING_TIMESTAMP) != null) {
                long lastTrainingTimestamp = lastTrainingTimestampRecord.getField(FieldDictionary.RECORD_TRAINING_TIMESTAMP).asLong();
                if((currentTimestamp - lastTrainingTimestamp) < normalCheckWindow) return outputRecords;
            }

            List<Record> recordsFromRedis = cacheClientService.get(metricId, pastTimestamp, currentTimestamp, trainingHistorySize );
            if(recordsFromRedis != null) {
                double[][] data = new double[trainingHistorySize.intValue()][2];
                int cptRows = 0;
                for (Record recordRedis : recordsFromRedis) {
                    Long timestamp = recordRedis.getField("timestamp").asLong();
                    float value = recordRedis.getField("value").asFloat();
                    data[cptRows][0] = timestamp;
                    data[cptRows][1] = value;
                    cptRows++;
                }

                simpleRegression.addData(data);

                long predictionTimestamp = currentTimestamp + predictionHorizonSize;
                double predicted_value = simpleRegression.predict(predictionTimestamp);

                // Set the field values
                outputRecord.setField(FieldDictionary.RECORD_GROUP_ID, FieldType.STRING, groupId);
                outputRecord.setField(FieldDictionary.RECORD_METRIC_ID, FieldType.STRING, metricId);
                outputRecord.setField(FieldDictionary.RECORD_METRIC_NAME, FieldType.STRING, metricName);
                outputRecord.setField(FieldDictionary.RECORD_SLOPE, FieldType.DOUBLE, simpleRegression.getSlope());
                outputRecord.setField(FieldDictionary.RECORD_INTERCEPT, FieldType.DOUBLE, simpleRegression.getIntercept());
                outputRecord.setField(FieldDictionary.RECORD_TRAINING_TIMESTAMP, FieldType.LONG, currentTimestamp);
                outputRecord.setField(FieldDictionary.RECORD_PREDICTION_TIMESTAMP, FieldType.LONG, predictionTimestamp);
                outputRecord.setField(FieldDictionary.RECORD_PREDICTION_VALUE, FieldType.DOUBLE, predicted_value);

                //Save the prediction to Redis
                cacheClientService.set(groupId + "#" + metricId, predictionTimestamp, outputRecord);

                //Save the last training timestamp to Redis
                StandardRecord redisRecord = new StandardRecord(eventType);
                redisRecord.setField(FieldDictionary.RECORD_TRAINING_TIMESTAMP, FieldType.LONG, currentTimestamp);
                cacheClientService.set("#" + metricId, redisRecord);

                outputRecords.add(outputRecord);
            }
        }

        return outputRecords;
    }
}
