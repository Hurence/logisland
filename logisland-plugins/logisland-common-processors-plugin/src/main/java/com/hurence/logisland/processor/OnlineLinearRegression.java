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

import java.sql.Time;
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
            .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
            .build();

    public static final PropertyDescriptor TRAINING_TIMELAPSE = new PropertyDescriptor.Builder()
            .name("training.timelapse")
            .description("Timelapse to wait before to trigger a new training.")
            .required(true)
            .defaultValue("true")
            .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
            .build();


    public static final PropertyDescriptor PREDICTION_HORIZON_SIZE = new PropertyDescriptor.Builder()
            .name("prediction.horizon.size")
            .description("Predction horizon : number of seconds in the futur to add to the present timestamp.")
            .required(true)
            .defaultValue("true")
            .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
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
        descriptors.add(TRAINING_TIMELAPSE);
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
        final Map<Long, Long> predictionHorizonSizeMap = context.getPropertyValue(PREDICTION_HORIZON_SIZE).asMapLong();
        final Map<Long, Long> trainingHistorySizeMap = context.getPropertyValue(TRAINING_HISTORY_SIZE).asMapLong();
        final Map<Long, Long> trainingTimelapseMap = context.getPropertyValue(TRAINING_TIMELAPSE).asMapLong();
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
            String metricWarning = record.getField("warnThreshold").asString();
            String metricCritical = record.getField("critThreshold").asString();
            String groupId = record.getField("groupId").asString();
            Long normalCheckWindow = record.getField("normalCheckWindow").asLong();
            Long timestampMetric = record.getField("timestamp").asLong();

            Long trainingHistorySize = getMapValueInterval(trainingHistorySizeMap,  normalCheckWindow);
            Long predictionHorizonSize = getMapValueInterval(predictionHorizonSizeMap,  normalCheckWindow);
            Long predictionTimelapse = getMapValueInterval(trainingTimelapseMap,  normalCheckWindow);

            long pastTimestamp = currentTimestamp - (normalCheckWindow * trainingHistorySize) ;

            Record lastTrainingTimestampRecord = (Record) cacheClientService.get("#"+metricId);
            if(lastTrainingTimestampRecord.getField(FieldDictionary.RECORD_TRAINING_TIMESTAMP) != null) {
                long lastTrainingTimestamp = lastTrainingTimestampRecord.getField(FieldDictionary.RECORD_TRAINING_TIMESTAMP).asLong();
                if((currentTimestamp - lastTrainingTimestamp) < predictionTimelapse) return outputRecords;
            }

            List<Record> recordsFromRedis = cacheClientService.get(metricId, pastTimestamp, currentTimestamp, trainingHistorySize );
            if(recordsFromRedis != null && recordsFromRedis.size() >= trainingHistorySize) {
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
                outputRecord.setField(FieldDictionary.RECORD_METRIC_WARNING, FieldType.STRING, metricWarning);
                outputRecord.setField(FieldDictionary.RECORD_METRIC_CRITICAL, FieldType.STRING, metricCritical);
                outputRecord.setField(FieldDictionary.RECORD_SLOPE, FieldType.DOUBLE, simpleRegression.getSlope());
                outputRecord.setField(FieldDictionary.RECORD_INTERCEPT, FieldType.DOUBLE, simpleRegression.getIntercept());
                outputRecord.setField(FieldDictionary.RECORD_TIMESTAMP, FieldType.LONG, timestampMetric);
                outputRecord.setField(FieldDictionary.RECORD_TRAINING_TIMESTAMP, FieldType.LONG, currentTimestamp);
                outputRecord.setField(FieldDictionary.RECORD_PREDICTION_TIMESTAMP, FieldType.LONG, predictionTimestamp);
                outputRecord.setField(FieldDictionary.RECORD_PREDICTION_VALUE, FieldType.DOUBLE, predicted_value);


                //Save the prediction to Redis
                //cacheClientService.set(groupId + "#" + metricId, timestampMetric, outputRecord);
                cacheClientService.set(groupId + "#" + metricId, outputRecord);

                //Save the last training timestamp to Redis
                StandardRecord redisRecordTrainingTimestamp = new StandardRecord(eventType);
                redisRecordTrainingTimestamp.setField(FieldDictionary.RECORD_TRAINING_TIMESTAMP, FieldType.LONG, currentTimestamp);
                cacheClientService.set("#" + metricId, redisRecordTrainingTimestamp);

                outputRecords.add(outputRecord);

                //Clear metric measurements past data in Redis Cache
                Long max_timestamp = new Long(pastTimestamp - normalCheckWindow);
                cacheClientService.remove(metricId,0L, max_timestamp);

                //Add the metricId to the groupId set
                LightRecord redisGroupIdMetrics = new LightRecord(eventType);
                redisGroupIdMetrics.setField(FieldDictionary.RECORD_METRIC_ID, FieldType.STRING, metricId);
                cacheClientService.sAdd(groupId, redisGroupIdMetrics);

            }
        }

        return outputRecords;
    }

    /**
     * Get a value from a Map<Long,Long>
     *     If the key does not exist then get the value of smallest key which is greater than
     *     keySearch.
     *     Example: map={(5,20),(10,40),(20,60)}
     *     Result for map.get(7) = 40 because the nearest key to 7 is 10.
     * @param sizeMap
     * @param keySearch
     * @return the value of the keySearch
     */
    private Long getMapValueInterval(Map<Long, Long> sizeMap, Long keySearch){
        Long size = sizeMap.get(keySearch);
        if(size == null) {
            List<Long> keyList = new ArrayList(sizeMap.keySet());
            keyList.sort(Comparator.naturalOrder());
            boolean found = false;
            Iterator<Long> it = keyList.iterator();
            Long key = null;
            while(it.hasNext() && !found){
                key  = it.next();
                if(key < keySearch ){
                    size = sizeMap.get(key);
                    found = true;
                }
            }
            if(!found){
                //get the value of the last key
                size = sizeMap.get(key);
            }
        }
        return size;
    }
}
