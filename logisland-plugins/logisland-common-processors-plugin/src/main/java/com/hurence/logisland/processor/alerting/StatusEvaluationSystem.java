package com.hurence.logisland.processor.alerting;

import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.record.*;
import com.hurence.logisland.service.cache.CacheService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Created by a.ait-bachir on 11/07/2018.
 */
public class StatusEvaluationSystem extends AbstractNashornSandboxProcessor{

    private static final Logger logger = LoggerFactory.getLogger(CheckAlerts.class);

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

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(CACHE_CLIENT_SERVICE);
        descriptors.add(RECORD_TYPE);
        return Collections.unmodifiableList(descriptors);
    }


    @Override
    protected void setupDynamicProperties(ProcessContext context) {

    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {

        // check if we need initialization
        if (datastoreClientService == null) {
            init(context);
        }

        cacheClientService = context.getPropertyValue(CACHE_CLIENT_SERVICE).asControllerService(CacheService.class);
        if (cacheClientService == null) {
            throw new IllegalArgumentException(CACHE_CLIENT_SERVICE.getName() + " does not resolve to any valid Cache service. " +
                    "Please check your configuration.");
        }


        List<Record> outputRecords = new ArrayList<>();

        for(Record record : records){

            String groupId = record.getField(FieldDictionary.RECORD_GROUP_ID).asString();
            Long recordTimestamp = record.getField(FieldDictionary.RECORD_TIMESTAMP).asLong();
            Long predictionTimestamp = record.getField(FieldDictionary.RECORD_PREDICTION_TIMESTAMP).asLong();

            //Get from Redis the metricId list
            List<Record> metricIdList = cacheClientService.sMembers(groupId);

            // Generate the JS variable initialisation with Cached prediction values in Redis
            StringBuffer jsVariableInit = generateJsVariableInitialisation(metricIdList, groupId, recordTimestamp);
            if(jsVariableInit == null || jsVariableInit.toString().isEmpty()){
                return outputRecords;
            }

            //Get from Redis the JS function definition for a Given GroupId
            //TODO Get the JS according to ServiceCategoryId
            String jsFunction = (String) cacheClientService.getString("js#"+groupId);
            jsVariableInit.append(jsFunction);

            //Evaluate the status
            try{
                sandbox.eval(jsVariableInit.toString());
                Integer statusCode = (Integer) sandbox.get("status1");
                Record statusRecord = new StandardRecord(outputRecordType)
                        .setId(groupId)
                        .setStringField(FieldDictionary.RECORD_VALUE, statusCode.toString())
                        .setField(FieldDictionary.RECORD_PREDICTION_TIMESTAMP, FieldType.LONG, predictionTimestamp);;

                outputRecords.add(statusRecord);

            } catch (ScriptException e) {
                Record errorRecord = new StandardRecord(RecordDictionary.ERROR)
                        .setId(groupId)
                        .addError("ScriptException", e.getMessage());
                outputRecords.add(errorRecord);
                logger.error(e.toString());
            }

        }

        return outputRecords;
    }

    /**
     * Generates the JS for all the metrics of the groupId
     * @param metricIdList
     * @param groupId
     * @param recordTimestamp
     * @return
     */
    public StringBuffer generateJsVariableInitialisation(List<Record> metricIdList, String groupId, Long recordTimestamp){
        StringBuffer res = new StringBuffer();
        for(Record record : metricIdList){

            String metricId = record.getField(FieldDictionary.RECORD_METRIC_ID).asString();
            if(metricId != null && metricId != "null") {
                //Get the prediction value from Redis
                Record prediction = (Record) cacheClientService.get(groupId + "#" + metricId);
                Long recordTimestampCache = prediction.getField(FieldDictionary.RECORD_TIMESTAMP).asLong();
                if (!recordTimestampCache.equals(recordTimestamp)) {
                    return null;
                }
                String metricName = prediction.getField(FieldDictionary.RECORD_METRIC_NAME).asString().replaceAll(" ", "_").replaceAll("\\%", "percent");
                String predictedValue = prediction.getField(FieldDictionary.RECORD_PREDICTION_VALUE).asString();
                res.append("var " + metricName + "=" + predictedValue + ";\n");

                //Add warning and Critical values
                String warningValue = prediction.getField(FieldDictionary.RECORD_METRIC_WARNING).asString();
                String criticalValue = prediction.getField(FieldDictionary.RECORD_METRIC_CRITICAL).asString();
                if (warningValue != "null") res.append("var warning_" + metricName + "=" + warningValue + ";\n");
                if (criticalValue != "null") res.append("var critical_" + metricName + "=" + criticalValue + ";\n");
            }

        }
        logger.debug(res.toString());
        return res;
    }

}
