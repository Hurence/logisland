package com.hurence.logisland.processor;

import com.caseystella.analytics.DataPoint;
import com.caseystella.analytics.outlier.Outlier;
import com.caseystella.analytics.outlier.Severity;
import com.caseystella.analytics.outlier.streaming.OutlierAlgorithm;
import com.caseystella.analytics.outlier.streaming.OutlierConfig;
import com.caseystella.analytics.util.JSONUtil;
import com.hurence.logisland.components.PropertyDescriptor;
import com.hurence.logisland.event.Event;
import com.hurence.logisland.validators.StandardValidators;
import com.hurence.logisland.utils.string.Multiline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Outlier Analysis: A Hybrid Approach
 * <p/>
 * In order to function at scale, a two-phase approach is taken
 * <p/>
 * - For every data point
 * - Detect outlier candidates using a robust estimator of variability (e.g. median absolute deviation) that uses distributional sketching (e.g. Q-trees)
 * - Gather a biased sample (biased by recency)
 * - Extremely deterministic in space and cheap in computation
 * <p/>
 * - For every outlier candidate
 * - Use traditional, more computationally complex approaches to outlier analysis (e.g. Robust PCA) on the biased sample
 * - Expensive computationally, but run infrequently
 * <p/>
 * This becomes a data filter which can be attached to a timeseries data stream within a distributed computational framework (i.e. Storm, Spark, Flink, NiFi) to detect outliers.
 */
public class OutlierProcessor extends AbstractEventProcessor {

    public static String EVENT_TYPE = "sensor_outlier";
    public static String EVENT_PARSING_EXCEPTION_TYPE = "event_parsing_exception";
    public static String OUTLIER_PROCESSING_EXCEPTION_TYPE = "outlier_processing_exception";

    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesCsvLoader.class);
    OutlierConfig outlierConfig;
    OutlierAlgorithm sketchyOutlierAlgorithm;
    com.caseystella.analytics.outlier.batch.OutlierAlgorithm batchOutlierAlgorithm;



    public static final PropertyDescriptor ROTATION_POLICY_TYPE = new PropertyDescriptor.Builder()
            .name("Rotation Policy Type")
            .description("...")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("BY_AMOUNT")
            .build();

    public static final PropertyDescriptor ROTATION_POLICY_AMOUNT = new PropertyDescriptor.Builder()
            .name("Rotation Policy Amount")
            .description("...")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("100")
            .build();

    public static final PropertyDescriptor ROTATION_POLICY_UNIT = new PropertyDescriptor.Builder()
            .name("Rotation Policy Amount")
            .description("...")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("100")
            .build();

    public static final PropertyDescriptor CHUNKING_POLICY_TYPE = new PropertyDescriptor.Builder()
            .name("Chunking Policy Type")
            .description("...")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("BY_AMOUNT")
            .build();

    public static final PropertyDescriptor CHUNKING_POLICY_AMOUNT = new PropertyDescriptor.Builder()
            .name("Chunking Policy Amount")
            .description("...")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("100")
            .build();

    public static final PropertyDescriptor CHUNKING_POLICY_UNIT = new PropertyDescriptor.Builder()
            .name("Chunking Policy Amount")
            .description("...")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("100")
            .build();


    public static final PropertyDescriptor SKETCHY_OUTLIER_ALGORITHM  = new PropertyDescriptor.Builder()
            .name("Sketchy outlier algorithm")
            .description("...")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues("SKETCHY_MOVING_MAD")
            .defaultValue("SKETCHY_MOVING_MAD")
            .build();

    public static final PropertyDescriptor BATCH_OUTLIER_ALGORITHM  = new PropertyDescriptor.Builder()
            .name("Batch outlier algorithm")
            .description("...")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues("RAD")
            .defaultValue("RAD")
            .build();

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(INPUT_TOPICS);
        descriptors.add(OUTPUT_TOPICS);
        descriptors.add(ERROR_TOPICS);
        descriptors.add(ROTATION_POLICY_TYPE);
        descriptors.add(ROTATION_POLICY_AMOUNT);
        descriptors.add(ROTATION_POLICY_UNIT);
        descriptors.add(CHUNKING_POLICY_TYPE);
        descriptors.add(CHUNKING_POLICY_AMOUNT);
        descriptors.add(CHUNKING_POLICY_UNIT);
        descriptors.add(SKETCHY_OUTLIER_ALGORITHM);
        descriptors.add(BATCH_OUTLIER_ALGORITHM);

        return Collections.unmodifiableList(descriptors);
    }

    /**
      {
      "rotationPolicy" : {
            "type" : "BY_AMOUNT"
            ,"amount" : 100
            ,"unit" : "POINTS"
      }
      ,"chunkingPolicy" : {
            "type" : "BY_AMOUNT"
            ,"amount" : 10
            ,"unit" : "POINTS"
      }
      ,"globalStatistics" : {
      }
      ,"sketchyOutlierAlgorithm" : "SKETCHY_MOVING_MAD"
      ,"batchOutlierAlgorithm" : "RAD"
      ,"config" : {
            "minAmountToPredict" : 100
            ,"reservoirSize" : 100
            ,"zscoreCutoffs" : {
                "NORMAL" : 0.000000000000001
                ,"MODERATE_OUTLIER" : 1.5
            }
            ,"minZscorePercentile" : 95
      }
      }
     */
    @Multiline
    public static String streamingOutlierConfigStr;

    public OutlierProcessor() throws IOException {

        this.outlierConfig = JSONUtil.INSTANCE.load(streamingOutlierConfigStr,
                com.caseystella.analytics.outlier.streaming.OutlierConfig.class
        );
        outlierConfig.getSketchyOutlierAlgorithm().configure(outlierConfig);
        sketchyOutlierAlgorithm = outlierConfig.getSketchyOutlierAlgorithm();
        sketchyOutlierAlgorithm.configure(outlierConfig);
        batchOutlierAlgorithm = outlierConfig.getBatchOutlierAlgorithm();
        batchOutlierAlgorithm.configure(outlierConfig);
    }


    /**
     *
     */
    @Override
    public Collection<Event> process(final ProcessContext context, final Collection<Event> events) {

        Collection list = new ArrayList();


        // loop over all events in collection
        for (Event event : events) {

            try {

                // convert an event to a dataPoint.
                long timestamp = (long) event.get("timestamp").getValue();
                double value = (double) event.get("value").getValue();

                DataPoint dp = new DataPoint(timestamp, value, new HashMap<String, String>(), "kafka_topic");


                // now let's look for outliers
                Outlier outlier = sketchyOutlierAlgorithm.analyze(dp);
                if (outlier.getSeverity() == Severity.SEVERE_OUTLIER) {

                    outlier = batchOutlierAlgorithm.analyze(outlier, outlier.getSample(), dp);
                    if (outlier.getSeverity() == Severity.SEVERE_OUTLIER) {

                        Event evt = new Event(EVENT_TYPE);
                        evt.put("root_event_value", "double", event.get("value").getValue());
                        evt.put("root_event_id", "string", event.getId());
                        evt.put("root_event_type", "string", event.getType());
                        evt.put("severity", "string", outlier.getSeverity());
                        evt.put("score", "string", outlier.getScore());
                        evt.put("num_points", "string", outlier.getNumPts());
                        list.add(evt);


                    }/*else{
                        logger.info("outlier not so severe");
                    }*/
                }

            } catch (RuntimeException e) {

                Event evt = new Event(OUTLIER_PROCESSING_EXCEPTION_TYPE);
                evt.put("message", "string", e);
                list.add(evt);
              //  logger.info(e.getMessage(), e);
            }
        }

        return list;
    }

    @Override
    public String getIdentifier() {
        return null;
    }
}
