/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.processor;

import com.caseystella.analytics.DataPoint;
import com.caseystella.analytics.distribution.GlobalStatistics;
import com.caseystella.analytics.distribution.config.Type;
import com.caseystella.analytics.distribution.config.Unit;
import com.caseystella.analytics.outlier.Outlier;
import com.caseystella.analytics.outlier.Severity;
import com.caseystella.analytics.outlier.batch.rpca.RPCAOutlierAlgorithm;
import com.caseystella.analytics.outlier.streaming.OutlierAlgorithm;
import com.caseystella.analytics.outlier.streaming.OutlierConfig;
import com.caseystella.analytics.outlier.streaming.mad.SketchyMovingMAD;
import com.hurence.logisland.annotation.behavior.Stateful;
import com.hurence.logisland.annotation.documentation.*;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.validator.ValidationContext;
import com.hurence.logisland.validator.ValidationResult;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.validator.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


@Category(ComponentCategory.ANALYTICS)
@Stateful
@Tags({"analytic", "outlier", "record", "iot", "timeseries"})
@CapabilityDescription("Outlier Analysis: A Hybrid Approach\n" +
        "\n" +
        "In order to function at scale, a two-phase approach is taken\n" +
        "\n" +
        "For every data point\n" +
        "\n" +
        "- Detect outlier candidates using a robust estimator of variability (e.g. median absolute deviation) that uses distributional sketching (e.g. Q-trees)\n" +
        "- Gather a biased sample (biased by recency)\n" +
        "- Extremely deterministic in space and cheap in computation\n" +
        "\n" +
        "For every outlier candidate\n" +
        "\n" +
        "- Use traditional, more computationally complex approaches to outlier analysis (e.g. Robust PCA) on the biased sample\n" +
        "- Expensive computationally, but run infrequently\n" +
        "\n" +
        "This becomes a data filter which can be attached to a timeseries data stream within a distributed computational framework (i.e. Storm, Spark, Flink, NiFi) to detect outliers.")
@ExtraDetailFile("./details/DetectOutliers-Detail.rst")
public class DetectOutliers extends AbstractProcessor {

    static final long serialVersionUID = -1L;

    public static String EVENT_TYPE = "outlier";
    public static String OUTLIER_PROCESSING_EXCEPTION_TYPE = "outlier_processing_exception";

    private static final Logger logger = LoggerFactory.getLogger(DetectOutliers.class);
    private OutlierConfig outlierConfig;
    private OutlierAlgorithm sketchyOutlierAlgorithm;
    com.caseystella.analytics.outlier.batch.OutlierAlgorithm batchOutlierAlgorithm;


    public static final PropertyDescriptor RECORD_VALUE_FIELD = new PropertyDescriptor.Builder()
            .name("value.field")
            .description("the numeric field to get the value")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(FieldDictionary.RECORD_VALUE)
            .build();


    public static final PropertyDescriptor RECORD_TIME_FIELD = new PropertyDescriptor.Builder()
            .name("time.field")
            .description("the numeric field to get the value")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(FieldDictionary.RECORD_TIME)
            .build();

    public static final PropertyDescriptor ROTATION_POLICY_TYPE = new PropertyDescriptor.Builder()
            .name("rotation.policy.type")
            .description("...")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("by_amount")
            .allowableValues("by_amount", "by_time", "never")
            .build();

    public static final PropertyDescriptor ROTATION_POLICY_AMOUNT = new PropertyDescriptor.Builder()
            .name("rotation.policy.amount")
            .description("...")
            .required(true)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .defaultValue("100")
            .build();

    public static final PropertyDescriptor ROTATION_POLICY_UNIT = new PropertyDescriptor.Builder()
            .name("rotation.policy.unit")
            .description("...")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("points")
            .allowableValues("milliseconds", "seconds", "hours", "days", "months", "years", "points")
            .build();

    public static final PropertyDescriptor CHUNKING_POLICY_TYPE = new PropertyDescriptor.Builder()
            .name("chunking.policy.type")
            .description("...")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("by_amount")
            .allowableValues("by_amount", "by_time", "never")
            .build();

    public static final PropertyDescriptor CHUNKING_POLICY_AMOUNT = new PropertyDescriptor.Builder()
            .name("chunking.policy.amount")
            .description("...")
            .required(true)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .defaultValue("100")
            .build();

    public static final PropertyDescriptor CHUNKING_POLICY_UNIT = new PropertyDescriptor.Builder()
            .name("chunking.policy.unit")
            .description("...")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("points")
            .allowableValues("milliseconds", "seconds", "hours", "days", "months", "years", "points")
            .build();


    public static final PropertyDescriptor SKETCHY_OUTLIER_ALGORITHM = new PropertyDescriptor.Builder()
            .name("sketchy.outlier.algorithm")
            .description("...")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues("SKETCHY_MOVING_MAD")
            .defaultValue("SKETCHY_MOVING_MAD")
            .build();

    public static final PropertyDescriptor BATCH_OUTLIER_ALGORITHM = new PropertyDescriptor.Builder()
            .name("batch.outlier.algorithm")
            .description("...")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues("RAD")
            .defaultValue("RAD")
            .build();


    public static final PropertyDescriptor MIN_AMOUNT_TO_PREDICT = new PropertyDescriptor.Builder()
            .name("min.amount.to.predict")
            .description("minAmountToPredict")
            .required(true)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .defaultValue("100")
            .build();

    public static final PropertyDescriptor ZSCORE_CUTOFFS_NORMAL = new PropertyDescriptor.Builder()
            .name("zscore.cutoffs.normal")
            .description("zscoreCutoffs level for normal outlier")
            .required(true)
            .addValidator(StandardValidators.DOUBLE_VALIDATOR)
            .defaultValue("0.000000000000001")
            .build();

    public static final PropertyDescriptor ZSCORE_CUTOFFS_MODERATE = new PropertyDescriptor.Builder()
            .name("zscore.cutoffs.moderate")
            .description("zscoreCutoffs level for moderate outlier")
            .required(true)
            .addValidator(StandardValidators.DOUBLE_VALIDATOR)
            .defaultValue("1.5")
            .build();

    public static final PropertyDescriptor ZSCORE_CUTOFFS_SEVERE = new PropertyDescriptor.Builder()
            .name("zscore.cutoffs.severe")
            .description("zscoreCutoffs level for severe outlier")
            .required(true)
            .addValidator(StandardValidators.DOUBLE_VALIDATOR)
            .defaultValue("10.0")
            .build();

    public static final PropertyDescriptor ZSCORE_CUTOFFS_NOT_ENOUGH_DATA = new PropertyDescriptor.Builder()
            .name("zscore.cutoffs.notEnoughData")
            .description("zscoreCutoffs level for notEnoughData outlier")
            .required(false)
            .addValidator(StandardValidators.DOUBLE_VALIDATOR)
            .defaultValue("100")
            .build();

    public static final PropertyDescriptor RESERVOIR_SIZE = new PropertyDescriptor.Builder()
            .name("reservoir_size")
            .description("the size of points reservoir")
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("100")
            .build();

    public static final PropertyDescriptor SMOOTH = new PropertyDescriptor.Builder()
            .name("smooth")
            .description("do smoothing ?")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor DECAY = new PropertyDescriptor.Builder()
            .name("decay")
            .description("the decay")
            .required(false)
            .addValidator(StandardValidators.DOUBLE_VALIDATOR)
            .defaultValue("0.1")
            .build();

    public static final PropertyDescriptor MIN_ZSCORE_PERCENTILE = new PropertyDescriptor.Builder()
            .name("min_zscore_percentile")
            .description("minZscorePercentile")
            .required(false)
            .addValidator(StandardValidators.DOUBLE_VALIDATOR)
            .defaultValue("50.0")
            .build();


    public static final PropertyDescriptor GLOBAL_STATISTICS_MIN = new PropertyDescriptor.Builder()
            .name("global.statistics.min")
            .description("minimum value")
            .required(false)
            .addValidator(StandardValidators.DOUBLE_VALIDATOR)
            .build();


    public static final PropertyDescriptor GLOBAL_STATISTICS_MAX = new PropertyDescriptor.Builder()
            .name("global.statistics.max")
            .description("maximum value")
            .required(false)
            .addValidator(StandardValidators.DOUBLE_VALIDATOR)
            .build();

    public static final PropertyDescriptor GLOBAL_STATISTICS_MEAN = new PropertyDescriptor.Builder()
            .name("global.statistics.mean")
            .description("mean value")
            .required(false)
            .addValidator(StandardValidators.DOUBLE_VALIDATOR)
            .build();

    public static final PropertyDescriptor GLOBAL_STATISTICS_STDDEV = new PropertyDescriptor.Builder()
            .name("global.statistics.stddev")
            .description("standard deviation value")
            .required(false)
            .addValidator(StandardValidators.DOUBLE_VALIDATOR)
            .build();


    public static final PropertyDescriptor RPCA_THRESHOLD = new PropertyDescriptor.Builder()
            .name("rpca.threshold")
            .description("")
            .required(false)
            .addValidator(StandardValidators.DOUBLE_VALIDATOR)
            .build();

    public static final PropertyDescriptor RPCA_LPENALTY = new PropertyDescriptor.Builder()
            .name("rpca.lpenalty")
            .description("")
            .required(false)
            .addValidator(StandardValidators.DOUBLE_VALIDATOR)
            .build();

    public static final PropertyDescriptor RPCA_SPENALTY = new PropertyDescriptor.Builder()
            .name("rpca.spenalty")
            .description("")
            .required(false)
            .addValidator(StandardValidators.DOUBLE_VALIDATOR)
            .build();

    public static final PropertyDescriptor RPCA_FORCE_DIFF = new PropertyDescriptor.Builder()
            .name("rpca.force.diff")
            .description("")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor RPCA_MIN_RECORDS = new PropertyDescriptor.Builder()
            .name("rpca.min.records")
            .description("")
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor OUTPUT_RECORD_TYPE = new PropertyDescriptor.Builder()
            .name("output.record.type")
            .description("the output type of the record")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("alert_match")
            .build();

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(RECORD_VALUE_FIELD);
        descriptors.add(RECORD_TIME_FIELD);
        descriptors.add(OUTPUT_RECORD_TYPE);
        descriptors.add(ROTATION_POLICY_TYPE);
        descriptors.add(ROTATION_POLICY_AMOUNT);
        descriptors.add(ROTATION_POLICY_UNIT);
        descriptors.add(CHUNKING_POLICY_TYPE);
        descriptors.add(CHUNKING_POLICY_AMOUNT);
        descriptors.add(CHUNKING_POLICY_UNIT);
        descriptors.add(SKETCHY_OUTLIER_ALGORITHM);
        descriptors.add(BATCH_OUTLIER_ALGORITHM);
        descriptors.add(GLOBAL_STATISTICS_MIN);
        descriptors.add(GLOBAL_STATISTICS_MAX);
        descriptors.add(GLOBAL_STATISTICS_MEAN);
        descriptors.add(GLOBAL_STATISTICS_STDDEV);

        descriptors.add(ZSCORE_CUTOFFS_NORMAL);
        descriptors.add(ZSCORE_CUTOFFS_MODERATE);
        descriptors.add(ZSCORE_CUTOFFS_SEVERE);
        descriptors.add(ZSCORE_CUTOFFS_NOT_ENOUGH_DATA);
        descriptors.add(SMOOTH);
        descriptors.add(DECAY);
        descriptors.add(MIN_AMOUNT_TO_PREDICT);
        descriptors.add(MIN_ZSCORE_PERCENTILE);
        descriptors.add(RESERVOIR_SIZE);

        descriptors.add(RPCA_FORCE_DIFF);
        descriptors.add(RPCA_LPENALTY);
        descriptors.add(RPCA_MIN_RECORDS);
        descriptors.add(RPCA_SPENALTY);
        descriptors.add(RPCA_THRESHOLD);


        return Collections.unmodifiableList(descriptors);
    }


    @Override
    public void init(ProcessContext context) {
        super.init(context);
        logger.info("init");

        outlierConfig = new OutlierConfig();
        /**
         * chunking policy
         */
        outlierConfig.getChunkingPolicy().setAmount(context.getPropertyValue(CHUNKING_POLICY_AMOUNT).asLong());
        switch (context.getPropertyValue(CHUNKING_POLICY_TYPE).asString().toLowerCase()) {
            case "by_time":
                outlierConfig.getChunkingPolicy().setType(Type.BY_TIME);
                break;
            case "by_amount":
                outlierConfig.getChunkingPolicy().setType(Type.BY_AMOUNT);
                break;
            case "never":
                outlierConfig.getChunkingPolicy().setType(Type.NEVER);
                break;
        }
        switch (context.getPropertyValue(CHUNKING_POLICY_UNIT).asString().toLowerCase()) {
            case "milliseconds":
                outlierConfig.getChunkingPolicy().setUnit(Unit.MILLISECONDS);
                break;
            case "seconds":
                outlierConfig.getChunkingPolicy().setUnit(Unit.SECONDS);
                break;
            case "hours":
                outlierConfig.getChunkingPolicy().setUnit(Unit.HOURS);
                break;
            case "days":
                outlierConfig.getChunkingPolicy().setUnit(Unit.DAYS);
                break;
            case "months":
                outlierConfig.getChunkingPolicy().setUnit(Unit.MONTHS);
                break;
            case "years":
                outlierConfig.getChunkingPolicy().setUnit(Unit.YEARS);
                break;
            case "points":
                outlierConfig.getChunkingPolicy().setUnit(Unit.POINTS);
                break;
        }

        /**
         * rotation policy
         */
        outlierConfig.getRotationPolicy().setAmount(context.getPropertyValue(ROTATION_POLICY_AMOUNT).asLong());
        switch (context.getPropertyValue(ROTATION_POLICY_TYPE).asString().toLowerCase()) {
            case "by_time":
                outlierConfig.getRotationPolicy().setType(Type.BY_TIME);
                break;
            case "by_amount":
                outlierConfig.getRotationPolicy().setType(Type.BY_AMOUNT);
                break;
            case "never":
                outlierConfig.getRotationPolicy().setType(Type.NEVER);
                break;
        }
        switch (context.getPropertyValue(ROTATION_POLICY_UNIT).asString().toLowerCase()) {
            case "milliseconds":
                outlierConfig.getRotationPolicy().setUnit(Unit.MILLISECONDS);
                break;
            case "seconds":
                outlierConfig.getRotationPolicy().setUnit(Unit.SECONDS);
                break;
            case "hours":
                outlierConfig.getRotationPolicy().setUnit(Unit.HOURS);
                break;
            case "days":
                outlierConfig.getRotationPolicy().setUnit(Unit.DAYS);
                break;
            case "months":
                outlierConfig.getRotationPolicy().setUnit(Unit.MONTHS);
                break;
            case "years":
                outlierConfig.getRotationPolicy().setUnit(Unit.YEARS);
                break;
            case "points":
                outlierConfig.getRotationPolicy().setUnit(Unit.POINTS);
                break;
        }


        /**
         * global stats
         */
        GlobalStatistics globalStatistics = new GlobalStatistics();
        if (context.getPropertyValue(GLOBAL_STATISTICS_MIN).isSet()) {
            globalStatistics.setMin(context.getPropertyValue(GLOBAL_STATISTICS_MIN).asDouble());
        }
        if (context.getPropertyValue(GLOBAL_STATISTICS_MAX).isSet()) {
            globalStatistics.setMax(context.getPropertyValue(GLOBAL_STATISTICS_MAX).asDouble());
        }
        if (context.getPropertyValue(GLOBAL_STATISTICS_MEAN).isSet()) {
            globalStatistics.setMean(context.getPropertyValue(GLOBAL_STATISTICS_MEAN).asDouble());
        }
        if (context.getPropertyValue(GLOBAL_STATISTICS_STDDEV).isSet()) {
            globalStatistics.setStddev(context.getPropertyValue(GLOBAL_STATISTICS_STDDEV).asDouble());
        }
        outlierConfig.setGlobalStatistics(globalStatistics);


        /**
         * skechy conf
         */
        if (context.getPropertyValue(MIN_AMOUNT_TO_PREDICT).isSet()) {
            outlierConfig.getConfig().put(
                    SketchyMovingMAD.MIN_AMOUNT_TO_PREDICT,
                    context.getPropertyValue(MIN_AMOUNT_TO_PREDICT).asLong());
        }
        if (context.getPropertyValue(RESERVOIR_SIZE).isSet()) {
            outlierConfig.getConfig().put(
                    SketchyMovingMAD.RESERVOIR_SIZE,
                    context.getPropertyValue(RESERVOIR_SIZE).asInteger());
        }
        if (context.getPropertyValue(SMOOTH).isSet()) {
            outlierConfig.getConfig().put(
                    SketchyMovingMAD.SMOOTH,
                    context.getPropertyValue(SMOOTH).asBoolean());
        }
        if (context.getPropertyValue(DECAY).isSet()) {
            outlierConfig.getConfig().put(
                    SketchyMovingMAD.DECAY,
                    context.getPropertyValue(DECAY).asDouble());
        }
        if (context.getPropertyValue(MIN_ZSCORE_PERCENTILE).isSet()) {
            outlierConfig.getConfig().put(
                    SketchyMovingMAD.MIN_ZSCORE_PERCENTILE,
                    context.getPropertyValue(MIN_ZSCORE_PERCENTILE).asDouble());
        }


        /**
         * zscore cuttoffs
         */
        Map<String, Object> zscoreCuttoffsConf = new HashMap<>();
        if (context.getPropertyValue(ZSCORE_CUTOFFS_NORMAL).isSet()) {
            zscoreCuttoffsConf.put("NORMAL",
                    context.getPropertyValue(ZSCORE_CUTOFFS_NORMAL).asDouble());
        }
        if (context.getPropertyValue(ZSCORE_CUTOFFS_MODERATE).isSet()) {
            zscoreCuttoffsConf.put("MODERATE_OUTLIER",
                    context.getPropertyValue(ZSCORE_CUTOFFS_MODERATE).asDouble());
        }
        if (context.getPropertyValue(ZSCORE_CUTOFFS_SEVERE).isSet()) {
            zscoreCuttoffsConf.put("SEVERE_OUTLIER",
                    context.getPropertyValue(ZSCORE_CUTOFFS_SEVERE).asDouble());
        }
        if (context.getPropertyValue(ZSCORE_CUTOFFS_NOT_ENOUGH_DATA).isSet()) {
            zscoreCuttoffsConf.put("NOT_ENOUGH_DATA",
                    context.getPropertyValue(ZSCORE_CUTOFFS_NOT_ENOUGH_DATA).asDouble());
        }
        outlierConfig.getConfig().put(SketchyMovingMAD.ZSCORE_CUTOFFS_CONF, zscoreCuttoffsConf);


        /**
         * rpca conf
         */
        if (context.getPropertyValue(RPCA_FORCE_DIFF).isSet()) {
            outlierConfig.getConfig().put(
                    RPCAOutlierAlgorithm.FORCE_DIFF_CONFIG,
                    context.getPropertyValue(RPCA_FORCE_DIFF).asBoolean());
        }
        if (context.getPropertyValue(RPCA_THRESHOLD).isSet()) {
            outlierConfig.getConfig().put(
                    RPCAOutlierAlgorithm.THRESHOLD_CONF,
                    context.getPropertyValue(RPCA_THRESHOLD).asDouble());
        }
        if (context.getPropertyValue(RPCA_LPENALTY).isSet()) {
            outlierConfig.getConfig().put(
                    RPCAOutlierAlgorithm.LPENALTY_CONFIG,
                    context.getPropertyValue(RPCA_LPENALTY).asDouble());
        }
        if (context.getPropertyValue(RPCA_SPENALTY).isSet()) {
            outlierConfig.getConfig().put(
                    RPCAOutlierAlgorithm.SPENALTY_CONFIG,
                    context.getPropertyValue(RPCA_SPENALTY).asDouble());
        }
        if (context.getPropertyValue(RPCA_MIN_RECORDS).isSet()) {
            outlierConfig.getConfig().put(
                    RPCAOutlierAlgorithm.MIN_RECORDS_CONFIG,
                    context.getPropertyValue(RPCA_MIN_RECORDS).asInteger());
        }

        sketchyOutlierAlgorithm = new SketchyMovingMAD();
        sketchyOutlierAlgorithm.configure(outlierConfig);
        batchOutlierAlgorithm = new RPCAOutlierAlgorithm();
        batchOutlierAlgorithm.configure(outlierConfig);
    }


    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {

        final Collection<ValidationResult> results = new ArrayList<>();

        if (context.getPropertyValue(DECAY).asDouble() < 0.0) {
            results.add(
                    new ValidationResult.Builder()
                            .valid(false)
                            .input(null)
                            .subject(DECAY.getName())
                            .explanation(DECAY.getName() + " must be >= 0.0")
                            .build()
            );
        }
        if (context.getPropertyValue(DECAY).asDouble() >= 1.0) {
            results.add(
                    new ValidationResult.Builder()
                            .valid(false)
                            .input(null)
                            .subject(DECAY.getName())
                            .explanation(DECAY.getName() + " must be < 1.0")
                            .build()
            );
        }


        return results;


    }

    /**
     *
     */
    @Override
    public Collection<Record> process(final ProcessContext context, final Collection<Record> records) {

        // context may not have been initialized
        if(outlierConfig == null)
            init(context);

        Collection<Record> list = new ArrayList<>();

        final String valueField = context.getPropertyValue(RECORD_VALUE_FIELD).asString();
        final String timeField = context.getPropertyValue(RECORD_TIME_FIELD).asString();
        final String outputRecordType = context.getPropertyValue(OUTPUT_RECORD_TYPE).asString();

        // loop over all events in collection
        for (Record record : records) {

            try {
                // convert an event to a dataPoint.
                long timestamp = record.getField(timeField).asLong();
                double value = record.getField(valueField).asDouble();

                DataPoint dp = new DataPoint(timestamp, value, new HashMap<>(), record.getType());

                // now let's look for outliers
                Outlier outlier = sketchyOutlierAlgorithm.analyze(dp);
                if (outlier.getSeverity() == Severity.SEVERE_OUTLIER) {

                    outlier = batchOutlierAlgorithm.analyze(outlier, outlier.getSample(), dp);
                    if (outlier.getSeverity() == Severity.SEVERE_OUTLIER) {

                        Record evt = new StandardRecord(record)
                                .setType(outputRecordType)
                                .setTime(new Date(timestamp))
                                .setStringField("outlier_severity", "severe")
                                .setField("outlier_score", FieldType.DOUBLE, outlier.getScore())
                                .setField("outlier_num_points", FieldType.INT, outlier.getNumPts());
                        list.add(evt);
                    }
                }

            } catch (RuntimeException e) {
                list.add(new StandardRecord(OUTLIER_PROCESSING_EXCEPTION_TYPE)
                        .setStringField(FieldDictionary.RECORD_ERRORS, ProcessError.RUNTIME_ERROR.toString())
                        .setStringField(FieldDictionary.RECORD_VALUE, e.getMessage())
                        .setStringField(FieldDictionary.PROCESSOR_NAME, DetectOutliers.class.getName())
                );
            }
        }

        return list;
    }

}
