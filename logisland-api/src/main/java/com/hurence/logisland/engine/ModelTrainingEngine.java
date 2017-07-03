package com.hurence.logisland.engine;

import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.ml.model.Model;
import com.hurence.logisland.validator.StandardValidators;

/**
 * Created by pducjac on 25/05/17.
 */
public interface ModelTrainingEngine extends ProcessingEngine {

        PropertyDescriptor MODEL_REFRESH_DELAY = new PropertyDescriptor.Builder()
                .name("model.refresh.delay")
                .description("The delay before retraining model")
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build();

        PropertyDescriptor DATA_LOCATION = new PropertyDescriptor.Builder()
                .name("data.location")
                .description("The place where sit the input data kafka://data/logisland_traces or " +
                        "hdfs://data/logisland_traces or" +
                        "http://logisland-agent:9000/data/logisland_traces or" +
                        "file://data/logisland_traces ")
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build();

        PropertyDescriptor DATA_BUCKET = new PropertyDescriptor.Builder()
                .name("data.bucket")
                .description("the period to consider, all data, between two dates/offsets")
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build();

        PropertyDescriptor MODEL_LOCATION = new PropertyDescriptor.Builder()
                .name("model.location")
                .description("The place where sit the output model kafka://models/trace_analytics or " +
                        "hdfs://data/models/trace_analytics or" +
                        "http://logisland-agent:9000/models/trace_analytics or" +
                        "file://data/models/trace_analytics ")
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build();

        Model train(Model untrainedModel) throws Exception;
}
