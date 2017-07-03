package com.hurence.logisland.ml.model;

import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.ConfigurableComponent;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.Record;

import java.util.Collection;

/**
 * Created by pducjac on 31/05/17.
 */
public interface Model<T,V> extends ConfigurableComponent {

    public static final AllowableValue MLLIB_KMEANS_CENTROID = new AllowableValue(
            "mllib_kmeans",
            "MLLIB KMeans centroid",
            "A centroid list for MLLIB Kmeans");

    public static final AllowableValue DL4J_MULTI_LAYER_NETWORK = new AllowableValue(
            "deeplearning4j MLN model",
            "DL4J MLN",
            "A Deep learning LMN model");

    PropertyDescriptor MODEL_TYPE = new PropertyDescriptor.Builder()
            .name("model.type")
            .description("The ML model type")
            .allowableValues(MLLIB_KMEANS_CENTROID, DL4J_MULTI_LAYER_NETWORK)
            .build();


    Record predict(Record inputRecord) throws Exception;

    Collection<Record> predict(Collection<Record> inputRecord) throws Exception;

    V predict(T input) throws Exception;
}
