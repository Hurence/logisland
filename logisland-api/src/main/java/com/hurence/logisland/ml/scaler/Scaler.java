package com.hurence.logisland.ml.scaler;

import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.ConfigurableComponent;
import com.hurence.logisland.component.PropertyDescriptor;


public interface Scaler<T,V> extends ConfigurableComponent {

    public static final AllowableValue STANDARD_SCALER = new AllowableValue(
            "mllib_standard_scaler",
            "MLLIB Standard Scaler model",
            "A standard scaler model that can transforms vectors");

    PropertyDescriptor MODEL_TYPE = new PropertyDescriptor.Builder()
            .name("model.type")
            .description("The ML model type")
            .allowableValues(STANDARD_SCALER)
            .build();


    V transform (T input) throws Exception;
}
