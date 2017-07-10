package com.hurence.logisland.service.ml;

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.annotation.lifecycle.OnEnabled;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.AbstractControllerService;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.ml.scaler.Scaler;
import com.hurence.logisland.ml.scaling.StandardScalerModelWrapper;
import com.hurence.logisland.validator.StandardValidators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


@Tags({"ml", "scaler", "client"})
@CapabilityDescription("A controller service for accessing an Machine Learning  client.")
public class ScalerModelClientServiceImpl extends AbstractControllerService implements ScalerModelClientService {

    public static final AllowableValue STANDARD_SCALER = new AllowableValue("standard_scaler", "MLlib Standard scaler model",
            "MLlib Standard scaler model");

    PropertyDescriptor ML_SCALER_MODEL_NAME = new PropertyDescriptor.Builder()
            .name("ml.scaler.model.name")
            .description("Scaler model name")
            .allowableValues(STANDARD_SCALER)
            .build();

    PropertyDescriptor ML_SCALER_MODEL_FILE_PATH = new PropertyDescriptor.Builder()
            .name("ml.scaler.file.path")
            .description("path to the pre-trained MNIST Deep Learning model file.")
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    private volatile String scalerModelName = null;
    private volatile String scalerModelFilePath = null;

    private volatile Scaler scalerModel = null;

    @Override
    @OnEnabled
    public void init(ControllerServiceInitializationContext context) throws InitializationException {
        try {
            scalerModelName = context.getPropertyValue(ML_SCALER_MODEL_NAME).asString();
            scalerModelFilePath = context.getPropertyValue(ML_SCALER_MODEL_FILE_PATH).asString();
        } catch (Exception e){
            throw new InitializationException(e);
        }

        // Standard scaler :
        if(scalerModelName == "standard_scaler") {
            scalerModel = new StandardScalerModelWrapper(scalerModelFilePath);
        }

    }

    @Override
    public Scaler getScalerModel() {
        return scalerModel;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {

        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(ML_SCALER_MODEL_NAME);
        props.add(ML_SCALER_MODEL_FILE_PATH);

        return Collections.unmodifiableList(props);
    }

}
