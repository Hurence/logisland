package com.hurence.logisland.service.ml;


import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.ControllerService;
import com.hurence.logisland.ml.scaler.Scaler;
import com.hurence.logisland.validator.StandardValidators;


@Tags({"ml", "scaler", "client"})
@CapabilityDescription("A controller service for accessing Machine Learning scaler models.")
public interface ScalerModelClientService extends  ControllerService {

    PropertyDescriptor ML_SCALER_MODEL_FILE_PATH = new PropertyDescriptor.Builder()
            .name("ml.scaler.file.path")
            .description("path to the already fit scaler model file.")
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();


    Scaler getScalerModel();


}
