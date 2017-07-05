package com.hurence.logisland.service.ml;


import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.ControllerService;
import com.hurence.logisland.ml.model.Model;
import com.hurence.logisland.validator.StandardValidators;


@Tags({"ml", "client"})
@CapabilityDescription("A controller service for accessing Machine Learning trained models.")
public interface ModelClientService extends  ControllerService {

    PropertyDescriptor ML_MODEL_FILE_PATH = new PropertyDescriptor.Builder()
            .name("ml.model.file.path")
            .description("path to the pre-trained MNIST Deep Learning model file.")
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();


    /**
     * Restore the previously computed Neural Network model.
     *
     */
    Model restoreModel() ;

    Model getModel();


}
