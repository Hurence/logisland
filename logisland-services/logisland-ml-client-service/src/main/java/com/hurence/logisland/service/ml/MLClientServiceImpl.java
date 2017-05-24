package com.hurence.logisland.service.ml;

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.annotation.lifecycle.OnEnabled;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.AbstractControllerService;
import com.hurence.logisland.controller.ControllerService;

import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.processor.ml.MLClientService;
import com.hurence.logisland.validator.StandardValidators;
import com.hurence.logisland.validator.ValidationContext;
import com.hurence.logisland.validator.ValidationResult;
import org.deeplearning4j.eval.Evaluation;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.util.ModelSerializer;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;


@Tags({"ml", "client"})
@CapabilityDescription("A controller service for accessing an Machine Learning  client.")
public class MLClientServiceImpl extends AbstractControllerService implements MLClientService {


    PropertyDescriptor ML_MODEL_FILE_PATH = new PropertyDescriptor.Builder()
            .name("ml.model.file.path")
            .description("path to the pre-trained MNIST Deep Learning model file.")
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();


    private String pathName = null;



    /**
     * Restore the previously computed Neural Network model.
     *
     */
    public MultiLayerNetwork restoreModel() {

        final File locationToSave = new File(pathName);
        MultiLayerNetwork restored = null ;

        try {
            restored  = ModelSerializer.restoreMultiLayerNetwork(locationToSave);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return(restored);
    }

    @Override
    @OnEnabled
    public void init(ControllerServiceInitializationContext context) throws InitializationException {
        try {
            pathName = context.getPropertyValue(ML_MODEL_FILE_PATH).asString();
        } catch (Exception e){
            throw new InitializationException(e);
        }
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return null;
    }

}
