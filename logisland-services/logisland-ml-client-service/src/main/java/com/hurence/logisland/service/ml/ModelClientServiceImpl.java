package com.hurence.logisland.service.ml;

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.annotation.lifecycle.OnEnabled;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.AbstractControllerService;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.ml.model.KMeansModelWrapper;
import com.hurence.logisland.ml.model.MLNModel;
import com.hurence.logisland.ml.model.Model;
import com.hurence.logisland.validator.StandardValidators;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.util.ModelSerializer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


@Tags({"ml", "client"})
@CapabilityDescription("A controller service for accessing an Machine Learning  client.")
public class ModelClientServiceImpl extends AbstractControllerService implements ModelClientService {

    public static final AllowableValue MLLIB = new AllowableValue("mllib", "Spark MLlib",
            "Spark Machine Learning library");

    PropertyDescriptor ML_MODEL_LIBRARY = new PropertyDescriptor.Builder()
            .name("ml.model.library")
            .description("Library implementing the model.")
            .allowableValues(MLLIB)
            .build();

    public static final AllowableValue KMEANS = new AllowableValue("kmeans", "KMeans clustering model",
            "KMeans clustering model");

    PropertyDescriptor ML_MODEL_NAME = new PropertyDescriptor.Builder()
            .name("ml.model.name")
            .description("Model name")
            .allowableValues(KMEANS)
            .build();

    PropertyDescriptor ML_MODEL_FILE_PATH = new PropertyDescriptor.Builder()
            .name("ml.model.file.path")
            .description("path to the pre-trained MNIST Deep Learning model file.")
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    private volatile String modelLibrary = null;
    private volatile String modelName = null;
    private volatile String modelFilePath = null;

    private volatile Model model = null;

    @Override
    @OnEnabled
    public void init(ControllerServiceInitializationContext context) throws InitializationException {
        try {
            modelLibrary = context.getPropertyValue(ML_MODEL_LIBRARY).asString();
            modelName = context.getPropertyValue(ML_MODEL_NAME).asString();
            modelFilePath = context.getPropertyValue(ML_MODEL_FILE_PATH).asString();
        } catch (Exception e){
            throw new InitializationException(e);
        }

        // KMeans :
        if(modelName == "kmeans") {
            model = new KMeansModelWrapper(modelFilePath);
        }

    }

    @Override
    public Model getModel() {
        return model;
    }

    /**
     * Restore the previously computed Neural Network model.
     *
     */
    public Model restoreModel() {

        final File locationToSave = new File(modelFilePath);
        MultiLayerNetwork restored = null ;

        try {
            restored  = ModelSerializer.restoreMultiLayerNetwork(locationToSave);
        } catch (IOException e) {
            e.printStackTrace();
        }
        MLNModel m = new MLNModel();
        m.setMLNModel(restored);
        return(m);
    }


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {

        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(ML_MODEL_LIBRARY);
        props.add(ML_MODEL_NAME);
        props.add(ML_MODEL_FILE_PATH);

        return Collections.unmodifiableList(props);
    }

}
