package com.hurence.logisland.processor.ml;


import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.annotation.lifecycle.OnEnabled;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.AbstractControllerService;
import com.hurence.logisland.controller.ControllerService;

import com.hurence.logisland.controller.ControllerServiceInitializationContext;
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
@CapabilityDescription("A controller service for accessing an ML client.")
public interface MLClientService extends  ControllerService {


    PropertyDescriptor ML_MODEL_FILE_PATH = new PropertyDescriptor.Builder()
            .name("ml.model.file.path")
            .description("Comma-separated list of Hadoop Configuration files," +
                    " such as hbase-site.xml and core-site.xml for kerberos, " +
                    "including full paths to the files.")
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();





    /**
     * Restore the previously computed Neural Network model.
     *
     */
    MultiLayerNetwork restoreModel() ;


}
