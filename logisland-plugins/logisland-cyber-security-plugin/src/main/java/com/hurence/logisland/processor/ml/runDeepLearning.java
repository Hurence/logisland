/**
 * Copyright (C) 2017 Hurence
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.processor.ml;

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.component.PropertyValue;
import com.hurence.logisland.model.MLNModel;
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.service.ml.MLClientService;
import com.hurence.logisland.validator.StandardValidators;
import org.deeplearning4j.eval.Evaluation;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.factory.Nd4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * MNIST MLN Machine Learning processor
 */
@Tags({"machine learning", "deep learning"})
@CapabilityDescription(
        "The ml processor has been written to describe how to implement machine Learning Capabilities to logisland. "
                + "It relies on deeplearning4j libraries to load the model and to run the Neural Network prediction")


public class runDeepLearning extends AbstractProcessor {

    private static Logger logger = LoggerFactory.getLogger(runDeepLearning.class);

    private boolean debug = false;

    private static final String KEY_DEBUG = "debug";

    protected MLClientService clientService;

    public static final PropertyDescriptor DEBUG = new PropertyDescriptor.Builder()
            .name(KEY_DEBUG)
            .description("Enable debug.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(false)
            .build();

    public static final PropertyDescriptor ML_MODEL_FILE_PATH = new PropertyDescriptor.Builder()
            .name("ml.model.file.path")
            .description("the path of the ml model")
            .required(true)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    public static final PropertyDescriptor ML_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("ml.client.service")
            .description("The instance of the Controller Service to use for using ML services.")
            .required(true)
            .identifiesControllerService(MLClientService.class)
            .build();

    @Override
    public boolean hasControllerService() {
        return true;
    }

    @Override
    public void init(final ProcessContext context) {
        PropertyValue ac =  context.getPropertyValue(ML_CLIENT_SERVICE);

        clientService = context.getPropertyValue(ML_CLIENT_SERVICE).asControllerService(MLClientService.class);
        if(clientService == null)
            logger.error("ML client service is not initialized!");
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {

        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(DEBUG);
        descriptors.add(ML_MODEL_FILE_PATH);
        return Collections.unmodifiableList(descriptors);
    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {

       // process test images
        records.forEach(record -> {
            logger.info("****************starting processing of one record********************");

            // Note: one record could contain several images. Records also contain the corresponding labels
            // The fact to have the labels for examples images could appear quite strange, but this is a machine learning
            // demo processor and the label allow to monitor exactly the quality of predictions.
            // in a real example, you obviously don't have these labels.

            final int outputNum = 10;
            final Evaluation eval = new Evaluation(outputNum);

            MultiLayerNetwork restored = ((MLNModel) clientService.restoreModel()).getMLNModel();

            RecordDecryptor rd = new MNISTRecordDecryptor();
            rd.decrypt(record,false);
            INDArray image = rd.getFeaturedData();
            INDArray label = rd.getLabels();

            INDArray output = restored.output(image); //get the networks prediction
            //eval.eval(label, output); //check the prediction against the true class

            /*HashMap<Integer,Integer> positive = (HashMap<Integer,Integer>)eval.positive();
            for  (Map.Entry<Integer, Integer> p: positive.entrySet()) {
                logger.info(p.getKey()+" "+p.getValue());
            }
            */

            for (int i=0; i<rd.getCount(); i++) {
               // INDArray exp = label.getRow(i);
                INDArray pred = output.getRow(i);

                float maxpred = pred.getFloat(0);
             //   int maxexp = exp.getInt(0);
                int rpred=0;
                int rexp=0;

                for (int j=1; j<outputNum; j++) {

                    if (pred.getFloat(j) > maxpred) {
                        rpred=j;
                        maxpred = pred.getFloat(j);
                    }

              /*      if (exp.getInt(j) > maxexp) {
                        rexp=j;
                        maxexp = exp.getInt(j);
                    }*/
                }

                /*logger.info("Image "+i+ " is supposed to be " + rexp
                + " : model predict " + rpred);*/
                logger.info("predicted image is "+rpred);
                logger.info("prediction array is :" +output);
            }
            logger.info("****************General stats********************");
            logger.info(eval.stats(true));
            logger.info("****************Example finished********************");

        });

        return (new ArrayList<>());
    }


    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {

        logger.debug("property {} value changed from {} to {}", descriptor.getName(), oldValue, newValue);

        /**
         * Handle the debug property
         */
        if (descriptor.getName().equals(KEY_DEBUG)) {
            if (newValue != null) {
                if (newValue.equalsIgnoreCase("true")) {
                    debug = true;
                }
            } else {
                debug = false;
            }
        }
    }

}
