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
import com.hurence.logisland.component.AbstractConfigurableComponent;
import com.hurence.logisland.component.ComponentContext;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.component.PropertyValue;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;
import org.deeplearning4j.eval.Evaluation;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.factory.Nd4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Netflow (http://www.cisco.com/c/en/us/td/docs/ios/solutions_docs/netflow/nfwhite.html) processor
 */
@Tags({"machine learning", "deep learning"})
@CapabilityDescription(
        "The ml processor has been writteen to describe how to implement machine Learning Capabilities to logismlland"
                + "It relies on deeplearning4j lmibraries to load the model and to run the Neural Network prediction")


public class runDeepLearning extends AbstractProcessor {

    private static Logger logger = LoggerFactory.getLogger(runDeepLearning.class);

    final int FIN = 1;
    final int SYN = 2;
    final int RST = 4;
    final int PSH = 8;
    final int ACK = 16;
    final int URG = 32;

    private boolean debug = false;

    private static final String KEY_DEBUG = "debug";

    protected MLClientService clientService;


    public static final PropertyDescriptor DEBUG = new PropertyDescriptor.Builder()
            .name(KEY_DEBUG)
            .description("Enable debug. If enabled, the original JSON string is embedded in the record_value field of the record.")
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
            byte[] recordValue = null;
            try {
                recordValue = (byte[]) record.getField(FieldDictionary.RECORD_VALUE).getRawValue();
            } catch (Exception e) {
                logger.error("Error while retrieving the record value : record skipped");
                return;
            }
            if (debug) {
                logger.debug("record=" + Arrays.toString(recordValue));
            }
            int shift = 0;
            long magic = readInt(Arrays.copyOfRange(recordValue, shift, shift + 4));
            shift += 4;
            int count = (int) readInt(Arrays.copyOfRange(recordValue, shift, shift + 4));
            shift += 4;
            int row = (int) readInt(Arrays.copyOfRange(recordValue, shift, shift + 4));
            shift += 4;
            int col = (int) readInt(Arrays.copyOfRange(recordValue, shift, shift + 4));
            shift += 4;

            float[][] featureData = new float[count][0];

            for (int i = 0; i < count; i++) {
                float[] featureVec = new float[row * col];
                byte[] im = Arrays.copyOfRange(recordValue, shift, shift + row * col);
                shift += row * col;
                for (int j = 0; j < im.length; ++j) {
                    float v = (float) (im[j] & 255);
                    featureVec[j] = v / 255.0F;
                }
                featureData[i] = featureVec;
            }

            magic = readInt(Arrays.copyOfRange(recordValue, shift, shift + 4));
            shift += 4;
            count = (int) readInt(Arrays.copyOfRange(recordValue, shift, shift + 4));
            shift += 4;
            float[][] labelData = new float[count][0];
            for (int i = 0; i < count; i++) {
                int lab = getByte(Arrays.copyOfRange(recordValue, shift, shift + 1));
                shift += 1;
                labelData[i] = new float[10];
                labelData[i][lab] = 1.0F;
            }
            final int outputNum = 10;
            final Evaluation eval = new Evaluation(outputNum);

            MultiLayerNetwork restored = clientService.restoreModel() ;

            INDArray image = Nd4j.create(featureData);
            INDArray label = Nd4j.create(labelData);
            DataSet myData = new DataSet(image, label);

            INDArray output = restored.output(myData.getFeatureMatrix()); //get the networks prediction
            eval.eval(myData.getLabels(), output); //check the prediction against the true class

            HashMap<Integer,Integer> positive = (HashMap<Integer,Integer>)eval.positive();
            for  (Map.Entry<Integer, Integer> p: positive.entrySet()) {
                logger.info(p.getKey()+" "+p.getValue());
            }


            for (int i=0; i<count; i++) {
                INDArray exp = myData.getLabels().getRow(i);
                INDArray pred = output.getRow(i);

                float maxpred = pred.getFloat(0);
                int maxexp = exp.getInt(0);
                int rpred=0;
                int rexp=0;

                for (int j=1; j<outputNum; j++) {

                    if (pred.getFloat(j) > maxpred) {
                        rpred=j;
                        maxpred = pred.getFloat(j);
                    }

                    if (exp.getInt(j) > maxexp) {
                        rexp=j;
                        maxexp = exp.getInt(j);
                    }
                }

                logger.info("Image "+i+ " is supposed to be " + rexp
                + " : model predict " + rpred);
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


    private Map getRecord(byte[] record) {

        String[] record_label = {"src_ip4", "dst_ip4", "nexthop", "input", "output", "dPkts", "dOctets", "first",
                "last", "src_port", "dst_port", "pad1", "flags", "nprot", "tos", "src_as", "dst_as", "src_mask",
                "dst_mask", "pad2"};
        String[] record_type = {"ip", "ip", "ip", "short", "short", "int", "int", "int", "int", "short", "short", "byte", "byte",
                "byte", "byte", "short", "short", "byte", "byte", "short"};

        Map<String, Object> record_value = new HashMap<>();

        byte shift = 0;
        byte[] ID = null;

        for (byte i = 0; i < record_label.length; i++) {
            switch (record_type[i]) {
                case "byte":
                    ID = Arrays.copyOfRange(record, shift, shift + 1);
                    record_value.put(record_label[i], getByte(ID));
                    shift += 1;
                    break;

                case "short":
                    ID = Arrays.copyOfRange(record, shift, shift + 2);
                    record_value.put(record_label[i], getShort(ID));
                    shift += 2;
                    break;

                case "int":
                    ID = Arrays.copyOfRange(record, shift, shift + 4);
                    record_value.put(record_label[i], readInt(ID));
                    shift += 4;
                    break;

                case "ip":
                    ID = Arrays.copyOfRange(record, shift, shift + 4);
                    record_value.put(record_label[i], getIP(ID));
                    shift += 4;
                    break;
            }
        }

        if (debug) {
            for (String label : record_label) {
                logger.debug(label + "=" + record_value.get(label));
            }
        }

        return (record_value);

    }

    private short getByte(byte[] buffer) {
        return ((short) (buffer[0] & 0xFF));
    }

    private int getShort(byte[] buffer) {
        return ((int) (((buffer[0] & 0xFF) << 8) | (buffer[1] & 0xFF)));
    }

    private long readInt(byte[] buffer) {
        return ((long) (((buffer[0] & 0xFF) << 24) | ((buffer[1] & 0xFF) << 16)
                | ((buffer[2] & 0xFF) << 8) | (buffer[3] & 0xFF)));
    }

    private String getIP(byte[] buffer) {

        try {
            for (byte i = 1; i < 4; i++) {
                buffer[i] = (byte) (buffer[i] & 0xFF);
            }
            InetAddress address = InetAddress.getByAddress(buffer);
            return (address.toString().replace("/", ""));
        } catch (UnknownHostException e) {
            logger.error("Bad host address");
        }
        return ("");
    }
}
