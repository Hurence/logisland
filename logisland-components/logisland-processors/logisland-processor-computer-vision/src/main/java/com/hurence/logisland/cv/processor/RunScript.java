/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
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
package com.hurence.logisland.cv.processor;

import java.awt.image.BufferedImage;
import java.io.*;

import clojure.lang.Compiler;
import clojure.lang.RT;
import clojure.lang.Var;
import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.ExtraDetailFile;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.ProcessException;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.utils.CVUtils;
import com.hurence.logisland.validator.ValidationContext;
import com.hurence.logisland.validator.ValidationResult;

import org.opencv.core.Core;
import org.opencv.core.Mat;
import org.scijava.nativelib.NativeLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.util.*;

import static com.hurence.logisland.utils.CVUtils.*;
import static java.util.stream.Collectors.joining;

/**
 * !!! WARNING !!!!
 * The python processor is currently an experimental feature : it is delivered as is, with the current set of features
 * and is subject to modifications in API or anything else in further logisland releases without warnings.
 * <p>
 * So far identified list of things still to be done:
 * - see TODOs here
 * - init code is always called! Remove this!
 * - inline mode: init usage (access context?))
 * - onPropertyModified up to python code (test)
 * - doc for tutorial (inline?, file? , both?)
 */

@Tags({"scripting", "python"})
@CapabilityDescription(
        " !!!! WARNING !!!!\n\nThe RunScript processor is currently an experimental feature : it is delivered as is, with the"
                + " current set of features and is subject to modifications in API or anything else in further logisland releases"
                + " without warnings. There is no tutorial yet. If you want to play with this processor, use the python-processing.yml"
                + " example and send the apache logs of the index apache logs tutorial. The debug stream processor at the end"
                + " of the stream should output events in stderr file of the executors from the spark console.\n\n"
                + "This processor allows to implement and run a processor written in python."
                + " This can be done in 2 ways. Either directly defining the process method code in the **script.code.process**"
                + " configuration property or poiting to an external python module script file in the **script.path**"
                + " configuration property. Directly defining methods is called the inline mode whereas using a script file is"
                + " called the file mode. Both ways are mutually exclusive. Whether using the inline of file mode, your"
                + " python code may depend on some python dependencies. If the set of python dependencies already delivered with"
                + " the Logisland framework is not sufficient, you can use the **dependencies.path** configuration property to"
                + " give their location. Currently only the nltk python library is delivered with Logisland.")
@ExtraDetailFile("./details/RunScript-Detail.rst")
public class RunScript extends AbstractProcessor {

    private static Logger logger = LoggerFactory.getLogger(RunScript.class);


    public static final PropertyDescriptor SCRIPT_CODE = new PropertyDescriptor.Builder()
            .name("script.code")
            .description("The clojure code to be called to process the records.")
            .required(true)
            .build();

    public static final PropertyDescriptor SCRIPT_NS = new PropertyDescriptor.Builder()
            .name("script.ns")
            .description("The clojure code to be called to process the records.")
            .required(false)
            .defaultValue("com.hurence.logisland")
            .build();

    public static final PropertyDescriptor SCRIPT_FUNCTION = new PropertyDescriptor.Builder()
            .name("script.function")
            .description("The clojure code namespace to be called to process the records.")
            .required(true)
            .build();


    public static final AllowableValue OVERWRITE =
            new AllowableValue("overwrite", "overwrite existing value", "the previous value will be overwritten");

    public static final AllowableValue NEW =
            new AllowableValue("new", "new value value", "the previous value will be kept and the new value goes to image.output.field");

    public static final PropertyDescriptor OUTPUT_MODE = new PropertyDescriptor.Builder()
            .name("output.mode")
            .description("Where do you want to store the processed image")
            .required(false)
            .defaultValue(OVERWRITE.getValue())
            .allowableValues(OVERWRITE, NEW)
            .build();


    public static final AllowableValue FIELD_BASED =
            new AllowableValue("field_based", "based on input field value", "the process method will act on field value");

    public static final AllowableValue RECORD_BASED =
            new AllowableValue("record_based", "based on the whole record", "the process method will act on the whole record");

    public static final PropertyDescriptor PROCESSING_MODE = new PropertyDescriptor.Builder()
            .name("processing.mode")
            .description("How does the processor get its data")
            .required(false)
            .defaultValue(FIELD_BASED.getValue())
            .allowableValues(FIELD_BASED, RECORD_BASED)
            .build();

    public static final PropertyDescriptor INPUT_FIELD = new PropertyDescriptor.Builder()
            .name("input.field")
            .description("The field containing the input value")
            .required(false)
            .defaultValue(FieldDictionary.RECORD_VALUE)
            .build();

    public static final PropertyDescriptor OUTPUT_FIELD = new PropertyDescriptor.Builder()
            .name("output.field")
            .description("The field containing the output value")
            .required(false)
            .defaultValue(FieldDictionary.RECORD_VALUE)
            .build();

    public static final PropertyDescriptor IMAGE_FORMAT = new PropertyDescriptor.Builder()
            .name("image.format")
            .description("The field containing the output value")
            .required(false)
            .defaultValue("jpg")
            .build();


    Var processFunction = null;

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(SCRIPT_CODE);
        descriptors.add(SCRIPT_FUNCTION);
        descriptors.add(SCRIPT_NS);
        descriptors.add(INPUT_FIELD);
        descriptors.add(OUTPUT_FIELD);
        descriptors.add(OUTPUT_MODE);
        descriptors.add(PROCESSING_MODE);
        descriptors.add(IMAGE_FORMAT);

        return Collections.unmodifiableList(descriptors);
    }


    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        final List<ValidationResult> validationResults = new ArrayList<>(super.customValidate(context));

        logger.debug("customValidate");

        if (context.getPropertyValue(PROCESSING_MODE).isSet() &&
                context.getPropertyValue(PROCESSING_MODE).asString().equals(RECORD_BASED.getValue())) {

            if (context.getPropertyValue(INPUT_FIELD).isSet()) {
                // attempt to use both modes -> error
                validationResults.add(
                        new ValidationResult.Builder()
                                .explanation("You must declare " + PROCESSING_MODE.getName() + " or " + INPUT_FIELD.getName() + " but not both")
                                .valid(false)
                                .build());
            }

            if (context.getPropertyValue(OUTPUT_FIELD).isSet()) {
                // attempt to use both modes -> error
                validationResults.add(
                        new ValidationResult.Builder()
                                .explanation("You must declare " + PROCESSING_MODE.getName() + " or " + OUTPUT_FIELD.getName() + " but not both")
                                .valid(false)
                                .build());
            }
        }

        return validationResults;
    }


    @Override
    public void init(final ProcessContext context) throws InitializationException {
        super.init(context);
        if(isInitialized)
            return;

        try {
            NativeLoader.loadLibrary(Core.NATIVE_LIBRARY_NAME);

            RT.load("clojure/core");
            StringReader stringReader = new StringReader(context.getPropertyValue(SCRIPT_CODE).asString());

            // Load the Clojure script -- as a side effect this initializes the runtime.
            Compiler.load(stringReader);

            processFunction = RT.var(context.getPropertyValue(SCRIPT_NS).asString(),
                    context.getPropertyValue(SCRIPT_FUNCTION).asString());

            isInitialized = true;
        } catch (IOException e) {
            throw new InitializationException(e);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }


    }


    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {
        if (!isInitialized) {
            logger.error("Processor not initialized, returning input records and doing nothing!");
            return records;
        }

        String imageInputField = context.getPropertyValue(INPUT_FIELD).asString();
        String imageOutputField = context.getPropertyValue(OUTPUT_FIELD).asString();
        String imageFormat = context.getPropertyValue(IMAGE_FORMAT).asString();

        // do we replace existing records or add new ones ?
        Collection<Record> outputRecords = new ArrayList<>(records);
        if (context.getPropertyValue(OUTPUT_MODE).asString().equals(OVERWRITE.getValue())) {
            outputRecords = new ArrayList<>();
        }

        // loop on incoming records
        for (Record record : records) {
            try {


                // do the OpenCV processing on a Mat
                Mat originalImageMat = toMat(record, imageInputField);
                Mat processedImageMat = (Mat) processFunction.invoke(originalImageMat);

                // convert processed Mat to image
                BufferedImage processedBufferedImage = toBI(processedImageMat);
                byte[] processedImageBytes = toBytes(processedBufferedImage, imageFormat);


                if (context.getPropertyValue(PROCESSING_MODE).asString().equals(FIELD_BASED.getValue())) {
                    record.setBytesField(imageOutputField, processedImageBytes)
                            .setIntField(FieldDictionary.IMAGE_HEIGHT, processedBufferedImage.getHeight())
                            .setIntField(FieldDictionary.IMAGE_WIDTH, processedBufferedImage.getWidth())
                            .setIntField(FieldDictionary.IMAGE_TYPE, processedBufferedImage.getType());
                }
                outputRecords.add(record);


            } catch (ProcessException | IOException t) {
                logger.error(t.toString());
            }

        }


        return outputRecords;


    }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {

        logger.info("property {} value changed from {} to {}", descriptor.getName(), oldValue, newValue);


    }

}
