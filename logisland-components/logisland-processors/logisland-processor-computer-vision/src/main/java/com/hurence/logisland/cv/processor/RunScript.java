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
import com.hurence.logisland.validator.ValidationContext;
import com.hurence.logisland.validator.ValidationResult;

import org.opencv.core.Core;
import org.opencv.core.Mat;
import org.scijava.nativelib.NativeLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.hurence.logisland.cv.utils.CVUtils.*;


@Tags({"scripting", "clojure", "opencv", "image"})
@CapabilityDescription(
        "This processor allows to run a processor written in clojure."
                + " directly by defining the process method code in the **script.code**"
                + " Currently only the opencv library is delivered with Logisland.")
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
            new AllowableValue("new", "new value value", "the previous value will be kept and the new value will be added to the input list");

    public static final PropertyDescriptor OUTPUT_MODE = new PropertyDescriptor.Builder()
            .name("output.mode")
            .description("Where do you want to store the processed image")
            .required(false)
            .defaultValue(OVERWRITE.getValue())
            .allowableValues(OVERWRITE, NEW)
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
        descriptors.add(IMAGE_FORMAT);

        return Collections.unmodifiableList(descriptors);
    }


    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        final List<ValidationResult> validationResults = new ArrayList<>(super.customValidate(context));

        logger.debug("customValidate");

       /* if (context.getPropertyValue(PROCESSING_MODE).isSet() &&
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
        }*/

        return validationResults;
    }


    @Override
    public void init(final ProcessContext context) throws InitializationException {
        super.init(context);

        try {
            NativeLoader.loadLibrary(Core.NATIVE_LIBRARY_NAME);

            RT.load("clojure/core");
            StringReader stringReader = new StringReader(context.getPropertyValue(SCRIPT_CODE).asString());

            // Load the Clojure script -- as a side effect this initializes the runtime.
            Compiler.load(stringReader);

            processFunction = RT.var(context.getPropertyValue(SCRIPT_NS).asString(),
                    context.getPropertyValue(SCRIPT_FUNCTION).asString());

            isInitialized = true;
        } catch (IOException | ClassNotFoundException e) {
            throw new InitializationException(e);
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


                record.setBytesField(imageOutputField, processedImageBytes)
                        .setIntField(FieldDictionary.IMAGE_HEIGHT, processedBufferedImage.getHeight())
                        .setIntField(FieldDictionary.IMAGE_WIDTH, processedBufferedImage.getWidth())
                        .setIntField(FieldDictionary.IMAGE_TYPE, processedBufferedImage.getType());

                outputRecords.add(record);


            } catch (Throwable t) {
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
