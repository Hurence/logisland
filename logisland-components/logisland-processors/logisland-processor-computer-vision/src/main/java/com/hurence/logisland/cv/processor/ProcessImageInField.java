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
import clojure.lang.LineNumberingPushbackReader;
import clojure.lang.RT;
import clojure.lang.Var;
import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.ExtraDetailFile;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.ProcessException;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;
import com.hurence.logisland.validator.ValidationContext;
import com.hurence.logisland.validator.ValidationResult;

import org.opencv.core.Core;
import org.opencv.core.Mat;
import org.scijava.nativelib.NativeLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.util.*;

import static com.hurence.logisland.utils.CVUtils.toBI;
import static com.hurence.logisland.utils.CVUtils.toMat;
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
        " !!!! WARNING !!!!\n\nThe ProcessImageInField processor is currently an experimental feature : it is delivered as is, with the"
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
@ExtraDetailFile("./details/ProcessImageInField-Detail.rst")
public class ProcessImageInField extends AbstractProcessor {

    private static Logger logger = LoggerFactory.getLogger(ProcessImageInField.class);


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

    public static final PropertyDescriptor IMAGE_FIELD = new PropertyDescriptor.Builder()
            .name("image.field")
            .description("The field containing ")
            .required(true)
            .build();

    public static final PropertyDescriptor OUTPUT_MODE = new PropertyDescriptor.Builder()
            .name("output.mode")
            .description("The field containing ")
            .required(true)
            .build();

    Var processFunction = null;

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(SCRIPT_CODE);
        descriptors.add(SCRIPT_FUNCTION);
        descriptors.add(SCRIPT_NS);

        return Collections.unmodifiableList(descriptors);
    }


    @Override
    public void init(final ProcessContext context) throws InitializationException {
        super.init(context);
        try {
            NativeLoader.loadLibrary(Core.NATIVE_LIBRARY_NAME);

            RT.load("clojure/core");
            StringReader stringReader = new StringReader(context.getPropertyValue(SCRIPT_CODE).asString());

            Var.pushThreadBindings(
                    RT.map(RT.CURRENT_NS, RT.CURRENT_NS.deref(),
                            RT.IN, new LineNumberingPushbackReader(stringReader)));


            processFunction = RT.var(context.getPropertyValue(SCRIPT_NS).asString(),
                    context.getPropertyValue(SCRIPT_FUNCTION).asString());

            initDone();
        } catch (IOException e) {
            throw new InitializationException(e);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }


    }


    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {

        String imageField = context.getPropertyValue(IMAGE_FIELD).asString();

        if (isInitialized) {
            Collection<Record> outputRecords = new ArrayList<>();

            try {

                for (Record record : records) {
                    if(record.hasField(imageField)){
                        // make an image from bytes
                        byte[] inputBytes = record.getField(imageField).asBytes();
                        BufferedImage originalImage = ImageIO.read(new ByteArrayInputStream(inputBytes));

                        // do the OpenCV processing on a Mat
                        Mat mat = toMat(originalImage);
                        Mat processedMat = (Mat) processFunction.invoke(mat);

                        // convert processed Mat to image
                        BufferedImage processedImage = toBI(processedMat);
                        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

                        // TODO handle format name
                        ImageIO.write(processedImage, "jpg", byteArrayOutputStream);
                        byte[] outputBytes = byteArrayOutputStream.toByteArray();
                        byteArrayOutputStream.close();

                        // TODO handle output policies
                        record.setField(imageField, FieldType.BYTES, outputBytes);

                    }else {
                        throw new ProcessException("no field");
                    }

                }

            } catch (Throwable t) {

            }

            return outputRecords;
        } else {
            return records;
        }

    }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {

        logger.info("property {} value changed from {} to {}", descriptor.getName(), oldValue, newValue);


    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        final List<ValidationResult> validationResults = new ArrayList<>(super.customValidate(context));

        logger.debug("customValidate");

        /**
         * Either script file mode or inline mode, not both
         */
      /*  if (context.getPropertyValue(SCRIPT_CODE_PROCESS).isSet()) {
            // inline mode, 
            if (context.getPropertyValue(SCRIPT_PATH).isSet()) {
                // attempt to use both modes -> error
                validationResults.add(
                        new ValidationResult.Builder()
                                .explanation("You must declare " + SCRIPT_CODE_PROCESS.getName() + " or " + SCRIPT_PATH.getName() + " but not both")
                                .valid(false)
                                .build());
            }
        } */

        return validationResults;
    }
}
