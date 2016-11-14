/**
 * Copyright (C) 2016 Hurence
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
package com.hurence.logisland.processor.scripting.python;

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.*;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.util.validator.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.python.util.PythonInterpreter;

import java.util.*;

@Tags({"scripting", "python"})
@CapabilityDescription("This processor allows to implement a processor written in python")
public class PythonProcessor extends AbstractProcessor {

    private static Logger logger = LoggerFactory.getLogger(PythonProcessor.class);
    
    // Python interpreter
    private PythonInterpreter pythonInterpreter = new PythonInterpreter();

    public static final PropertyDescriptor PYTHON_PROCESSOR_SCRIPT = new PropertyDescriptor.Builder()
            .name("python_processor.python_rocessor_script")
            .description("The path to the python processor script")
            .required(true)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(PYTHON_PROCESSOR_SCRIPT);

        return Collections.unmodifiableList(descriptors);
    }
    
    @Override
    public void init(final ProcessContext context) {

        final String pythonProcessorScript = context.getProperty(PYTHON_PROCESSOR_SCRIPT).asString();
        
        logger.info("Python processor: initializing " + pythonProcessorScript);
        
        pythonInterpreter.execfile(pythonProcessorScript);
        
        pythonInterpreter.eval("")
        
        //pythonInterpreter.
    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {

       
        List<Record> outputRecords = new ArrayList<>();
        
        return outputRecords;
    }
    
    @Override
    public Collection<Record> process(ProcessContext context, Record record) {
        return null;
        
    }
}
