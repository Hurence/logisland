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
import org.python.core.PyObject;
import org.python.util.PythonInterpreter;

import java.io.File;
import java.util.*;

@Tags({"scripting", "python"})
@CapabilityDescription("This processor allows to implement and run a processor written in python")
public class PythonProcessor extends AbstractProcessor {

    private static Logger logger = LoggerFactory.getLogger(PythonProcessor.class);
   
    // Path to the user's python processor script
    private String pythonProcessorScript = null;

    // Path of the directory containing the logisland python modules
    private String pythonModulesPath = null;
    
    // Python interpreter
    private PythonInterpreter pythonInterpreter = new PythonInterpreter();

    // Reference to the python processor object (instance of the user's processor code)
    private PyObject pyProcessor = null;

    public static final PropertyDescriptor PYTHON_PROCESSOR_SCRIPT = new PropertyDescriptor.Builder()
            .name("python_processor.python_rocessor_script")
            .description("The path to the user's python processor script")
            .required(true)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor PYTHON_LOGISLAND_MODULES_PATH = new PropertyDescriptor.Builder()
            .name("python_processor.python_logisland_modules_path_script")
            .description("The path to the directory containing the logisland python modules")
            .defaultValue("src/main/resources/python") // Default path
            .required(true)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors()
    {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(PYTHON_PROCESSOR_SCRIPT);
        descriptors.add(PYTHON_LOGISLAND_MODULES_PATH);

        return Collections.unmodifiableList(descriptors);
    }
    
    /**
     * Gets config parameters
     */
    private void getConfigParameters(ProcessContext context)
    {
        pythonProcessorScript = context.getProperty(PYTHON_PROCESSOR_SCRIPT).asString();
        pythonModulesPath = context.getProperty(PYTHON_LOGISLAND_MODULES_PATH).asString();
    }
    
    @Override
    public void init(final ProcessContext context)
    {
        // Get config parameters
        getConfigParameters(context);
        
        logger.info("Python processor: initializing " + pythonProcessorScript);
        
        // Load necessary logisland python modules
        loadLogislandPythonModules();
        
        // Get python processor name
        String pythonProcessorName = null;
        try {
            pythonProcessorName = getPythonProcessorName(pythonProcessorScript);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
              
        // Load processor script
        pythonInterpreter.execfile(pythonProcessorScript);
        
        /**
         * Instantiate and init the processor python object
         */

        pythonInterpreter.exec("pyProcessor = " + pythonProcessorName + "()" ); // Equivalent to "pyProcessor = MyProcessor()"
        pyProcessor = pythonInterpreter.get("pyProcessor");
        
        pythonInterpreter.set("context", context);
        pythonInterpreter.exec("pyProcessor.init(context)"); // TODO see if one can access context insiders in python code
    }
    
    /**
     * Loads the logisland python modules
     */
    private void loadLogislandPythonModules()
    {
        // TODO: this system to be reinforced or replaced by a security feature allowing to load only specific
        // python modules. A potential way of doing this seems to be the usage of sys.meta_path
        
        logger.info("Using logsiland python modules directory: " + pythonModulesPath);
        pythonInterpreter.exec("import sys"); // Allows to call next sys.path.append
        pythonInterpreter.exec("sys.path.append('" + pythonModulesPath + "')");
    }
    
    /**
     * Gets the name of the processor from the processor script file name
     * @param pythonProcessorScript Path to processor script file
     * @return
     * @throws Exception
     */
    private String getPythonProcessorName(String pythonProcessorScript) throws Exception
    {
        File processorFile = null;
        try {
            processorFile = new File(pythonProcessorScript);
        } catch (NullPointerException npe)
        {
            throw new Exception("Null python processor script");
        }
        
        String processorFileName = processorFile.getName();
        
        if (!processorFileName.endsWith(".py"))
        {
            throw new Exception("Python processor script file should end with .py: " + processorFileName);
        }
        
        if (processorFileName.startsWith(".py"))
        {
            throw new Exception("No python processor name in .py");
        }
        
        String pythonProcessorName = processorFileName.substring(0, processorFileName.length()-3);

        return pythonProcessorName;   
    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records)
    {
        logger.debug("Processing multi records: " + records);

        // TODO call python processor code
        List<Record> outputRecords = new ArrayList<>();
        
        return outputRecords;
    }
    
    @Override
    public Collection<Record> process(ProcessContext context, Record record) {
        
        logger.debug("Processing mono record: " + record);
        
        // TODO call python processor code
        return process(context, Collections.singleton(record));
    }
    
    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        // TODO call python processor code
        logger.info("property {} value changed from {} to {}", descriptor.getName(), oldValue, newValue);
    }
}
