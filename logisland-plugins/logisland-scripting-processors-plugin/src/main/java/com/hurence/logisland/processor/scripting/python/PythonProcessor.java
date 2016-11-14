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
@CapabilityDescription("This processor allows to implement a processor written in python")
public class PythonProcessor extends AbstractProcessor {

    private static Logger logger = LoggerFactory.getLogger(PythonProcessor.class);
    
    /**
     * Logisland python modules definitions
     */
    private static final String logislandPythonModulesBasePath = File.separator + "python";
    //private static final String logislandPythonModulesBasePath = "./src/main/resources/python";
    private static final Set<String> logislandPythonModules = new HashSet<String>();
    
    static
    {
        // Set logisland python modules to be loaded
        logislandPythonModules.add("AbstractProcessor");
    }
    
    // Python interpreter
    private PythonInterpreter pythonInterpreter = new PythonInterpreter();

    public static final PropertyDescriptor PYTHON_PROCESSOR_SCRIPT = new PropertyDescriptor.Builder()
            .name("python_processor.python_rocessor_script")
            .description("The path to the python processor script")
            .required(true)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors()
    {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(PYTHON_PROCESSOR_SCRIPT);

        return Collections.unmodifiableList(descriptors);
    }
    
    @Override
    public void init(final ProcessContext context)
    {

        final String pythonProcessorScript = context.getProperty(PYTHON_PROCESSOR_SCRIPT).asString();
        
        logger.info("Python processor: initializing " + pythonProcessorScript);
        
        // Get python processor name
        String pythonProcessorName = null;
        try {
            pythonProcessorName = getPythonProcessorName(pythonProcessorScript);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Load necessary logisland python modules
        loadLogislandPythonModules();
        
        // Load processor script
        pythonInterpreter.execfile(pythonProcessorScript);
        
        // processor = MyProcessor()
        PyObject pyProcessor = pythonInterpreter.eval("processor = " + pythonProcessorName + "()" );
        
        //pythonInterpreter.
    }
    
    /**
     * Loads the logisland python modules
     */
    private void loadLogislandPythonModules()
    {
        // TODO: this system to be reinforced or replaced by a security feature allowing to load only specific
        // python modules. A potential way of doing this seems to be the usage of sys.meta_path

        Class thisClass = getClass();
        for (String logislandModule : logislandPythonModules)
        {
            String logislandModulePath = logislandPythonModulesBasePath + File.separator + logislandModule + ".py";
            logger.info("Loading logisland python module: " + logislandModulePath);
            pythonInterpreter.execfile(thisClass.getResourceAsStream(logislandModulePath));
            //pythonInterpreter.execfile(logislandModulePath);
        }
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
        // TODO
        List<Record> outputRecords = new ArrayList<>();
        
        return outputRecords;
    }
    
    @Override
    public Collection<Record> process(ProcessContext context, Record record) {
        
        // TODO
        return null;
    }
}
