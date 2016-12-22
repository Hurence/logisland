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
import org.python.core.PyInteger;
import org.python.core.PyObject;
import org.python.util.PythonInterpreter;

import java.io.File;
import java.util.*;

@Tags({"scripting", "python"})
@CapabilityDescription("This processor allows to implement and run a processor written in python")
public class PythonProcessor extends AbstractProcessor {

    private static Logger logger = LoggerFactory.getLogger(PythonProcessor.class);
   
    // Path to the user's python processor script
    private String pythonProcessorScriptPath = null;
    
    // Python processor name (derived from pythonProcessorScriptPath)
    private String pythonProcessorName = null;
    // Python processor directory (derived from pythonProcessorScriptPath)
    private String pythonProcessorDirectoryPath = null;
    
    // Path to the directory of the dependencies of the processor script
    private String pythonProcessorDependenciesPath = null;
    // True if one must load dependencies in the dependencies path
    private boolean hasDependencies = false;

    // Path of the directory containing the logisland python modules (delivered with logisland)
    private String pythonLogislandModulesPath = null;
    
    // Python interpreter
    private PythonInterpreter pythonInterpreter = new PythonInterpreter();

    // Reference to the python processor object (instance of the user's processor code)
    private PyObject pyProcessor = null;
    
    private boolean initDone = false;

    public static final PropertyDescriptor PYTHON_PROCESSOR_SCRIPT_PATH = new PropertyDescriptor.Builder()
            .name("python_processor.python_processor_script_path")
            .description("The path to the user's python processor script")
            .required(true)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();
    
    // Name of the default directory for dependencies of the processor script
    private static final String DEFAULT_DEPENDENCIES_DIRNAME = "dependencies";
    
    public static final PropertyDescriptor PYTHON_PROCESSOR_DEPENDENCIES_PATH = new PropertyDescriptor.Builder()
            .name("python_processor.python_processor_dependencies_path")
            .description("The path to the dependencies for the user's python processor script. If not set, the following"
                    + " default directory is used: <directory_holding_processor_script>"
                    + File.separator + DEFAULT_DEPENDENCIES_DIRNAME)
            .required(false)
            .addValidator(StandardValidators.createDirectoryExistsValidator(false, false))
            .build();
    
    public static final PropertyDescriptor PYTHON_LOGISLAND_MODULES_PATH = new PropertyDescriptor.Builder()
            .name("python_processor.python_logisland_modules_path")
            .description("The path to the directory containing the logisland python modules")
            .defaultValue("src/main/resources/python") // Default path
            .required(true)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors()
    {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(PYTHON_PROCESSOR_SCRIPT_PATH);
        descriptors.add(PYTHON_PROCESSOR_DEPENDENCIES_PATH);
        descriptors.add(PYTHON_LOGISLAND_MODULES_PATH);

        return Collections.unmodifiableList(descriptors);
    }
    
    /**
     * Gets config parameters
     */
    private void getConfigParameters(ProcessContext context)
    {   
        // Extract needed configuration information
        try {
            getPythonProcessorNameAndDirectory(context);
            getPythonProcessorDependenciesPath(context);
            getPythonLogislandModulesPath(context);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }       
    }

    @Override
    public void init(final ProcessContext context)
    {
        // Get config parameters
        getConfigParameters(context);
        
        logger.info("Initializing python processor: " + pythonProcessorScriptPath);
        
        // Load necessary logisland python modules
        loadLogislandPythonModules();
        
        if (hasDependencies)
        {
            loadPythonProcessorDependencies();
        }
              
        // Load processor script
        pythonInterpreter.execfile(pythonProcessorScriptPath);

        /**
         * Instantiate and init the processor python object
         * 
         * Run python code:
         *
         * pyProcessor = MyProcessor()
         * # Check that the python class is inheriting from AbstractProcessor python class
         * isInheritingFromAbstractProcessor = issubclass(pyProcessor.__class__, AbstractProcessor)
         * pyProcessor.init(context)
         */

        pythonInterpreter.exec("pyProcessor = " + pythonProcessorName + "()" ); // Equivalent to "pyProcessor = MyProcessor()"
        pyProcessor = pythonInterpreter.get("pyProcessor");
        
        // Check that the python class is inheriting from AbstractProcessor python class
        pythonInterpreter.exec("isInheritingFromAbstractProcessor = issubclass(pyProcessor.__class__, AbstractProcessor)");
        PyObject pyIsInheritingFromAbstractProcessor = pythonInterpreter.get("isInheritingFromAbstractProcessor");
        boolean isInheritingFromAbstractProcessor = ((PyInteger) pyIsInheritingFromAbstractProcessor).asInt() != 0;
        if (!isInheritingFromAbstractProcessor)
        {
            // This is not a python logisland processor
            throw new RuntimeException("Not a logisland python processor class: " +
                    pythonProcessorName + " does not inherits from AbstractProcessor python class" );
        }

        pythonInterpreter.set("context", context);
        pythonInterpreter.exec("pyProcessor.init(context)");
        
        // Allow forwarding calls to onPropertyModified
        initDone = true;
    }
    
    /**
     * Loads the logisland python modules
     */
    private void loadLogislandPythonModules()
    {
        // TODO: this system to be reinforced or replaced by a security feature allowing to load only specific
        // python modules. A potential way of doing this seems to be the usage of sys.meta_path
        pythonInterpreter.exec("import sys"); // Allows to call next sys.path.append
        pythonInterpreter.exec("sys.path.append('" + pythonLogislandModulesPath + "')");
    }
    
    /**
     * Loads the python processor dependencies
     */
    private void loadPythonProcessorDependencies()
    {
        logger.info("Using python processor dependencies under: " + pythonProcessorDependenciesPath);
        // 'import sys' has already been called in loadLogislandPythonModules, so just add the path to sys.path
        pythonInterpreter.exec("sys.path.append('" + pythonProcessorDependenciesPath + "')");
    }
    
    /**
     * Gets the name and directory of the processor from the processor script file path of the configuration
     * @param context Logisland context
     * @throws Exception
     */
    private void getPythonProcessorNameAndDirectory(ProcessContext context) throws Exception
    {
        String configPythonProcessorScriptPath = context.getProperty(PYTHON_PROCESSOR_SCRIPT_PATH).asString();
        
        File processorFile = null;
        try {
            processorFile = new File(configPythonProcessorScriptPath);
        } catch (NullPointerException npe)
        {
            throw new Exception("Null python processor script path");
        }
        
        if (!processorFile.isFile())
        {
            throw new Exception("Python processor script path is not a file: " + configPythonProcessorScriptPath);
        }
        
        /**
         * Replace path with absolute path name
         */
        this.pythonProcessorScriptPath = processorFile.getAbsolutePath();
        
        /**
         * Get the directory in which resides the python processor script file
         */
        
        File parentFile = processorFile.getParentFile();
        if (parentFile == null)
        {
            parentFile = new File(".");
        }
        this.pythonProcessorDirectoryPath = parentFile.getAbsolutePath();
        
        /**
         * Get the name of the python processor script file
         */
        
        String processorFileName = processorFile.getName();
        
        if (!processorFileName.endsWith(".py"))
        {
            throw new Exception("Python processor script file should end with .py extension: " + configPythonProcessorScriptPath);
        }
        
        if (processorFileName.startsWith(".py"))
        {
            throw new Exception("Invalid python porcessor script path: " + configPythonProcessorScriptPath);
        }
        
        this.pythonProcessorName =  processorFileName.substring(0, processorFileName.length()-3);
    }
    
    /**
     * Gets the directory for dependencies of the processor script 
     * @param context Logisland context
     * @throws Exception
     */
    private void getPythonProcessorDependenciesPath(ProcessContext context) throws Exception
    {
        String configPythonProcessorDependenciesPath = context.getProperty(PYTHON_PROCESSOR_DEPENDENCIES_PATH).asString();
        
        /**
         * If no dependences path is given, use the default one which is the a directory named
         * DEFAULT_DEPENDENCIES_DIRNAME at the same location of the python processor script and us it if it exists,
         * otherwise use the specified one
         */
        if (configPythonProcessorDependenciesPath == null)
        {
            this.pythonProcessorDependenciesPath = this.pythonProcessorDirectoryPath + File.separator + DEFAULT_DEPENDENCIES_DIRNAME;
            logger.info("No python processor dependencies path specified, using default one: " + pythonProcessorDependenciesPath);
            
            File dependenciesDir = null;
            try {
                dependenciesDir = new File(pythonProcessorDependenciesPath);
            } catch (NullPointerException npe)
            {
                // Cannot happen in fact...
                throw new Exception("Null python processor dependencies path");
            }

            if (dependenciesDir.exists())
            {
                if (dependenciesDir.isDirectory())
                {
                    hasDependencies = true;
                }
                else
                {
                    // Exists but is a file !! Strange...
                    logger.info(pythonProcessorDependenciesPath + " is a file, not a directory");
                }
            }
        }
        else
        {
            // Dependencies path is provided, check and use it
            
            File dependenciesDir = null;
            try {
                dependenciesDir = new File(configPythonProcessorDependenciesPath);
            } catch (NullPointerException npe)
            {
                // Cannot happen in fact...
                throw new Exception("Null python processor dependencies path");
            }
            
            if (!dependenciesDir.isDirectory())
            {
                throw new Exception("Python processor dependencies path is not a directory: " + configPythonProcessorDependenciesPath);
            }
            
            this.pythonProcessorDependenciesPath = dependenciesDir.getAbsolutePath();
            hasDependencies = true;
        }
    }
    
    /**
     * Gets the directory containing the logisland python modules (delivered with logisland)
     * @param context Logisland context
     * @throws Exception
     */
    private void getPythonLogislandModulesPath(ProcessContext context) throws Exception
    {
        // TODO: can this be found with correct default value poiting to resources in jar file  
        pythonLogislandModulesPath = context.getProperty(PYTHON_LOGISLAND_MODULES_PATH).asString();
        logger.info("Using python logisland modules path: " + pythonLogislandModulesPath);
    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records)
    {
        logger.debug("Processing multi records: " + records);

        Collection<Record> outputRecords = null;

        /**
         * Call process method of python processor script with the records we received
         */
        pythonInterpreter.set("context", context);
        pythonInterpreter.set("records", records);
        pythonInterpreter.exec("outputRecords = pyProcessor.process(context, records)");
        outputRecords = pythonInterpreter.get("outputRecords", Collection.class);

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

        logger.info("property {} value changed from {} to {}", descriptor.getName(), oldValue, newValue);
        
        /**
         * In java, onPropertyModified for config properties is called before the init method is called so before our
         * init method is called our python interpreter is not initialized and we cannot call it before
         */
        if (initDone)
        {
            /**
             * pyProcessor.onPropertyModified(context, oldValue, newValue)
             */
            pythonInterpreter.set("descriptor", descriptor);
            pythonInterpreter.set("oldValue", oldValue);
            pythonInterpreter.set("newValue", newValue);
            pythonInterpreter.exec("pyProcessor.onPropertyModified(context, oldValue, newValue)");
        }
    }
}
