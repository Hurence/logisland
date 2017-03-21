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

import com.google.common.collect.Lists;
import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.*;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;
import com.hurence.logisland.validator.ValidationContext;
import com.hurence.logisland.validator.ValidationResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.python.core.PyInteger;
import org.python.core.PyObject;
import org.python.util.PythonInterpreter;

import java.io.File;
import java.util.*;

import static java.util.stream.Collectors.joining;

/**
 * 
 * !!! WARNING !!!!
 * The python processor is currently an experimental feature : it is delivered as is, with the current set of features
 * and is subject to modifications in API or anything else in further logisland releases without warnings.
 * 
 * So far identified list of things still to be done:
 * - see TODOs here
 * - init code is always called! Remove this!
 * - inline mode: default imports list
 * - inline mode: user imports  usage, init usage (access context?))
 * - onPropertyModified up to python code (test)
 * - doc for tutorial (inline?, file? , both?)
 */

@Tags({"scripting", "python"})
@CapabilityDescription(
        " !!!! WARNING !!!!\n\nThe python processor is currently an experimental feature : it is delivered as is, with the"
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
public class PythonProcessor extends AbstractProcessor {

    private static Logger logger = LoggerFactory.getLogger(PythonProcessor.class);
    
    // Python code for import statements
    private String scriptCodeImports = null;
    
    // Python code for init method
    private String scriptCodeInit = null;
    
    // Python code for process method
    private String scriptCodeProcess = null;
   
    // Path to the user provided python processor script file
    private String scriptPath = null;
    
    // Python processor name (derived from scriptPath)
    private String processorName = null;
    // Python processor directory (derived from scriptPath)
    private String scriptDirectory = null;
    
    // Path to the directory of the dependencies of the processor script
    private String dependenciesPath = null;
    // True if one must load dependencies in the dependencies path
    private boolean hasDependencies = false;

    // Path of the directory containing the logisland pyhton dependencies (shipped with logisland)
    private String logislandDependenciesPath = null;
    
    // Python interpreter
    private PythonInterpreter pythonInterpreter = null;

    // Reference to the python processor object (instance of the user's processor code)
    private PyObject pyProcessor = null;

    private boolean initDone = false;

    // If true, the python code to use resides in a script file, otherwise the code resides in the script.init and
    // script.process properties
    private boolean useScriptFile = false;
    
    private static final String KEY_SCRIPT_CODE_IMPORTS = "script.code.imports";
    private static final String KEY_SCRIPT_CODE_INIT = "script.code.init";
    private static final String KEY_SCRIPT_CODE_PROCESS = "script.code.process";
    private static final String KEY_SCRIPT_PATH = "script.path";
    private static final String KEY_DEPENDENCIES_PATH = "dependencies.path";
    private static final String KEY_LOGISLAND_DEPENDENCIES_PATH = "logisland.dependencies.path";
    
    public static final PropertyDescriptor SCRIPT_CODE_IMPORTS = new PropertyDescriptor.Builder()
            .name(KEY_SCRIPT_CODE_IMPORTS)
            .description("For inline mode only. This is the pyhton code that should hold the import statements if required.")
            .required(false)
            .build();

    public static final PropertyDescriptor SCRIPT_CODE_INIT = new PropertyDescriptor.Builder()
            .name(KEY_SCRIPT_CODE_INIT)
            .description("The python code to be called when the processor is initialized. This is the python"
                    + " equivalent of the init method code for a java processor. This is not mandatory but can only"
                    + " be used if **" + KEY_SCRIPT_CODE_PROCESS + "** is defined (inline mode).")
            .required(false)
            .build();
    
    public static final PropertyDescriptor SCRIPT_CODE_PROCESS = new PropertyDescriptor.Builder()
            .name(KEY_SCRIPT_CODE_PROCESS)
            .description("The python code to be called to process the records. This is the pyhton equivalent"
                    + " of the process method code for a java processor. For inline mode, this is the only minimum"
                    + " required configuration property. Using this property, you may also optionally define the **"
                    + KEY_SCRIPT_CODE_INIT + "** and **" + KEY_SCRIPT_CODE_IMPORTS + "** properties.")
            .required(false)
            .build();

    public static final PropertyDescriptor SCRIPT_PATH = new PropertyDescriptor.Builder()
            .name(KEY_SCRIPT_PATH)
            .description("The path to the user's python processor script. Use this property for file mode. Your python"
                    + " code must be in a python file with the following constraints: let's say your pyhton script is"
                    + " named MyProcessor.py. Then MyProcessor.py is a module file that must contain a class"
                    + " named MyProcessor which must inherits from the Logisland delivered class named AbstractProcessor."
                    + " You can then define your code in the process method and in the other traditional methods (init...)"
                    + " as you would do in java in a class inheriting from the AbstractProcessor java class.")
            .required(false)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();
    
    // Name of the default directory for dependencies of the processor script
    private static final String DEFAULT_DEPENDENCIES_DIRNAME = "dependencies";
    
    public static final PropertyDescriptor DEPENDENCIES_PATH = new PropertyDescriptor.Builder()
            .name(KEY_DEPENDENCIES_PATH)
            .description("The path to the additional dependencies for the user's python code, whether using inline or"
                    + " file mode. This is optional as your code may not have additional dependencies. If you defined **"
                    + KEY_SCRIPT_PATH + "** (so using file mode) and if **" + KEY_DEPENDENCIES_PATH + "** is not defined,"
                    + " Logisland will scan a potential directory named **" + DEFAULT_DEPENDENCIES_DIRNAME
                    + "** in the same directory where the script file resides and if it exists, any python code located"
                    + " there will be loaded as dependency as needed.")
            .required(false)
            .addValidator(StandardValidators.createDirectoryExistsValidator(false, false))
            .build();
    
    public static final PropertyDescriptor LOGISLAND_DEPENDENCIES_PATH = new PropertyDescriptor.Builder()
            .name(KEY_LOGISLAND_DEPENDENCIES_PATH)
            .description("The path to the directory containing the python dependencies shipped with logisland. You"
                    + " should not have to tune this parameter.")
            .required(false)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();
    
    // Default import statements automatically done in inline mode, before potential user ones are done.
    // TODO Also import StandardProcessContext, Processor (see when using/testing init method)
    private static final String DEFAULT_INLINE_MODE_IMPORTS =
            "from AbstractProcessor import AbstractProcessor\n" +
            "from com.hurence.logisland.record import StandardRecord"; 

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors()
    {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(SCRIPT_CODE_IMPORTS);
        descriptors.add(SCRIPT_CODE_INIT);
        descriptors.add(SCRIPT_CODE_PROCESS);
        descriptors.add(SCRIPT_PATH);
        descriptors.add(DEPENDENCIES_PATH);
        descriptors.add(LOGISLAND_DEPENDENCIES_PATH);

        return Collections.unmodifiableList(descriptors);
    }
    
    /**
     * Gets config parameters
     */
    private void getConfigParams(ProcessContext context)
    {   
        // Extract needed configuration information
        try {
            getInlineModeParams(context);
            getFileModeParams(context);
            getDependenciesPathParam(context);
            getLogislandDependenciesPathParam(context);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }       
    }

    @Override
    public void init(final ProcessContext context)
    {
        pythonInterpreter = new PythonInterpreter();
        // Get config parameters
        getConfigParams(context);

        // Load necessary logisland python modules
        loadLogislandPythonModules();
        
        if (hasDependencies)
        {
            loadPythonProcessorDependencies();
        }
        
        if (useScriptFile)
        {
            // File mode
            
            logger.debug("Initializing python processor (script file mode): " + scriptPath);

            // Load processor script
            pythonInterpreter.execfile(scriptPath);
    
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
    
            pythonInterpreter.exec("pyProcessor = " + processorName + "()" ); // Equivalent to "pyProcessor = MyProcessor()"
            pyProcessor = pythonInterpreter.get("pyProcessor");
            
            // Check that the python class is inheriting from AbstractProcessor python class
            pythonInterpreter.exec("isInheritingFromAbstractProcessor = issubclass(pyProcessor.__class__, AbstractProcessor)");
            PyObject pyIsInheritingFromAbstractProcessor = pythonInterpreter.get("isInheritingFromAbstractProcessor");
            boolean isInheritingFromAbstractProcessor = ((PyInteger) pyIsInheritingFromAbstractProcessor).asInt() != 0;
            if (!isInheritingFromAbstractProcessor)
            {
                // This is not a python logisland processor
                throw new RuntimeException("Not a logisland python processor class: " +
                        processorName + " does not inherits from AbstractProcessor python class" );
            }
    
            pythonInterpreter.set("context", context);
            pythonInterpreter.exec("pyProcessor.init(context)");
        } else
        {
            // Inline mode
            
            /**
             * Do something like:
             * 
             * # Default imports
             * import...
             * 
             * # User imports
             * import....
             * import...
             * 
             * # Call init user code if any
             * init(context)
             * 
             * # Define process method once for all with code from the user
             * def process(context, records):
             *     ....
             */

            pythonInterpreter.exec(DEFAULT_INLINE_MODE_IMPORTS);

            // Declare imports once for all if any
            if (scriptCodeImports != null)
            {
                pythonInterpreter.exec(scriptCodeImports);
            }

            // Call init method code if any
            if (scriptCodeInit != null)
            {
                pythonInterpreter.set("context", context);
                pythonInterpreter.exec(scriptCodeInit);
            }
            
            // Define the process method

            String statement = Lists.newArrayList(scriptCodeProcess.split("\n"))
                    .stream()
                    .map( t -> String.format("  %s", t))
                    .collect(joining("\n"));
            pythonInterpreter.exec("def process(context, records):\n" + statement);
        }
        
        // Allow forwarding calls to onPropertyModified
        initDone = true;
    }
    
    /**
     * Loads the logisland python modules
     */
    private void loadLogislandPythonModules()
    {       
        pythonInterpreter.exec("import sys"); // Allows to call next sys.path.append
        pythonInterpreter.exec("sys.path.append('" + logislandDependenciesPath + "')");
    }
    
    /**
     * Loads the python processor dependencies
     */
    private void loadPythonProcessorDependencies()
    {
        logger.debug("Using python processor dependencies under: " + dependenciesPath);
        // 'import sys' has already been called in loadLogislandPythonModules, so just add the path to sys.path
        pythonInterpreter.exec("sys.path.append('" + dependenciesPath + "')");
    }
    
    /**
     * Gets parameters needed for file mode.
     * Gets the name and directory of the processor from the processor script file path of the configuration
     * @param context Logisland context
     * @throws Exception
     */
    private void getFileModeParams(ProcessContext context) throws Exception
    {
        String configPythonProcessorScriptPath = context.getPropertyValue(SCRIPT_PATH).asString();
        
        if (configPythonProcessorScriptPath == null)
        {
            // If this is empty then init and process code must have been used...
            return;
        }
        
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
        this.scriptPath = processorFile.getAbsolutePath();
        
        /**
         * Get the directory in which resides the python processor script file
         */
        
        File parentFile = processorFile.getParentFile();
        if (parentFile == null)
        {
            parentFile = new File(".");
        }
        this.scriptDirectory = parentFile.getAbsolutePath();
        
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
        
        this.processorName =  processorFileName.substring(0, processorFileName.length()-3);
        
        useScriptFile = true;
    }
    
    /**
     * Gets the directory for dependencies of the processor script 
     * @param context Logisland context
     * @throws Exception
     */
    private void getDependenciesPathParam(ProcessContext context) throws Exception
    {
        String configPythonProcessorDependenciesPath = context.getPropertyValue(DEPENDENCIES_PATH).asString();
        
        /**
         * If a dependencies path is provided, just use it.
         * If no dependencies path is not given and if a script path is used, use the default dependencies path which is
         * e directory named DEFAULT_DEPENDENCIES_DIRNAME at the same location of the python processor script and us it
         * if it exists.
         */
        if (configPythonProcessorDependenciesPath == null)
        {
            if (useScriptFile)
            {
                this.dependenciesPath = this.scriptDirectory + File.separator + DEFAULT_DEPENDENCIES_DIRNAME;
                logger.debug("No python processor dependencies path specified, using default one: " + dependenciesPath);
                
                File dependenciesDir = null;
                try {
                    dependenciesDir = new File(dependenciesPath);
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
                        logger.info(dependenciesPath + " is a file, not a directory");
                    }
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
            
            this.dependenciesPath = dependenciesDir.getAbsolutePath();
            hasDependencies = true;
        }
    }
    
    /**
     * Gets the directory containing the logisland python dependencies (shipped with logisland)
     * @param context Logisland context
     * @throws Exception
     */
    private void getLogislandDependenciesPathParam(ProcessContext context) throws Exception
    {
        // TODO: can this be found with correct default value pointing to resources in jar file? 
        logislandDependenciesPath = context.getPropertyValue(LOGISLAND_DEPENDENCIES_PATH).asString();
        logger.debug("Using python logisland dependencies path: " + logislandDependenciesPath);
    }
    
    /**
     * Gets parameters needed for inline mode: imports, init and process methods code
     * @param context Logisland context
     * @throws Exception
     */
    private void getInlineModeParams(ProcessContext context) throws Exception
    {
        scriptCodeImports = context.getPropertyValue(SCRIPT_CODE_IMPORTS).asString();
        scriptCodeInit = context.getPropertyValue(SCRIPT_CODE_INIT).asString();
        scriptCodeProcess = context.getPropertyValue(SCRIPT_CODE_PROCESS).asString();        
    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records)
    {
        if(pythonInterpreter == null)
            init(context);

        Collection<Record> outputRecords = null;
        
        if (useScriptFile)
        {
            // File mode
             
            /**
             * Call process method of python processor script with the records we received
             */
            pythonInterpreter.set("context", context);
            pythonInterpreter.set("records", records);
            pythonInterpreter.exec("outputRecords = pyProcessor.process(context, records)");
            outputRecords = pythonInterpreter.get("outputRecords", Collection.class);
        } else
        {
            // Inline mode

            /**
             * Call process method of python processor script with the records we received
             */
            pythonInterpreter.set("context", context);
            pythonInterpreter.set("records", records);
            pythonInterpreter.exec("outputRecords = process(context, records)");
            outputRecords = pythonInterpreter.get("outputRecords", Collection.class);
        }
        logger.debug("Processed records ");

        return outputRecords;
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
            if (useScriptFile)
            {
                /**
                 * pyProcessor.onPropertyModified(context, oldValue, newValue)
                 */
                pythonInterpreter.set("descriptor", descriptor);
                pythonInterpreter.set("oldValue", oldValue);
                pythonInterpreter.set("newValue", newValue);
                pythonInterpreter.exec("pyProcessor.onPropertyModified(context, oldValue, newValue)");
            } else
            {
                // TODO
                logger.warn("Dynamic update of configuration properties is not supported when not using python"
                        + " script file");
            }
        }
    }
    
    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        final List<ValidationResult> validationResults = new ArrayList<>(super.customValidate(context));

        logger.debug("customValidate");

        /**
         * Either script file mode or inline mode, not both
         */
        if (context.getPropertyValue(SCRIPT_CODE_PROCESS).isSet())
        {
            // inline mode, 
            if (context.getPropertyValue(SCRIPT_PATH).isSet()) {
                // attempt to use both modes -> error
                validationResults.add(
                        new ValidationResult.Builder()
                            .explanation("You must declare " + SCRIPT_CODE_PROCESS.getName() + " or " + SCRIPT_PATH.getName() + " but not both")
                            .valid(false)
                            .build());
            }
        } else
        {
            // Not inline mode, check this will be file mode
            if (!context.getPropertyValue(SCRIPT_PATH).isSet()) {
                // no python code provided
                validationResults.add(
                        new ValidationResult.Builder()
                            .explanation("No python code declared. Use " + SCRIPT_CODE_PROCESS.getName() + " or " + SCRIPT_PATH.getName() + " to define some python code")
                            .valid(false)
                            .build());
            } else
            {
                // Be sure that other inline mode config properties are not set

                if (context.getPropertyValue(SCRIPT_CODE_INIT).isSet()) {
                    // init defined but not process!
                    validationResults.add(
                            new ValidationResult.Builder()
                                .explanation("Inline mode requires at least " + SCRIPT_CODE_PROCESS.getName() + " to be defined")
                                .valid(false)
                                .build());
                }
                
                if (context.getPropertyValue(SCRIPT_CODE_IMPORTS).isSet()) {
                    // init defined but not process!
                    validationResults.add(
                            new ValidationResult.Builder()
                                .explanation("Inline mode requires at least " + SCRIPT_CODE_IMPORTS.getName() + " to be defined")
                                .valid(false)
                                .build());
                }
            }
        }
      
        return validationResults;
    }
}
