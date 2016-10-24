/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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
package com.hurence.logisland.documentation;

import com.hurence.logisland.component.ConfigurableComponent;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.documentation.html.HtmlDocumentationWriter;
import com.hurence.logisland.documentation.html.HtmlProcessorDocumentationWriter;
import com.hurence.logisland.documentation.init.EngineInitializer;
import com.hurence.logisland.documentation.init.ProcessorInitializer;
import com.hurence.logisland.documentation.rst.RstDocumentationWriter;
import com.hurence.logisland.documentation.rst.RstPrintWriter;
import com.hurence.logisland.documentation.util.ClassFinder;
import com.hurence.logisland.documentation.util.Visitor;
import com.hurence.logisland.engine.ProcessingEngine;
import com.hurence.logisland.processor.Processor;
import com.sun.tools.javac.util.Assert;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/**
 * Uses the ExtensionManager to get a list of Processor, ControllerService, and
 * Reporting Task classes that were loaded and generate documentation for them.
 */
public class DocGenerator {

    private static final Logger logger = LoggerFactory.getLogger(DocGenerator.class);

    /**
     * Generates documentation into the work/docs dir specified

    public static void generate(final File docsDirectory, final String writerType) {
        @SuppressWarnings("rawtypes")
        final Set<Class> extensionClasses = new HashSet<>();

        // TODO add extensionmanager here
        ///extensionClasses.addAll(ExtensionManager.getExtensions(Processor.class));


        logger.debug("Generating documentation for: " + extensionClasses.size() + " components in: "
                + docsDirectory);

        for (final Class<?> extensionClass : extensionClasses) {
            if (ConfigurableComponent.class.isAssignableFrom(extensionClass)) {
                final Class<? extends ConfigurableComponent> componentClass = extensionClass.asSubclass(ConfigurableComponent.class);
                try {
                    logger.debug("Documenting: " + componentClass);
                    document(docsDirectory, componentClass, writerType);
                } catch (Exception e) {
                    logger.warn("Unable to document: " + componentClass, e);
                }
            }
        }
    }
 */
    /**
     * Generates documentation into the work/docs dir specified from a specified set of class
     */
    public static void generate(final File docsDirectory, final String writerType) {


        Map<String,Class> extensionClasses = new TreeMap<>();
        ClassFinder.findClasses(new Visitor<String>() {
            @Override
            public boolean visit(String clazz) {
                if(clazz.contains("logisland")){
                    try {
                        Class c = Class.forName(clazz);
                        extensionClasses.put(c.getSimpleName(), c);
                    } catch (NoClassDefFoundError | ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }

                return true; // return false if you don't want to see any more classes
            }
        });

        docsDirectory.mkdirs();
        logger.debug("Generating documentation for: " + extensionClasses.size() + " components in: "
                + docsDirectory);


        // write headers for single rst file
        if(writerType.equals("rst")){
            final File baseDocumenationFile = new File(docsDirectory, "plugins." + writerType);
            if (baseDocumenationFile.exists())
                baseDocumenationFile.delete();

            try (final PrintWriter writer = new PrintWriter(new FileOutputStream(baseDocumenationFile, true))) {
                writer.println("Extension plugins");
                writer.println("=================");
                writer.println("You'll find here the list of all usable Processors, Engines and other components " +
                        "taht can be usable out of the box in your analytics streams");
                writer.println();
            } catch (FileNotFoundException e) {
                logger.warn(e.getMessage());
            }
        }



        extensionClasses.values().stream()
                .filter(ConfigurableComponent.class::isAssignableFrom)
                .forEach(extensionClass -> {
                    final Class componentClass = extensionClass.asSubclass(ConfigurableComponent.class);
                    try {
                        logger.debug("Documenting: " + componentClass);
                        document(docsDirectory, componentClass, writerType);
                    } catch (Exception e) {
                        logger.warn("Unable to document: " + componentClass, e);
                    }
                });
    }

    /**
     * Generates the documentation for a particular configurable component. Will
     * check to see if an "additionalDetails.html" file exists and will link
     * that from the generated documentation.
     *
     * @param docsDir        the work\docs\components dir to stick component
     *                       documentation in
     * @param componentClass the class to document
     * @throws InstantiationException ie
     * @throws IllegalAccessException iae
     * @throws IOException            ioe
     */
    private static void document(final File docsDir, final Class<? extends ConfigurableComponent> componentClass, final String writerType)
            throws InstantiationException, IllegalAccessException, IOException, InitializationException {

        final ConfigurableComponent component = componentClass.newInstance();
        final ConfigurableComponentInitializer initializer = getComponentInitializer(componentClass);
        initializer.initialize(component);

        final DocumentationWriter writer = getDocumentWriter(componentClass, writerType);

        //  final File directory = new File(docsDir, componentClass.getCanonicalName());
        // directory.mkdirs();

        final File baseDocumenationFile = new File(docsDir, "plugins" + "." + writerType);
        if (baseDocumenationFile.exists()) {
            logger.warn(baseDocumenationFile + " already exists, overwriting!");
        }

        try (final OutputStream output = new BufferedOutputStream(new FileOutputStream(baseDocumenationFile, true))) {
            writer.write(component, output);
        }

        initializer.teardown(component);
    }

    /**
     * Returns the DocumentationWriter for the type of component. Currently
     * Processor, ControllerService, and ReportingTask are supported.
     *
     * @param componentClass the class that requires a DocumentationWriter
     * @return a DocumentationWriter capable of generating documentation for
     * that specific type of class
     */
    private static DocumentationWriter getDocumentWriter(final Class<? extends ConfigurableComponent> componentClass,
                                                         final String writerType) {

        if (writerType.equals("html")) {
            if (Processor.class.isAssignableFrom(componentClass) || ProcessingEngine.class.isAssignableFrom(componentClass)) {
                return new HtmlProcessorDocumentationWriter();
            }

            return null;
        } else if (writerType.equals("rst")) {
            if (Processor.class.isAssignableFrom(componentClass) || ProcessingEngine.class.isAssignableFrom(componentClass)) {
                return new RstDocumentationWriter();
            }

            return null;
        } else {
            return null;
        }
    }

    /**
     * Returns a ConfigurableComponentInitializer for the type of component.
     * Currently Processor, ControllerService and ReportingTask are supported.
     *
     * @param componentClass the class that requires a
     *                       ConfigurableComponentInitializer
     * @return a ConfigurableComponentInitializer capable of initializing that
     * specific type of class
     */
    private static ConfigurableComponentInitializer getComponentInitializer(
            final Class<? extends ConfigurableComponent> componentClass) {
        if (Processor.class.isAssignableFrom(componentClass)) {
            return new ProcessorInitializer();
        } else if (ProcessingEngine.class.isAssignableFrom(componentClass)) {
            return new EngineInitializer();
        }

        return null;
    }

    /**
     * Checks to see if a directory to write to has an additionalDetails.html in
     * it already.
     *
     * @param directory to check
     * @return true if additionalDetails.html exists, false otherwise.
     */
    private static boolean hasAdditionalInfo(File directory) {
        return directory.list(new FilenameFilter() {

            @Override
            public boolean accept(File dir, String name) {
                return name.equalsIgnoreCase(HtmlDocumentationWriter.ADDITIONAL_DETAILS_HTML);
            }

        }).length > 0;
    }

    public static void main(String[] args) {





        DocGenerator.generate(new File("logisland-docs"), "rst");



    }
}
