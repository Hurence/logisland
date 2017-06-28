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
package com.hurence.logisland.documentation;

import com.hurence.logisland.component.ConfigurableComponent;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.controller.ControllerService;
import com.hurence.logisland.documentation.html.HtmlDocumentationWriter;
import com.hurence.logisland.documentation.html.HtmlProcessorDocumentationWriter;
import com.hurence.logisland.documentation.init.EngineInitializer;
import com.hurence.logisland.documentation.init.ProcessorInitializer;
import com.hurence.logisland.documentation.init.RecordStreamInitializer;
import com.hurence.logisland.documentation.json.JsonDocumentationWriter;
import com.hurence.logisland.documentation.rst.RstDocumentationWriter;
import com.hurence.logisland.documentation.util.ClassFinder;
import com.hurence.logisland.documentation.util.Visitor;
import com.hurence.logisland.engine.ProcessingEngine;
import com.hurence.logisland.processor.Processor;
import com.hurence.logisland.processor.SplitText;
import com.hurence.logisland.stream.RecordStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

/**
 * Uses the ExtensionManager to get a list of Processor, ControllerService, and
 * Reporting Task classes that were loaded and generate documentation for them.
 */
public class DocGenerator {

    private static final Logger logger = LoggerFactory.getLogger(DocGenerator.class);
    public static final String OUTPUT_FILE = "components";

    /**
     * Generates documentation into the work/docs dir specified

     public static void generate(final File docsDirectory, final String writerType) {
    @SuppressWarnings("rawtypes") final Set<Class> extensionClasses = new HashSet<>();

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


        Processor p = new SplitText();

        Map<String, Class> extensionClasses = new TreeMap<>();
        ClassFinder.findClasses(new Visitor<String>() {
            @Override
            public boolean visit(String clazz) {
                if (clazz.contains("logisland") && !clazz.contains("Mock") && !clazz.contains("shade")) {
                    try {
                        Class c = Class.forName(clazz);


                        extensionClasses.put(c.getSimpleName(), c);
                    } catch (Throwable e) {
                        logger.error("Unable to load class " + clazz + " : " + e.getMessage());
                    }
                }

                return true; // return false if you don't want to see any more classes
            }
        });

        docsDirectory.mkdirs();


        // write headers for single rst file
        if (writerType.equals("rst")) {
            final File baseDocumenationFile = new File(docsDirectory, OUTPUT_FILE + "." + writerType);
            if (baseDocumenationFile.exists())
                baseDocumenationFile.delete();

            try (final PrintWriter writer = new PrintWriter(new FileOutputStream(baseDocumenationFile, true))) {
                writer.println("Components");
                writer.println("==========");
                writer.println("You'll find here the list of all usable Processors, Engines, Services and other components " +
                        "that can be usable out of the box in your analytics streams");
                writer.println();
            } catch (FileNotFoundException e) {
                logger.warn(e.getMessage());
            }
        } else if (writerType.equals("json")) {
            final File baseDocumenationFile = new File(docsDirectory, OUTPUT_FILE + "." + writerType);
            if (baseDocumenationFile.exists())
                baseDocumenationFile.delete();

            try (final PrintWriter writer = new PrintWriter(new FileOutputStream(baseDocumenationFile, true))) {
                writer.println("[");
            } catch (FileNotFoundException e) {
                logger.warn(e.getMessage());
            }
        }

        Class[] sortedExtensionsClasses = new Class[extensionClasses.size()];
        extensionClasses.values().toArray(sortedExtensionsClasses);

        Arrays.sort(sortedExtensionsClasses, new Comparator<Class>() {
            @Override
            public int compare(Class s1, Class s2) {
                // the +1 is to avoid including the '.' in the extension and to avoid exceptions
                // EDIT:
                // We first need to make sure that either both files or neither file
                // has an extension (otherwise we'll end up comparing the extension of one
                // to the start of the other, or else throwing an exception)
                final int s1Dot = s1.getName().lastIndexOf('.');
                final int s2Dot = s2.getName().lastIndexOf('.');
                if ((s1Dot == -1) == (s2Dot == -1)) { // both or neither
                    String s1Name = s1.getName().substring(s1Dot + 1);
                    String s2Name = s2.getName().substring(s2Dot + 1);
                    return s1Name.compareTo(s2Name);
                } else if (s1Dot == -1) { // only s2 has an extension, so s1 goes first
                    return -1;
                } else { // only s1 has an extension, so s1 goes second
                    return 1;
                }
            }
        });


        logger.info("Generating " +writerType + " documentation for " + Arrays.stream(sortedExtensionsClasses)
                .filter(ConfigurableComponent.class::isAssignableFrom).count() + " components in: "
                + docsDirectory);

        Arrays.stream(sortedExtensionsClasses)
                .filter(ConfigurableComponent.class::isAssignableFrom)
                .forEach(extensionClass -> {
            final Class componentClass = extensionClass.asSubclass(ConfigurableComponent.class);
            try {
                document(docsDirectory, componentClass, writerType);

            } catch (Exception e) {
                logger.error(e.toString());
            }

        });


        if (writerType.equals("json")) {
            final File baseDocumenationFile = new File(docsDirectory, OUTPUT_FILE + "." + writerType);
            try (final PrintWriter writer = new PrintWriter(new FileOutputStream(baseDocumenationFile, true))) {
                writer.println("]");
            } catch (FileNotFoundException e) {
                logger.warn(e.getMessage());
            }
        }
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

        logger.info("Documenting: " + componentClass);
        final ConfigurableComponent component = componentClass.newInstance();
        final ConfigurableComponentInitializer initializer = getComponentInitializer(componentClass);
        initializer.initialize(component);

        final DocumentationWriter writer = getDocumentWriter(componentClass, writerType);

        final File baseDocumenationFile = new File(docsDir, OUTPUT_FILE + "." + writerType);

        try (final OutputStream output = new BufferedOutputStream(new FileOutputStream(baseDocumenationFile, true))) {
            writer.write(component, output);
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            initializer.teardown(component);
            if (writerType.equals("json")) {
                try (final PrintWriter commaWriter = new PrintWriter(new FileOutputStream(baseDocumenationFile, true))) {
                    commaWriter.println(",");
                }
            }
        }


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


        if (Processor.class.isAssignableFrom(componentClass) ||
                RecordStream.class.isAssignableFrom(componentClass) ||
                ControllerService.class.isAssignableFrom(componentClass) ||
                ProcessingEngine.class.isAssignableFrom(componentClass)) {
            switch (writerType) {
                case "html":
                    return new HtmlProcessorDocumentationWriter();
                case "rst":
                    return new RstDocumentationWriter();
                case "json":
                    return new JsonDocumentationWriter();
                default:
                    return null;
            }
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
        } else if (RecordStream.class.isAssignableFrom(componentClass)) {
            return new RecordStreamInitializer();
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
        DocGenerator.generate(new File("."), "rst");
        DocGenerator.generate(new File("../logisland-framework/logisland-agent/src/main/resources"), "json");

        try {
            FileUtils.copyDirectory(
                    new File("."),
                    new File("../logisland-framework/logisland-resources/src/main/resources/docs"),
                    new WildcardFileFilter("*.rst"));

            FileUtils.copyDirectory(
                    new File("tutorials"),
                    new File("../logisland-framework/logisland-resources/src/main/resources/docs/tutorials"),
                    new WildcardFileFilter("*.rst"));

            FileUtils.copyDirectory(
                    new File("_static"),
                    new File("../logisland-framework/logisland-resources/src/main/resources/docs/_static"));
        } catch (IOException e) {
            logger.error(e.toString());
        }
    }
}
