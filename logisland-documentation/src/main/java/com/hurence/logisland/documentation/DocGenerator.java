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

import com.hurence.logisland.classloading.PluginLoader;
import com.hurence.logisland.classloading.PluginProxy;
import com.hurence.logisland.component.ConfigurableComponent;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.controller.ControllerService;
import com.hurence.logisland.documentation.html.HtmlDocumentationWriter;
import com.hurence.logisland.documentation.html.HtmlProcessorDocumentationWriter;
import com.hurence.logisland.documentation.json.JsonDocumentationWriter;
import com.hurence.logisland.documentation.rst.RstDocumentationWriter;
import com.hurence.logisland.documentation.util.ClassFinder;
import com.hurence.logisland.engine.ProcessingEngine;
import com.hurence.logisland.processor.Processor;
import com.hurence.logisland.stream.RecordStream;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Uses the ExtensionManager to get a list of Processor, ControllerService, and
 * Connectors classes that were loaded and generate documentation for them.
 */
public class DocGenerator {

    private static final Logger logger = LoggerFactory.getLogger(DocGenerator.class);
    public static final String OUTPUT_FILE = "components";

    /**
     * Generates documentation into the work/docs dir specified from a specified set of class
     */
    public static void generate(final File docsDirectory, final String writerType) {


        Map<String, Class> extensionClasses = new TreeMap<>();


        PluginLoader.getRegistry().forEach((className, classLoader) -> {
            try {
                extensionClasses.put(className, classLoader.loadClass(className));
            } catch (Exception e) {
                logger.error("Unable to load class " + className, e);
                throw new RuntimeException(e);//so we know there is something wrong with doc generation
            }
        });

        ClassFinder.findClasses(clazz -> {
            if (!clazz.startsWith("BOOT-INF") && clazz.contains("logisland") && !clazz.contains("Mock") && !clazz.contains("shade")) {
                try {
                    Class c = Class.forName(clazz);
                    if (c.isAssignableFrom(ConfigurableComponent.class) && !Modifier.isAbstract(c.getModifiers())) {
                        extensionClasses.put(c.getSimpleName(), c);
                    }
                } catch (Throwable e) {
                    logger.error("Unable to load class " + clazz + " : " + e.getMessage());
                    throw new RuntimeException(e);//so we know there is something wrong with doc generation
                }
            }

            return true; // return false if you don't want to see any more classes
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
                        "that can be usable out of the box in your logisland jobs");
                writer.println();
            } catch (FileNotFoundException e) {
                logger.warn(e.getMessage());
                throw new RuntimeException(e);//so we know there is something wrong with doc generation
            }
        } else if (writerType.equals("json")) {
            final File baseDocumenationFile = new File(docsDirectory, OUTPUT_FILE + "." + writerType);
            if (baseDocumenationFile.exists())
                baseDocumenationFile.delete();

            try (final PrintWriter writer = new PrintWriter(new FileOutputStream(baseDocumenationFile, true))) {
                writer.println("[");
            } catch (FileNotFoundException e) {
                logger.warn(e.getMessage());
                throw new RuntimeException(e);//so we know there is something wrong with doc generation
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

        logger.info("Generating {} documentation for {} components in: {}",
                writerType,
                Arrays.stream(sortedExtensionsClasses).count(),
                docsDirectory);

        Arrays.stream(sortedExtensionsClasses)
                .forEach(extensionClass -> {
                    final Class componentClass = extensionClass.asSubclass(ConfigurableComponent.class);
                    try {
                        document(docsDirectory, componentClass, writerType);
                    } catch (Exception e) {
                        logger.error("Unexpected error for " + extensionClass, e);
                        throw new RuntimeException(e);//so we know there is something wrong with doc generation
                    }
                });


        if (writerType.equals("json")) {
            final File baseDocumenationFile = new File(docsDirectory, OUTPUT_FILE + "." + writerType);
            try (final PrintWriter writer = new PrintWriter(new FileOutputStream(baseDocumenationFile, true))) {
                writer.println("]");
            } catch (FileNotFoundException e) {
                logger.warn(e.getMessage());
                throw new RuntimeException(e);//so we know there is something wrong with doc generation
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
            throws InstantiationException, IllegalAccessException, IOException, InitializationException, ClassNotFoundException {

        logger.info("Documenting: " + componentClass);
        final ConfigurableComponent component = PluginProxy.unwrap(PluginLoader.loadPlugin(componentClass.getCanonicalName()));

        final DocumentationWriter writer = getDocumentWriter(componentClass, writerType);

        final File baseDocumenationFile = new File(docsDir, OUTPUT_FILE + "." + writerType);

        try (final OutputStream output = new BufferedOutputStream(new FileOutputStream(baseDocumenationFile, true))) {
            writer.write(component, output);
        } catch (Exception e) {
            logger.error("Error occurred documenting " + componentClass, e);
            throw e;
        } finally {
            if (writerType.equals("json")) {
                try (final PrintWriter commaWriter = new PrintWriter(new FileOutputStream(baseDocumenationFile, true))) {
                    commaWriter.println(",");
                }
            }
        }


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
                return name.equalsIgnoreCase(RstDocumentationWriter.ADDITIONAL_DETAILS_RST);
            }

        }).length > 0;
    }

    /**
     * Returns the DocumentationWriter for the type of component. Currently
     * Processor, ControllerService are supported.
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
        }
        throw new IllegalArgumentException("class '" + componentClass + "' is not supported");
    }

    public static void main(String[] args) {
        Options options = new Options();
        options.addOption(OptionBuilder
                .withDescription("Print this help.")
                .withLongOpt("help")
                .create("h"));
        options.addOption(OptionBuilder
                .withDescription("dir to generate documentation")
                .withLongOpt("doc-dir")
                .hasArg()
                .create("d"));

        String dir = ".";
        try {
            CommandLine commandLine = new PosixParser().parse(options, args);
            System.out.println(commandLine.getArgList());
            if (commandLine.hasOption("h")) {
                printUsage(options);
            }
            if (commandLine.hasOption("d")) {
                dir = commandLine.getOptionValue("d");
            }
        } catch (ParseException e) {
            if (!options.hasOption("h")) {
                System.err.println(e.getMessage());
                System.out.println();
            }
            printUsage(options);

        }

        File rootDocDir = new File(dir);
        DocGenerator.generate(rootDocDir, "rst");

        try {
            FileUtils.copyDirectory(
                    rootDocDir,
                    new File("../logisland-core/logisland-framework/logisland-resources/src/main/resources/docs"),
                    new WildcardFileFilter("*.rst"));

            FileUtils.copyDirectory(
                    new File(rootDocDir, "tutorials"),
                    new File("../logisland-core/logisland-framework/logisland-resources/src/main/resources/docs/tutorials"),
                    new WildcardFileFilter("*.rst"));

            FileUtils.copyDirectory(
                    new File(rootDocDir, "_static"),
                    new File("../logisland-core/logisland-framework/logisland-resources/src/main/resources/docs/_static"));
        } catch (IOException e) {
            logger.error("I/O error", e);
        }
    }

    private static void printUsage(Options options) {
        System.out.println();
        new HelpFormatter().printHelp(180,
                "components.sh", "\n", options, "\n",
                true);
        System.exit(0);

    }
}
