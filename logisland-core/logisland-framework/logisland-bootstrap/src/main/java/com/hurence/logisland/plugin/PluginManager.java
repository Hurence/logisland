/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.plugin;

import com.hurence.logisland.BannerLoader;
import com.hurence.logisland.classloading.ManifestAttributes;
import com.hurence.logisland.classloading.ModuleInfo;
import com.hurence.logisland.classloading.PluginClassLoader;
import com.hurence.logisland.classloading.PluginLoader;
import com.hurence.logisland.packaging.LogislandPluginLayoutFactory;
import com.hurence.logisland.packaging.LogislandRepackager;
import com.hurence.logisland.util.Tuple;
import org.apache.commons.cli.*;
import org.apache.ivy.Ivy;
import org.apache.ivy.core.module.descriptor.DefaultArtifact;
import org.apache.ivy.core.module.id.ModuleId;
import org.apache.ivy.core.module.id.ModuleRevisionId;
import org.apache.ivy.core.report.ArtifactDownloadReport;
import org.apache.ivy.core.report.ResolveReport;
import org.apache.ivy.core.resolve.DownloadOptions;
import org.apache.ivy.core.resolve.ResolveOptions;
import org.apache.ivy.core.settings.IvySettings;
import org.springframework.boot.loader.tools.Library;
import org.springframework.boot.loader.tools.LibraryScope;
import org.springframework.boot.loader.tools.Repackager;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.stream.Collectors;

/**
 * Logisland plugin manager
 *
 * @author amarziali
 */
public class PluginManager {

    private static Set<ArtifactDownloadReport> downloadArtifacts(Ivy ivy, ModuleRevisionId moduleRevisionId, String[] confs) throws Exception {
        ResolveOptions resolveOptions = new ResolveOptions();
        resolveOptions.setDownload(true);
        resolveOptions.setTransitive(true);
        resolveOptions.setOutputReport(false);
        resolveOptions.setConfs(confs);
        resolveOptions.setLog(null);
        resolveOptions.setValidate(false);
        resolveOptions.setCheckIfChanged(true);


        ResolveReport report = (ivy.resolve(moduleRevisionId, resolveOptions, true));
        Set<ArtifactDownloadReport> reports = new LinkedHashSet<>();
        reports.addAll(Arrays.asList(report.getAllArtifactsReports()));

        resolveOptions.setTransitive(false);
        for (ArtifactDownloadReport artifactDownloadReport : report.getFailedArtifactsReports()) {
            reports.add(ivy.getResolveEngine().download(
                    new DefaultArtifact(artifactDownloadReport.getArtifact().getModuleRevisionId(),
                            artifactDownloadReport.getArtifact().getPublicationDate(),
                            artifactDownloadReport.getArtifact().getName(),
                            "jar",
                            "jar"), new DownloadOptions()));
        }
        return reports;
    }


    private static final List<String> excludedArtifactsId = Arrays.asList("logisland-utils", "logisland-api", "commons-logging",
            "logisland-scripting-mvel", "logisland-scripting-base", "logisland-hadoop-utils");
    private static final List<String> excludeGroupIds = Arrays.asList("org.slf4j", "ch.qos.logback", "log4j", "org.apache.logging.log4j");


    public static void main(String... args) throws Exception {
        System.out.println(BannerLoader.loadBanner());

        String logislandHome = new File(new File(PluginManager.class.getProtectionDomain().getCodeSource().getLocation().getPath()).getParent()).getParent();
        System.out.println("Using Logisland home: " + logislandHome);
        Options options = new Options();
        OptionGroup mainGroup = new OptionGroup()
                .addOption(OptionBuilder
                        .withDescription("Install a component. It can be either a logisland plugin or a kafka connect module.")
                        .withArgName("artifact")
                        .hasArgs(1)
                        .withLongOpt("install")
                        .create("i"))
                .addOption(OptionBuilder
                        .withDescription("Removes a component. It can be either a logisland plugin or a kafka connect module.")
                        .withArgName("artifact")
                        .hasArgs(1)
                        .withLongOpt("remove")
                        .create("r"))
                .addOption(OptionBuilder
                        .withDescription("List installed components.")
                        .withLongOpt("list")
                        .create("l"));

        mainGroup.setRequired(true);
        options.addOptionGroup(mainGroup);
        options.addOption(OptionBuilder
                .withDescription("Print this help.")
                .withLongOpt("help")
                .create("h"));

        try {
            CommandLine commandLine = new PosixParser().parse(options, args);
            System.out.println(commandLine.getArgList());
            if (commandLine.hasOption("l")) {
                listPlugins();
            } else if (commandLine.hasOption("i")) {
                installPlugin(commandLine.getOptionValue("i"), logislandHome);

            } else if (commandLine.hasOption("r")) {
                removePlugin(commandLine.getOptionValue("r"));
            } else {
                printUsage(options);
            }

        } catch (ParseException e) {
            if (!options.hasOption("h")) {
                System.err.println(e.getMessage());
                System.out.println();
            }
            printUsage(options);

        }
    }

    private static void printUsage(Options options) {
        System.out.println();
        new HelpFormatter().printHelp(180,
                "components.sh", "\n", options, "\n",
                true);

    }

    private static Map<ModuleInfo, List<String>> findPluginMeta() {
        return PluginLoader.getRegistry().entrySet().stream()
                .map(e -> new Tuple<>(((PluginClassLoader) e.getValue()).getModuleInfo(), e.getKey()))
                .collect(Collectors.groupingBy(t -> t.getKey().getArtifact()))
                .entrySet().stream().collect(Collectors.toMap(
                        e -> e.getValue().stream().findFirst().get().getKey(),
                        e -> e.getValue().stream().map(Tuple::getValue).sorted().collect(Collectors.toList())));

    }

    private static void listPlugins() {
        Map<ModuleInfo, List<String>> moduleMapping = findPluginMeta();

        System.out.println();
        System.out.println("Listing details for " + moduleMapping.size() + " installed modules.");
        System.out.println("==============================================================");
        System.out.println();
        moduleMapping.forEach((c, l) -> {
            System.out.println("Artifact: " + c.getArtifact());
            System.out.println("Name: " + c.getName());
            System.out.println("Version: " + c.getVersion());
            System.out.println("Location: " + c.getSourceArchive());
            System.out.println("Components provided:");
            l.forEach(ll -> System.out.println("\t" + ll));
            System.out.println("\n-----------------------------------------------------------\n");
        });
    }


    private static void removePlugin(String artifact) {
        Optional<ModuleInfo> moduleInfo = findPluginMeta().entrySet().stream().filter(e -> artifact.equals(e.getKey().getArtifact()))
                .map(Map.Entry::getKey).findFirst();
        if (moduleInfo.isPresent()) {
            String filename = moduleInfo.get().getSourceArchive();
            System.out.println("Removing component jar: " + filename);
            if (!new File(filename).delete()) {
                System.err.println("Unable to delete file " + artifact);
                System.exit(-1);
            }

        } else {
            System.err.println("Found no installed component matching artifact " + artifact);
            System.exit(-1);
        }
    }

    private static void installPlugin(String artifact, String logislandHome) {
        Optional<ModuleInfo> moduleInfo = findPluginMeta().entrySet().stream().filter(e -> artifact.equals(e.getKey().getArtifact()))
                .map(Map.Entry::getKey).findFirst();
        if (moduleInfo.isPresent()) {
            System.err.println("A component already matches the artifact " + artifact +
                    ". Please remove it first.");
            System.exit(-1);
        }

        try {


            IvySettings settings = new IvySettings();
            settings.load(new File(logislandHome, "conf/ivy.xml"));


            Ivy ivy = Ivy.newInstance(settings);
            ivy.bind();

            System.out.println("\nDownloading dependencies. Please hold on...\n");

            String parts[] = Arrays.stream(artifact.split(":")).map(String::trim).toArray(a -> new String[a]);
            if (parts.length != 3) {
                throw new IllegalArgumentException("Unrecognized artifact format. It should be groupId:artifactId:version");
            }
            ModuleRevisionId revisionId = new ModuleRevisionId(new ModuleId(parts[0], parts[1]), parts[2]);
            Set<ArtifactDownloadReport> toBePackaged = downloadArtifacts(ivy, revisionId, new String[]{"default", "compile", "runtime"});

            ArtifactDownloadReport artifactJar = toBePackaged.stream().filter(a -> a.getArtifact().getModuleRevisionId().equals(revisionId)).findFirst()
                    .orElseThrow(() -> new IllegalStateException("Unable to find artifact " + artifact +
                            ". Please check the name is correct and the repositories on ivy.xml are correctly configured"));

            Manifest manifest = new JarFile(artifactJar.getLocalFile()).getManifest();
            File libDir = new File(logislandHome, "lib");


            if (manifest.getMainAttributes().containsKey(ManifestAttributes.MODULE_ARTIFACT)) {
                org.apache.commons.io.FileUtils.copyFileToDirectory(artifactJar.getLocalFile(), libDir);
                //we have a logisland plugin. Just copy it
                System.out.println(String.format("Found logisland plugin %s version %s\n" +
                                "It will provide:",
                        manifest.getMainAttributes().getValue(ManifestAttributes.MODULE_NAME),
                        manifest.getMainAttributes().getValue(ManifestAttributes.MODULE_VERSION)));
                Arrays.stream(manifest.getMainAttributes().getValue(ManifestAttributes.MODULE_EXPORTS).split(","))
                        .map(String::trim).forEach(s -> System.out.println("\t" + s));


            } else {
                System.out.println("Repackaging artifact and its dependencies");
                Set<ArtifactDownloadReport> environment = downloadArtifacts(ivy, revisionId, new String[]{"provided"});
                Set<ArtifactDownloadReport> excluded = toBePackaged.stream()
                        .filter(adr -> excludeGroupIds.stream().anyMatch(s -> s.matches(adr.getArtifact().getModuleRevisionId().getOrganisation())) ||
                                excludedArtifactsId.stream().anyMatch(s -> s.matches(adr.getArtifact().getModuleRevisionId().getName())))
                        .collect(Collectors.toSet());

                toBePackaged.removeAll(excluded);
                environment.addAll(excluded);

                Repackager rep = new Repackager(artifactJar.getLocalFile(), new LogislandPluginLayoutFactory());
                rep.setMainClass("");
                File destFile = new File(libDir, "logisland-component-" + artifactJar.getLocalFile().getName());
                rep.repackage(destFile, callback ->
                        toBePackaged.stream()
                                .filter(adr -> adr.getLocalFile() != null)
                                .filter(adr -> !adr.getArtifact().getModuleRevisionId().equals(revisionId))
                                .map(adr -> new Library(adr.getLocalFile(), LibraryScope.COMPILE))
                                .forEach(library -> {
                                    try {
                                        callback.library(library);
                                    } catch (IOException e) {
                                        throw new UncheckedIOException(e);
                                    }
                                })
                );
                Thread.currentThread().setContextClassLoader(new URLClassLoader(environment.stream()
                        .filter(adr -> adr.getLocalFile() != null)
                        .map(adr -> {
                            try {
                                return adr.getLocalFile().toURI().toURL();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }).toArray(a -> new URL[a]),
                        Thread.currentThread().getContextClassLoader()));
                //now clean up package and write the manifest
                String newArtifact = "com.hurence.logisland.repackaged:" + parts[1] + ":" + parts[2];
                LogislandRepackager.execute(destFile.getAbsolutePath(), "BOOT-INF/lib-provided",
                        parts[2], newArtifact,
                        "Logisland Component for " + artifact,
                        "Logisland Component for " + artifact,
                        new String[]{"org.apache.kafka.*"},
                        new String[0],
                        "org.apache.kafka.connect.connector.Connector");
            }
            System.out.println("Install done!");
        } catch (Exception e) {
            System.err.println("Unable to install artifact " + artifact);
            e.printStackTrace();
            System.exit(-1);
        }

    }
}
