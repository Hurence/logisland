/*
 *  * Copyright (C) 2018 Hurence (support@hurence.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.hurence.logisland.plugin;

import com.hurence.logisland.BannerLoader;
import com.hurence.logisland.packaging.LogislandPluginLayoutFactory;
import com.hurence.logisland.packaging.LogislandRepackager;
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
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class PluginManager {

    private static Set<ArtifactDownloadReport> downloadArtifacts(Ivy ivy, ModuleRevisionId moduleRevisionId, String[] confs) throws Exception {
        ResolveOptions resolveOptions = new ResolveOptions();
        resolveOptions.setDownload(true);
        resolveOptions.setTransitive(true);
        resolveOptions.setOutputReport(false);
        resolveOptions.setConfs(confs);
        //resolveOptions.setArtifactFilter(o -> !((Artifact) o).getModuleRevisionId().getOrganisation().equals("org.apache.kafka"));

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

        File logislandHome = System.getenv("LOGISLAND_HOME") != null ? new File(System.getenv("LOGISLAND_HOME")) : null;
        Options options = new Options();
        OptionGroup mainGroup = new OptionGroup()
                .addOption(OptionBuilder
                        .withDescription("Install a component. It can be either a plugin or a connector.")
                        .withArgName("artifact")
                        .hasArgs(1)
                        .withLongOpt("install")
                        .create("i"))
                .addOption(OptionBuilder
                        .withDescription("Removes a component. It can be either a plugin or a connector.")
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
        options
                .addOption(OptionBuilder
                        .withDescription("Print this help.")
                        .withLongOpt("help")
                        .create("h"))
                .addOption(OptionBuilder
                        .withDescription("Logisland home directory (default will use env variable LOGISLAND_HOME")
                        .withArgName("logisland_home")
                        .hasArgs(1)
                        .isRequired(logislandHome == null || (logislandHome.exists() && logislandHome.isDirectory()))
                        .withLongOpt("logisland-home")
                        .create());

        try {
            CommandLine commandLine = new PosixParser().parse(options, args);
            if (commandLine.hasOption("l")) {
                System.out.println("Gathering information");

            } else if (commandLine.hasOption("l")) {

            } else if (commandLine.hasOption("i")) {

            } else if (commandLine.hasOption("r")) {
            } else {
                printUsage(options);
            }

        } catch (ParseException e) {
            System.err.println(e.getMessage());
            System.out.println();
            printUsage(options);

        }
    }

    private static void printUsage(Options options) {
        new HelpFormatter().printHelp(180,
                "logisland-components", "\n", options, "\n",
                true);

    }


    public static void execute(String... args) throws Exception {

        IvySettings settings = new IvySettings();
        if (args.length > 1) {
            settings.load(new File(args[1]));
        } else {
            settings.load(IvySettings.getDefaultSettingsURL());
        }


        Ivy ivy = Ivy.newInstance(settings);

        ivy.bind();
        ModuleRevisionId revisionId = new ModuleRevisionId(new ModuleId("com.hurence.logisland", "logisland-connector-opc"), "0.15.0");
        Set<ArtifactDownloadReport> toBePackaged = downloadArtifacts(ivy, revisionId, new String[]{"default", "compile", "runtime"});
        // toBePackaged.addAll(downloadArtifacts(ivy, new ModuleRevisionId(new ModuleId("org.apache.activemq", "activemq-all"), "5.15.5"),  new String[]{"default", "compile", "runtime"}));

        Set<ArtifactDownloadReport> environment = downloadArtifacts(ivy, revisionId, new String[]{"provided"});
        Set<ArtifactDownloadReport> excluded = toBePackaged.stream()
                .filter(adr -> excludeGroupIds.stream().anyMatch(s -> s.matches(adr.getArtifact().getModuleRevisionId().getOrganisation())) ||
                        excludedArtifactsId.stream().anyMatch(s -> s.matches(adr.getArtifact().getModuleRevisionId().getName())))
                .collect(Collectors.toSet());

        toBePackaged.removeAll(excluded);
        environment.addAll(excluded);

        Repackager rep = new Repackager(
                toBePackaged.stream().filter(adr -> adr.getArtifact().getModuleRevisionId().equals(revisionId)).findFirst().get().getLocalFile(),
                new LogislandPluginLayoutFactory());
        rep.setMainClass("");
        rep.repackage(new File("/Users/amarziali/test.jar"), callback ->
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
        //TODO changeme
        LogislandRepackager.main(new String[]{"/Users/amarziali/test.jar", "org.apache.kafka.connect.connector.Connector", "BOOT-INF/lib-provided", "0.15.0"});
    }
}
