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
package com.hurence.logisland.maven;

import com.hurence.logisland.packaging.LogislandRepackager;
import org.apache.maven.artifact.Artifact;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

import java.io.File;
import java.util.Arrays;
import java.util.Set;

@Mojo(name = "repackage")
public class LogislandRepackagerMojo extends AbstractMojo {

    /**
     * The maven project.
     */
    @Parameter(defaultValue = "${project}", readonly = true)
    private MavenProject project;

    /**
     * The base classes to export. If a classes inheriting one of those is found, it is considered as a component
     * to expert and the class name will be added to the manifest.
     */
    @Parameter(required = true)
    private String[] exportedBaseClasses;

    /**
     * The provided lib folder to be filtered out.
     */
    @Parameter(defaultValue = "lib-provided")
    private String providedLibFolder;

    @Parameter
    private ArtifactDescription[] apiArtifacts = {};


    /**
     * If specified, any class matching the pattern will be loaded first by the parent breaking the child first reverse
     * delegation.
     */
    @Parameter
    private String[] classloaderParentFirstPatterns = {};


    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        try {
            File file = getTargetFile();
            LogislandRepackager.execute(file.getAbsolutePath(),
                    providedLibFolder,
                    project.getVersion(),
                    project.getGroupId() + ":" + project.getArtifactId() + ":" + project.getVersion(),
                    project.getName(),
                    project.getDescription(),
                    classloaderParentFirstPatterns,
                    ((Set<Artifact>) project.getDependencyArtifacts()).stream()
                            .filter(artifact -> Arrays.stream(apiArtifacts)
                                    .anyMatch(a -> a.getGroupId().equals(artifact.getGroupId()) &&
                                            a.getArtifactId().equals(artifact.getArtifactId())))
                            .map(Artifact::getFile)
                            .map(File::getName)
                            .toArray(a -> new String[a]),
                    exportedBaseClasses);
//            project.getArtifact().setFile(file);
        } catch (Exception e) {
            throw new MojoFailureException("Logisland repackager failed", e);
        }


    }

    private File getTargetFile() {
        if (project.getBuild().getFinalName().equalsIgnoreCase(
                "logisland-service-elasticsearch_2_4_0-client-1.4.0"
        )) {
            return new File(project.getBuild().getDirectory(), project.getBuild().getFinalName()  + "-repackaged" + "."
                    + this.project.getArtifact().getArtifactHandler().getExtension());
        } else {
            return new File(project.getBuild().getDirectory(), project.getBuild().getFinalName() + "."
                    + this.project.getArtifact().getArtifactHandler().getExtension());
        }

    }
}
