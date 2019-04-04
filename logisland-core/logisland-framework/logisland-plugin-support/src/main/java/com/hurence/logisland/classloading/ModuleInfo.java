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
package com.hurence.logisland.classloading;

/**
 *A simple module metadata bean.
 *
 * @#author amarziali
 */
public class ModuleInfo {

    private final String name;
    private final String description;
    private final String sourceArchive;
    private final String artifact;
    private final String version;

    public ModuleInfo(String name, String description, String sourceArchive, String artifact, String version) {
        this.name = name;
        this.description = description;
        this.sourceArchive = sourceArchive;
        this.artifact = artifact;
        this.version = version;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public String getSourceArchive() {
        return sourceArchive;
    }

    public String getArtifact() {
        return artifact;
    }

    public String getVersion() {
        return version;
    }

    @Override
    public String toString() {
        return "ModuleInfo{" +
                "name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", sourceArchive='" + sourceArchive + '\'' +
                ", artifact='" + artifact + '\'' +
                ", version='" + version + '\'' +
                '}';
    }
}
