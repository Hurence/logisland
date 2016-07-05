package com.hurence.logisland.config;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by tom on 01/07/16.
 */
public class LogislandSessionConfiguration {

    private String documentation = "";
    private String version = "";

    private List<ComponentConfiguration> components = new ArrayList<>();

    public String getDocumentation() {
        return documentation;
    }

    public void setDocumentation(String documentation) {
        this.documentation = documentation;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public List<ComponentConfiguration> getComponents() {
        return components;
    }

    public void setComponents(List<ComponentConfiguration> components) {
        this.components = components;
    }

    @Override
    public String toString() {
        return "LogislandSessionConfiguration{" +
                "documentation='" + documentation + '\'' +
                ", version='" + version + '\'' +
                ", components=" + components +
                '}';
    }
}
