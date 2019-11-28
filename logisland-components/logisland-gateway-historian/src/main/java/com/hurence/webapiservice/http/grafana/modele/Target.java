package com.hurence.webapiservice.http.grafana.modele;

/**
 * Serialized so should be a java bean.
 */
public class Target {
    private String target;
    private String refId;
    private String type;


    public Target(String target, String refId, String type) {
        this.target = target;
        this.refId = refId;
        this.type = type;
    }

    public Target() {
    }

    public String getTarget() {
        return target;
    }

    public String getRefId() {
        return refId;
    }

    public String getType() {
        return type;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public void setRefId(String refId) {
        this.refId = refId;
    }

    public void setType(String type) {
        this.type = type;
    }
}
