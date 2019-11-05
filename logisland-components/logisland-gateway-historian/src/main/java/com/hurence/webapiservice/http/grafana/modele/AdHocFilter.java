package com.hurence.webapiservice.http.grafana.modele;

public class AdHocFilter {
    private String key;
    private String operator;
    private String value;


    public AdHocFilter(String target, String refId, String type) {
        this.key = target;
        this.operator = refId;
        this.value = type;
    }

    public AdHocFilter() {
    }

    public String getKey() {
        return key;
    }

    public String getOperator() {
        return operator;
    }

    public String getValue() {
        return value;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
