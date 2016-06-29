/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hurence.logisland.botsearch;


/**
 *
 * @author tom
 */
public class TraceCluster {

    private String id;
    private String description;
    private long tracesCount;
    private double[] center = new double[4];
    private double[] radius = new double[4];


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public long getTracesCount() {
        return tracesCount;
    }

    public void setTracesCount(long tracesCount) {
        this.tracesCount = tracesCount;
    }

    public double[] getCenter() {
        return center;
    }

    public void setCenter(double[] center) {
        this.center = center;
    }

    public double[] getRadius() {
        return radius;
    }

    public void setRadius(double[] radius) {
        this.radius = radius;
    }
}
