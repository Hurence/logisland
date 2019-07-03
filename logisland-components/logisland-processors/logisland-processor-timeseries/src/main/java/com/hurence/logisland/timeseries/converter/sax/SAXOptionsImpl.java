package com.hurence.logisland.timeseries.converter.sax;

public class SAXOptionsImpl implements SAXOptions {

    private int paaSize;
    private double nThreshold;
    private int alphabetSize;

    public SAXOptionsImpl(int paaSize, double nThreshold, int alphabetSize) {
        this.paaSize = paaSize;
        this.nThreshold = nThreshold;
        this.alphabetSize = alphabetSize;
    }

    @Override
    public int paaSize() {
        return paaSize;
    }

    @Override
    public double nThreshold() {
        return nThreshold;
    }

    @Override
    public int alphabetSize() {
        return alphabetSize;
    }
}
