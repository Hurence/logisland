package com.hurence.webapiservice.util.modele;

import com.hurence.logisland.record.Point;

import java.util.List;

public class ChunkExpected {
    public List<Point> points;
    public byte[] compressedPoints;
    public long start;
    public long end;
    public double avg;
    public double min;
    public double max;
    public double sum;
    public boolean trend;
    public String name;
    public String sax;
    public List<String> tags;
//    public String recordName;
}
