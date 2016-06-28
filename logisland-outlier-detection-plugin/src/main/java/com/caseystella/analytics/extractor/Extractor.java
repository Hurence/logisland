package com.caseystella.analytics.extractor;

import com.caseystella.analytics.DataPoint;

import java.io.Serializable;

/**
 * Created by cstella on 2/27/16.
 */
public interface Extractor extends Serializable{
    Iterable<DataPoint> extract(byte[] key, byte[] value, boolean failOnMalformed);
}
