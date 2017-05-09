package com.hurence.logisland.processor.elasticsearch.put;


import com.hurence.logisland.record.Record;

/**
 * Wrapper to encapsulate all of the information for the Put along with the Record.
 */
public class ElasticsearchPutRecord {

    private final String docIndex;
    private final String docType;
    private final Record record;

    public ElasticsearchPutRecord(String docIndex, String docType, Record record) {
        this.docIndex = docIndex;
        this.docType = docType;
        this.record = record;
    }

    public String getDocIndex() {
        return docIndex;
    }

    public String getDocType() {
        return docType;
    }

    public Record getRecord() {
        return record;
    }

}
