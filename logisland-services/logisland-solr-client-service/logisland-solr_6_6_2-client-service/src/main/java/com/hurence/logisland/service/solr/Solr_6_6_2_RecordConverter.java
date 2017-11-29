package com.hurence.logisland.service.solr;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class Solr_6_6_2_RecordConverter extends  SolrRecordConverter{

    @Override
    public SolrInputDocument toSolrInputDocument(SolrDocument document) {
        Map<String,SolrInputField> fields = new HashMap<>();
        SolrInputDocument inputDocument = new SolrInputDocument(fields);

        for (String name : document.getFieldNames()) {
            inputDocument.addField(name, document.getFieldValue(name));
        }

        return inputDocument;
    }
}
