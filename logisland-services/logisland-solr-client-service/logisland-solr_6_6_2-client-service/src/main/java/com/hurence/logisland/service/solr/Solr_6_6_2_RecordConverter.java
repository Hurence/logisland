package com.hurence.logisland.service.solr;

import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.Record;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class Solr_6_6_2_RecordConverter extends  SolrRecordConverter{
    @Override
    protected SolrInputDocument createNewSolrInputDocument() {
        Map<String,SolrInputField> fields = new HashMap<>();

        return new SolrInputDocument(fields);
    }
}
