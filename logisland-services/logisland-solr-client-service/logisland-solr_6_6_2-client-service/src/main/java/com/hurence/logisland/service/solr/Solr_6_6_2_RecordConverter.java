package com.hurence.logisland.service.solr;

import com.hurence.logisland.service.solr.api.SolrRecordConverter;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

import java.util.HashMap;
import java.util.Map;

public class Solr_6_6_2_RecordConverter extends SolrRecordConverter {
    @Override
    protected SolrInputDocument createNewSolrInputDocument() {
        Map<String,SolrInputField> fields = new HashMap<>();

        return new SolrInputDocument(fields);
    }
}
