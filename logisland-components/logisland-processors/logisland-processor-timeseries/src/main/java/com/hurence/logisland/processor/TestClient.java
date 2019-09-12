package com.hurence.logisland.processor;

import com.hurence.logisland.record.Point;
import com.hurence.logisland.timeseries.converter.compaction.BinaryCompactionConverter;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.MapSolrParams;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TestClient {

    public static void main(String[] args) throws IOException, SolrServerException {

        final String solrUrl = "http://localhost:8983/solr";
        SolrClient client =  new HttpSolrClient.Builder(solrUrl)
                .withConnectionTimeout(10000)
                .withSocketTimeout(60000)
                .build();

        final Map<String, String> queryParamMap = new HashMap<String, String>();
        queryParamMap.put("q", "*:*");
      /*  queryParamMap.put("fl", "id, name");
        queryParamMap.put("sort", "id asc");*/
        MapSolrParams queryParams = new MapSolrParams(queryParamMap);

        final QueryResponse response = client.query("historian", queryParams);
        final SolrDocumentList documents = response.getResults();

        System.out.println("Found " + documents.getNumFound() + " documents");

        BinaryCompactionConverter converter = new BinaryCompactionConverter.Builder().build();
        for(SolrDocument document : documents) {
            final String id = (String) document.getFirstValue("id");
            final long start = (long) document.getFirstValue("chunk_start");
            final long end = (long) document.getFirstValue("chunk_end");
            final byte[] binaryTimeseries = (byte[]) document.getFirstValue("chunk_value");

            List<Point> points = Collections.emptyList();

            try {
               points = converter.unCompressPoints(binaryTimeseries, start, end);


            } catch (IOException e) {
                e.printStackTrace();
            }

            System.out.println("id: " + id + "; count: " + points.size());
        }
    }
}
