/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.service.solr;

import com.hurence.logisland.record.Record;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import static java.awt.SystemColor.text;

public class SolrUpdater implements Runnable {
    private final UpdateRequest req = new UpdateRequest();
    private final SolrClient solr;
    private final BlockingQueue<Record> records;
    private final int batchSize;
    private final long flushInterval;
    private volatile int batchedUpdates = 0;
    private volatile long lastTS = 0;

    public SolrUpdater(SolrClient solr, BlockingQueue<Record> records, int batchSize, long flushInterval) {
        this.solr = solr;
        this.records = records;
        this.batchSize = batchSize;
        this.flushInterval = flushInterval;
        this.lastTS = System.nanoTime() * 100; // far in the future ...
    }

    @Override
    public void run() {
        while (true) {

            // process record if one
            try {
                Record record = records.take();
                if (record != null) {
                    SolrInputDocument doc = new SolrInputDocument();
                    //   doc.setField("id", "doc-" + id.getAndIncrement());
                    doc.setField("text", text);
                    req.add(doc);
                    batchedUpdates++;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            //
            try {
                long currentTS = System.nanoTime();
                if ((currentTS - lastTS) >= flushInterval || batchedUpdates >= batchSize){
                    req.process(solr);
                    req.clear();
                    lastTS = currentTS;
                }

                // Thread.sleep(10);
            } catch (IOException | SolrServerException e) {
                e.printStackTrace();
            }
        }
    }
}
