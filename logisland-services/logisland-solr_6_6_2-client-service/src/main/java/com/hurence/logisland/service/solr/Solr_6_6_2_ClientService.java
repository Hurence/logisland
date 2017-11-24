/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.service.solr;

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.service.datastore.DatastoreClientService;
import org.slf4j.LoggerFactory;

@Tags({ "solr", "client"})
@CapabilityDescription("Implementation of ElasticsearchClientService for Solr 5.5.5.")
public class Solr_6_6_2_ClientService extends SolrClientService implements DatastoreClientService {
    private static org.slf4j.Logger logger = LoggerFactory.getLogger(Solr_6_6_2_ClientService.class);

}
