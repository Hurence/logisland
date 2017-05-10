/**
 * Copyright (C) 2017 Hurence (bailet.thomas@gmail.com)
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
package com.hurence.logisland.processor.elasticsearchasaservice;


import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.logging.ComponentLog;
import com.hurence.logisland.logging.StandardComponentLogger;
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.validator.StandardValidators;


public abstract class AbstractElasticsearchProcessor extends AbstractProcessor {

    private ComponentLog logger = new StandardComponentLogger(this.getIdentifier(), AbstractElasticsearchProcessor.class);

    public static final PropertyDescriptor ELASTICSEARCH_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("elasticsearch.client.service")
            .description("The instance of the Controller Service to use for accessing Elasticsearch.")
            .required(true)
            .identifiesControllerService(ElasticsearchClientService.class)
            .build();

    public static final PropertyDescriptor DEFAULT_INDEX = new PropertyDescriptor.Builder()
            .name("default.index")
            .description("The name of the index to insert into")
            .required(true)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor DEFAULT_TYPE = new PropertyDescriptor.Builder()
            .name("default.type")
            .description("The type of this document (used by Elasticsearch for indexing and searching)")
            .required(true)
            .expressionLanguageSupported(true)
            .build();

    public static final AllowableValue NO_DATE_SUFFIX = new AllowableValue("no", "No date",
            "no date added to default index");

    public static final AllowableValue TODAY_DATE_SUFFIX = new AllowableValue("today", "Today's date",
            "today's date added to default index");

    public static final AllowableValue YESTERDAY_DATE_SUFFIX = new AllowableValue("yesterday", "yesterday's date",
            "yesterday's date added to default index");

    public static final PropertyDescriptor TIMEBASED_INDEX = new PropertyDescriptor.Builder()
            .name("timebased.index")
            .description("do we add a date suffix")
            .required(true)
            .allowableValues(NO_DATE_SUFFIX, TODAY_DATE_SUFFIX, YESTERDAY_DATE_SUFFIX)
            .defaultValue(NO_DATE_SUFFIX.getValue())
            .build();

    public static final PropertyDescriptor ES_INDEX_FIELD = new PropertyDescriptor.Builder()
            .name("es.index.field")
            .description("the name of the event field containing es index type => will override index value if set")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ES_TYPE_FIELD = new PropertyDescriptor.Builder()
            .name("es.type.field")
            .description("the name of the event field containing es doc type => will override type value if set")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    protected ElasticsearchClientService elasticsearchClientService;

    @Override
    public boolean hasControllerService() {
        return true;
    }

    @Override
    public void init(final ProcessContext context) {
        elasticsearchClientService = context.getPropertyValue(ELASTICSEARCH_CLIENT_SERVICE).asControllerService(ElasticsearchClientService.class);
        if(elasticsearchClientService == null) {
            logger.error("Elasticsearch client service is not initialized!");
        }
    }




}
