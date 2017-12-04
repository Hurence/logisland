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
package com.hurence.logisland.processor;

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.component.PropertyValue;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@Tags({"record", "traffic", "source"})
@CapabilityDescription("Compute the source of traffic of a user from a website visit\n" +
        "...")
public class SourceOfTraffic extends AbstractProcessor {


    private static final Logger logger = LoggerFactory.getLogger(SourceOfTraffic.class);

    private static final PropertyDescriptor REFERER_FIELD = new PropertyDescriptor.Builder()
            .name("referer.field")
            .description("Name of the field containing the referer value in the record")
            .required(false)
            .defaultValue("referer")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final PropertyDescriptor UTM_SOURCE_FIELD = new PropertyDescriptor.Builder()
            .name("utm_source.field")
            .description("Name of the field containing the utm_source value in the record")
            .required(false)
            .defaultValue("utm_source")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final PropertyDescriptor UTM_CAMPAIGN_FIELD = new PropertyDescriptor.Builder()
            .name("utm_campaign.field")
            .description("Name of the field containing the utm_campaign value in the record")
            .required(false)
            .defaultValue("utm_campaign")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final PropertyDescriptor UTM_MEDIUM_FIELD = new PropertyDescriptor.Builder()
            .name("utm_medium.field")
            .description("Name of the field containing the utm_medium value in the record")
            .required(false)
            .defaultValue("utm_medium")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final PropertyDescriptor UTM_CONTENT_FIELD = new PropertyDescriptor.Builder()
            .name("utm_content.field")
            .description("Name of the field containing the utm_content value in the record")
            .required(false)
            .defaultValue("utm_content")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final PropertyDescriptor UTM_TERM_FIELD = new PropertyDescriptor.Builder()
            .name("utm_term.field")
            .description("Name of the field containing the utm_term value in the record")
            .required(false)
            .defaultValue("utm_term")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final PropertyDescriptor SOURCE_OF_TRAFFIC_SUFFIX_FIELD = new PropertyDescriptor.Builder()
            .name("source.out.field")
            .description("Name of the field containing the source of the traffic outcome")
            .required(false)
            .defaultValue("source_of_traffic")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    /**
     * The source identified for the web session
     */
    public static final String SOURCE_OF_TRAFIC_FIELD_SOURCE = "source";

    /**
     * The medium identified for the web session
     */
    public static final String SOURCE_OF_TRAFIC_FIELD_MEDIUM = "medium";

    /**
     * The campaign identified for the web session
     */
    public static final String SOURCE_OF_TRAFIC_FIELD_CAMPAIGN = "campaign";

    /**
     * The content identified for the web session
     */
    public static final String SOURCE_OF_TRAFIC_FIELD_CONTENT = "content";

    /**
     * The term/keyword identified for the web session
     */
    public static final String SOURCE_OF_TRAFIC_FIELD_KEYWORD = "keyword";

    /**
     * Wether source of trafic is an organic search or not for the web session
     */
    public static final String SOURCE_OF_TRAFIC_FIELD_ORGANIC_SEARCHES = "organic_search";

    /**
     * The referral path identified for the web session
     */
    public static final String SOURCE_OF_TRAFIC_FIELD_REFERRAL_PATH = "referral_path";


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(REFERER_FIELD);
        descriptors.add(UTM_SOURCE_FIELD);
        descriptors.add(UTM_MEDIUM_FIELD);
        descriptors.add(UTM_CAMPAIGN_FIELD);
        descriptors.add(UTM_CONTENT_FIELD);
        descriptors.add(UTM_TERM_FIELD);
        descriptors.add(SOURCE_OF_TRAFFIC_SUFFIX_FIELD);
        return Collections.unmodifiableList(descriptors);
    }


    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {
        final String utm_source    = context.getProperty(UTM_SOURCE_FIELD);
        final String utm_medium    = context.getProperty(UTM_MEDIUM_FIELD);
        final String utm_campaign  = context.getProperty(UTM_CAMPAIGN_FIELD);
        final String utm_content   = context.getProperty(UTM_CONTENT_FIELD);
        final String utm_term      = context.getProperty(UTM_TERM_FIELD);
        final String output_suffix = context.getProperty(SOURCE_OF_TRAFFIC_SUFFIX_FIELD);
        final String referer       = context.getProperty(REFERER_FIELD);

        for (Record record : records) {

            //
        }
        return records;
    }


}
