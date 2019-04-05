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
package com.hurence.logisland.processor.enrichment;

import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;
import sun.net.util.IPAddressUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by gregoire on 09/05/17.
 */
public abstract class IpAbstractProcessor extends AbstractProcessor {

    protected static final String PROP_IP_ADDRESS_FIELD = "ip.address.field";

    public static final PropertyDescriptor IP_ADDRESS_FIELD = new PropertyDescriptor.Builder()
            .name(PROP_IP_ADDRESS_FIELD)
            .description("The name of the field containing the ip address to use.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {

        final long start = System.nanoTime();

        String ipAddrField = context.getPropertyValue(IP_ADDRESS_FIELD).asString();

        if (records == null || records.size() == 0) {
            return Collections.emptyList();
        }

        String ip = null;
        for (final Record record : records) {
            if (record.hasField(ipAddrField)) {
                String ipAsString = record.getField(ipAddrField).asString();
                if (ipAsString == null)
                {
                    getLogger().debug("record has a null IP_ADDRESS_FIELD : {}. So it is ignored. record : '{}'", new Object[]{ipAddrField, record});
                    continue;
                }
                ip = ipAsString.trim();
            } else {
                getLogger().debug("record has no IP_ADDRESS_FIELD : {}. So it is ignored. record : '{}'", new Object[]{ipAddrField, record});
                continue;
            }
            if (ip.isEmpty()) {
                getLogger().debug("record has an empty IP_ADDRESS_FIELD : {}. So it is ignored. record : '{}'", new Object[]{ipAddrField, record});
                continue;
            }
            if (!IPAddressUtil.isIPv4LiteralAddress(ip) && !IPAddressUtil.isIPv6LiteralAddress(ip)) {
                getLogger().debug("record has an invalid ip '{}'. So it is ignored.  record : '{}'", new Object[]{ip, record});
                continue;
            }
            // ip cannot be null from here
            processIp(record, ip, context);
        }

        final long sendMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        getLogger().info("Processed {} records in {} milliseconds",
                new Object[]{records.size(), sendMillis});

        return records;
    }

    // ip is never null when passed
    protected abstract void processIp(Record record, String ip, ProcessContext context);

    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(IP_ADDRESS_FIELD);
        return properties;
    }
}

