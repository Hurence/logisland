package com.hurence.logisland.processor.enrichment;

import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.logging.ComponentLog;
import com.hurence.logisland.logging.StandardComponentLogger;
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
    private ComponentLog logger = new StandardComponentLogger(this.getIdentifier(), IpAbstractProcessor.class);

    public static final PropertyDescriptor IP_ADDRESS_FIELD = new PropertyDescriptor.Builder()
            .name("ip.address.field")
            .description("The field containing the ip address we want to discover FQDN (full qualified domain name)")
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
            if (record.hasField(context.getPropertyValue(IP_ADDRESS_FIELD).asString()))
                ip = record.getField(ipAddrField).asString().trim();
            else {
                logger.debug("record has no IP_ADDRESS_FIELD : {}. So it is ignored. record : '{}'", new Object[]{ipAddrField, record});
                continue;
            }
            if (ip.isEmpty()) {
                logger.debug("record has an empty IP_ADDRESS_FIELD : {}. So it is ignored. record : '{}'", new Object[]{ipAddrField, record});
                continue;
            }
            if (!IPAddressUtil.isIPv4LiteralAddress(ip) && !IPAddressUtil.isIPv6LiteralAddress(ip)) {
                logger.debug("record has an invalid ip '{}'. So it is ignored.  record : '{}'", new Object[]{ip, record});
                continue;
            }
            processIp(record, ip, context);
        }

        final long sendMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        logger.info("Processed {} records in {} milliseconds",
                new Object[]{records.size(), sendMillis});

        return records;
    }

    protected abstract void processIp(Record record, String ip, ProcessContext context);

    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(IP_ADDRESS_FIELD);
        return properties;
    }
}

