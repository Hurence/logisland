package com.hurence.logisland.processor.enrichment;

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.logging.ComponentLog;
import com.hurence.logisland.logging.StandardComponentLogger;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.ProcessError;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.service.cache.CacheService;
import com.hurence.logisland.validator.StandardValidators;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

/**
 * Created by gregoire on 09/05/17.
 */
@Tags({"dns", "ip", "fqdn", "domain", "address", "fqhn", "reverse", "resolution"})
@CapabilityDescription("find full domain name corresponding to an ip")
public class IpToFqdn extends IpAbstractProcessor {
    private ComponentLog logger = new StandardComponentLogger(this.getIdentifier(), IpToFqdn.class);

    public static final PropertyDescriptor FQDN_FIELD = new PropertyDescriptor.Builder()
            .name("fqdn.field")
            .description("The field that will contain the full qualified domain name corresponding to the ip address.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor OVERRIDE_FQDN = new PropertyDescriptor.Builder()
            .name("override.fqdn.field")
            .description("If the field should be overridden when it already exists.")
            .required(false)
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor CACHE_SERVICE = new PropertyDescriptor.Builder()
            .name("cache.service")
            .description("The maximum number of element in the cache.")
            .required(true)
            .identifiesControllerService(CacheService.class)
            .build();

    protected CacheService<String, String> cacheService;

    @Override
    public boolean hasControllerService() {
        return true;
    }

    @Override
    public void init(final ProcessContext context) {
        cacheService = context.getPropertyValue(CACHE_SERVICE).asControllerService(CacheService.class);
        if(cacheService == null) {
            logger.error("Cache service is not initialized!");
        }

    }

    protected void processIp(Record record, String ip, ProcessContext context) {

        String fqdnField = context.getPropertyValue(FQDN_FIELD).asString();
        Boolean override = context.getPropertyValue(OVERRIDE_FQDN).asBoolean();

        if (!override && record.hasField(fqdnField)) {
            logger.trace("skipped domain name resolution for Record (Field is already set and override is set to false):" + record,
                    new Object[]{IP_ADDRESS_FIELD,
                            record.getField(fqdnField).getRawValue()});
            return;
        }

        String fqdn = null;
        try {
            fqdn = cacheService.get(ip);
        } catch (Exception e) {
            logger.trace("Could not use cache!");
        }

        if (fqdn == null) {
            try {
                InetAddress addr = InetAddress.getByName(ip);
                //Returns: the fully qualified domain name for this IP address, or if the operation is not allowed by the security check,
                //the textual representation of the IP address.
                fqdn = addr.getCanonicalHostName();
                try {
                    cacheService.set(ip, fqdn);
                } catch (Exception e) {
                    logger.trace("Could not use cache!");
                }
            } catch(UnknownHostException ex) {
                logger.error("following error for ip {}, for record {}.", new Object[]{ip, record}, ex);
                String msg = "Could not find FQDN for ip: '" + ip + "', for record: '" + record.toString() + "'.\n cause: " + ex.getMessage();
                record.addError(ProcessError.RUNTIME_ERROR.toString(), msg);
                return;
            }
        }

        if (fqdn.equals(ip)) {
            logger.debug("Could not find fqdn corresponding to ip {}. This may be an authorization problem.",
                    new Object[]{ip});
        } else {
            record.setField(fqdnField, FieldType.STRING, fqdn);
            logger.trace("set value of field {} to {} for record {}",
                    new Object[]{fqdnField, fqdn, record});
        }

    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = super.getSupportedPropertyDescriptors();
        properties.add(FQDN_FIELD);
        properties.add(OVERRIDE_FQDN);
        properties.add(CACHE_SERVICE);
        return properties;
    }
}


