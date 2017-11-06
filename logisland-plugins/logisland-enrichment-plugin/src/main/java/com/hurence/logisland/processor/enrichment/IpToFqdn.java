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
@CapabilityDescription("Translates an IP address into a FQDN (Fully Qualified Domain Name)")
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
            .description("The name of the cache service to use.")
            .required(true)
            .identifiesControllerService(CacheService.class)
            .build();

    public static final PropertyDescriptor CACHE_MAX_TIME = new PropertyDescriptor.Builder()
            .name("cache.max.time")
            .description("The amount of time, in seconds, for which a cached FQDN value is valid in the cache service. After this delay, " +
                    "the next new request to translate the same IP into FQDN will trigger a new reverse DNS request and the" +
                    " result will overwrite the entry in the cache. This allows two things: if the IP was not resolved into" +
                    " a FQDN, this will get a chance to obtain a FQDN if the DNS system has been updated," +
                    " if the IP is resolved into a FQDN, this will allow to be more accurate if the DNS system has been updated. " +
                    " A value of 0 seconds disables this expiration mechanism. The default value is 86400 seconds, which corresponds " +
                    " to new requests triggered every day if a record with the same IP passes every day in the processor."
            )
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("84600")
            .build();

    protected CacheService<String, CacheEntry> cacheService;

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
        long cacheValidityPeriod = (long)context.getPropertyValue(CACHE_MAX_TIME).asInteger();

        if (!override && record.hasField(fqdnField)) {
            logger.trace("skipped domain name resolution for Record (Field is already set and override is set to false):" + record,
                    new Object[]{IP_ADDRESS_FIELD,
                            record.getField(fqdnField).getRawValue()});
            return;
        }

        /**
         * Attempt to find info from the cache
         */
        CacheEntry cacheEntry = null;
        try {
            cacheEntry = cacheService.get(ip);
        } catch (Exception e) {
            logger.trace("Could not use cache!");
        }

        /**
         * If something in the cache, get it and be sure it is not obsolete
         */
        String fqdn = null;
        if (cacheEntry != null) { // Something in the cache?
            fqdn = cacheEntry.getFqdn(); // May be null, in which case this means there was no FQDN found at last attempt
            if (cacheValidityPeriod > 0) { // Cache validity period enabled?
                long cacheTime = cacheEntry.getTime();
                long now = System.currentTimeMillis();
                long cacheAge = now - cacheTime;
                if (cacheAge > (cacheValidityPeriod * 1000L)) { // Cache entry older than allowed max age?
                    fqdn = null; // Cache entry expired, force triggering a new request
                }
            }
        }

        if (fqdn == null) {
            try {
                InetAddress addr = InetAddress.getByName(ip);
                //Returns: the fully qualified domain name for this IP address, or if the operation is not allowed by the security check,
                //the textual representation of the IP address.
                fqdn = addr.getCanonicalHostName();
                try {
                    // Store the found FQDN (or the ip if the FQDN could not be found)
                    cacheEntry = new CacheEntry(fqdn, System.currentTimeMillis());
                    cacheService.set(ip, cacheEntry);
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
        properties.add(CACHE_MAX_TIME);
        return properties;
    }

    /**
     * Cached entity
     */
    private static class CacheEntry
    {
        // FQDN translated from the ip (or the ip if the FQDN could not be found)
        private String fqdn = null;
        // Time at which this cache entry has been stored in the cache service
        private long time = 0L;

        public CacheEntry(String fqdn, long time)
        {
            this.fqdn = fqdn;
            this.time = time;
        }

        public String getFqdn()
        {
            return fqdn;
        }

        public long getTime()
        {
            return time;
        }
    }
}


