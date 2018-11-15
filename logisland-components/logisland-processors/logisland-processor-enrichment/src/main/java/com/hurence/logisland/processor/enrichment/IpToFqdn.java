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

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.classloading.PluginProxy;
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
import java.util.concurrent.*;

/**
 * Processor to resolve an IP into a FQDN (Fully Qualified Domain Name).
 * <p>
 * An input field from the record has the IP as value. An new field is created and its value is the FQDN matching the
 * IP address.
 */
@Tags({"dns", "ip", "fqdn", "domain", "address", "fqhn", "reverse", "resolution", "enrich"})
@CapabilityDescription("Translates an IP address into a FQDN (Fully Qualified Domain Name). An input field from the" +
        " record has the IP as value. An new field is created and its value is the FQDN matching the IP address. The" +
        " resolution mechanism is based on the underlying operating system. The resolution request may take some time," +
        " specially if the IP address cannot be translated into a FQDN. For these reasons this processor relies on the" +
        " logisland cache service so that once a resolution occurs or not, the result is put into the cache. That way," +
        " the real request for the same IP is not re-triggered during a certain period of time, until the cache entry" +
        " expires. This timeout is configurable but by default a request for the same IP is not triggered before 24 hours" +
        " to let the time to the underlying DNS system to be potentially updated.")
public class IpToFqdn extends IpAbstractProcessor {

    private ComponentLog logger = new StandardComponentLogger(this.getIdentifier(), IpToFqdn.class);

    protected CacheService<String, CacheEntry> cacheService;

    protected String fqdnField = null;
    protected boolean overwrite = false;
    protected static final long DEFAULT_CACHE_VALIDITY_PERIOD = 84600L;
    protected long cacheValidityPeriodSec = DEFAULT_CACHE_VALIDITY_PERIOD;
    protected static final long DEFAULT_RESOLUTION_TIMEOUT = 1000L;
    protected long resolutionTimeoutMs = DEFAULT_RESOLUTION_TIMEOUT;
    protected boolean debug = false;

    static final String DEBUG_OS_RESOLUTION_TIME_MS_SUFFIX = "_os_resolution_time_ms";
    static final String DEBUG_OS_RESOLUTION_TIMEOUT_SUFFIX = "_os_resolution_timeout";
    static final String DEBUG_FROM_CACHE_SUFFIX = "_from_cache";

    // Definitions for config properties
    protected static final String PROP_FQDN_FIELD = "fqdn.field";
    protected static final String PROP_OVERWRITE_FQDN = "overwrite.fqdn.field";
    protected static final String PROP_CACHE_SERVICE = "cache.service";
    protected static final String PROP_CACHE_MAX_TIME = "cache.max.time";
    protected static final String PROP_RESOLUTION_TIMEOUT = "resolution.timeout";
    protected static final String PROP_DEBUG = "debug";

    public static final PropertyDescriptor CONFIG_FQDN_FIELD = new PropertyDescriptor.Builder()
            .name(PROP_FQDN_FIELD)
            .description("The field that will contain the full qualified domain name corresponding to the ip address.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CONFIG_OVERWRITE_FQDN = new PropertyDescriptor.Builder()
            .name(PROP_OVERWRITE_FQDN)
            .description("If the field should be overwritten when it already exists.")
            .required(false)
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor CONFIG_CACHE_SERVICE = new PropertyDescriptor.Builder()
            .name(PROP_CACHE_SERVICE)
            .description("The name of the cache service to use.")
            .required(true)
            .identifiesControllerService(CacheService.class)
            .build();

    public static final PropertyDescriptor CONFIG_CACHE_MAX_TIME = new PropertyDescriptor.Builder()
            .name(PROP_CACHE_MAX_TIME)
            .description("The amount of time, in seconds, for which a cached FQDN value is valid in the cache service. After this delay, " +
                    "the next new request to translate the same IP into FQDN will trigger a new reverse DNS request and the" +
                    " result will overwrite the entry in the cache. This allows two things: if the IP was not resolved into" +
                    " a FQDN, this will get a chance to obtain a FQDN if the DNS system has been updated," +
                    " if the IP is resolved into a FQDN, this will allow to be more accurate if the DNS system has been updated. " +
                    " A value of 0 seconds disables this expiration mechanism. The default value is " + DEFAULT_CACHE_VALIDITY_PERIOD +
                    " seconds, which corresponds to new requests triggered every day if a record with the same IP passes every" +
                    " day in the processor."
            )
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue(new Long(DEFAULT_CACHE_VALIDITY_PERIOD).toString())
            .build();

    public static final PropertyDescriptor CONFIG_RESOLUTION_TIMEOUT = new PropertyDescriptor.Builder()
            .name(PROP_RESOLUTION_TIMEOUT)
            .description("The amount of time, in milliseconds, to wait at most for the resolution to occur. This avoids to block the stream" +
                    " for too much time. Default value is " + DEFAULT_RESOLUTION_TIMEOUT + "ms. If the delay expires and no resolution could" +
                    " occur before, the FQDN field is not created. A special value of 0 disables the logisland timeout and the resolution" +
                    " request may last for many seconds if the IP cannot be translated into a FQDN by the underlying operating system. In" +
                    " any case, whether the timeout occurs in logisland of in the operating system, the fact that a timeout occurs is kept" +
                    " in the cache system so that a resolution request for the same IP will not occur before the cache entry expires."
            )
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue(new Long(DEFAULT_RESOLUTION_TIMEOUT).toString())
            .build();

    public static final PropertyDescriptor CONFIG_DEBUG = new PropertyDescriptor.Builder()
            .name(PROP_DEBUG)
            .description("If true, some additional debug fields are added. If the FQDN field is named X," +
                    " a debug field named X" + DEBUG_OS_RESOLUTION_TIME_MS_SUFFIX + " contains the resolution time in ms (using the operating system, not the cache)." +
                    " This field is added whether the resolution occurs or time is out. A debug field named  X" + DEBUG_OS_RESOLUTION_TIMEOUT_SUFFIX + " contains" +
                    " a boolean value to indicate if the timeout occurred. Finally, a debug field named X" + DEBUG_FROM_CACHE_SUFFIX + " contains a boolean value" +
                    " to indicate the origin of the FQDN field. The default value for this property is false (debug is disabled.")
            .required(false)
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    @Override
    public boolean hasControllerService() {
        return true;
    }

    @Override
    public void init(final ProcessContext context) {
        cacheService = PluginProxy.rewrap(context.getPropertyValue(CONFIG_CACHE_SERVICE).asControllerService());
        if (cacheService == null) {
            logger.error("Cache service is not initialized!");
        }

    }

    protected void processIp(Record record, String ip, ProcessContext context) {

        fqdnField = context.getPropertyValue(CONFIG_FQDN_FIELD).asString();
        overwrite = context.getPropertyValue(CONFIG_OVERWRITE_FQDN).asBoolean();
        cacheValidityPeriodSec = (long) context.getPropertyValue(CONFIG_CACHE_MAX_TIME).asInteger();
        resolutionTimeoutMs = (long) context.getPropertyValue(CONFIG_RESOLUTION_TIMEOUT).asInteger();
        debug = context.getPropertyValue(CONFIG_DEBUG).asBoolean();

        if (!overwrite && record.hasField(fqdnField)) {
            logger.trace("Skipped domain name resolution for Record (Field is already set and override is set to false):" + record,
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
            logger.warn("Could not use cache!", e);
        }

        /**
         * If something in the cache, get it and be sure it is not obsolete
         */
        String fqdn = null;
        boolean fromCache = true;
        if (cacheEntry != null) { // Something in the cache?
            fqdn = cacheEntry.getFqdn();
            if (cacheValidityPeriodSec > 0) { // Cache validity period enabled?
                long cacheTime = cacheEntry.getTime();
                long now = System.currentTimeMillis();
                long cacheAge = now - cacheTime;
                if (cacheAge > (cacheValidityPeriodSec * 1000L)) { // Cache entry older than allowed max age?
                    fqdn = null; // Cache entry expired, force triggering a new request
                }
            }
        }

        if (fqdn == null) {
            // Not in the cache or cache entry expired, trigger a real resolution request to the underlying OS
            fromCache = false;
            InetAddress addr = null;
            try {
                addr = InetAddress.getByName(ip);
            } catch (UnknownHostException ex) {
                logger.error("Error for ip {}, for record {}.", new Object[]{ip, record}, ex);
                String msg = "Could not translate ip: '" + ip + "' into InetAddress, for record: '" + record.toString() + "'.\n Cause: " + ex.getMessage();
                record.addError(ProcessError.RUNTIME_ERROR.toString(), msg);
                return;
            }

            // Attempt to translate the ip into an FQDN
            Result result = ipToFqdn(addr, record);

            fqdn = result.getFqdn();
            boolean timeout = (fqdn == null);
            if (timeout) {
                // Timeout. For the moment, we do as if the FQDN could not have been resolved and store the IP.
                // That way, following requests to for the same IP will not immediately trigger a new resolution
                // request. The cache timeout will however allow to retry later. This also ends up with no FQDN field
                // created
                fqdn = ip;
            }

            if (debug) {
                // Add some debug fields
                record.setField(fqdnField + DEBUG_OS_RESOLUTION_TIMEOUT_SUFFIX, FieldType.BOOLEAN, timeout);
                record.setField(fqdnField + DEBUG_OS_RESOLUTION_TIME_MS_SUFFIX, FieldType.LONG, result.getResolutionTimeMs());
            }

            try {
                // Store the found FQDN (or the ip if the FQDN could not be found)
                cacheEntry = new CacheEntry(fqdn, System.currentTimeMillis());
                cacheService.set(ip, cacheEntry);
            } catch (Exception e) {
                logger.trace("Could not put entry in the cache:" + e.getMessage());
            }
        }

        if (fqdn.equals(ip)) {
            logger.debug("Could not find FQDN corresponding to ip {}. This may be an authorization problem.",
                    new Object[]{ip});
        } else {
            // Ok got a FQDN matching the IP, enrich the record
            record.setField(fqdnField, FieldType.STRING, fqdn);
            if (debug) {
                // Add some debug fields
                record.setField(fqdnField + DEBUG_FROM_CACHE_SUFFIX, FieldType.BOOLEAN, fromCache);
            }
            logger.trace("set value of field {} to {} for record {}",
                    new Object[]{fqdnField, fqdn, record});
        }
    }

    /**
     * Helper class for result of the ipToFqdn method
     */
    private static class Result {
        private String fqdn = null;
        private long resolutionTimeMs = 0L;

        public String getFqdn() {
            return fqdn;
        }

        public void setFqdn(String fqdn) {
            this.fqdn = fqdn;
        }

        public long getResolutionTimeMs() {
            return resolutionTimeMs;
        }

        public void setResolutionMs(long resolutionTimeMs) {
            this.resolutionTimeMs = resolutionTimeMs;
        }
    }

    /**
     * Request to the OS the translation of the IP address into a FQDN
     *
     * @param ip     IP to resolve
     * @param record The record in which one can add error if any during resolution attempt
     * @return Three possibilities for the FQDN:
     * - The FQDN matching the IP
     * - The IP if no FQDN found
     * - null If timeout waiting for an answer from the subsystem.
     * Also the resolution time is returned in any case
     */
    private Result ipToFqdn(InetAddress ip, Record record) {
        /**
         * We'll use the InetAddress.getCanonicalHostName method but it's a synchronized call and does not exist
         * in asynchronous mode. We don't want too wait too mush for a resolution so to implement a timeout, we
         * use a separated thread. We wait for the completion of this thread for a certain amount of time then
         * we stop waiting. This method returns null if this timeout occurs.
         * If the timeout has the special 0 value, use the synchronized call
         */
        Result result = new Result();
        String fqdn = null; // null means timeout
        ExecutorService executor = Executors.newSingleThreadExecutor();

        long start, stop;
        if (resolutionTimeoutMs != 0L) {
            start = System.currentTimeMillis();
            Future<String> future = executor.submit(new Callable<String>() {
                public String call() throws Exception {
                    // Returns the fully qualified domain name for this IP address, or if the operation is not allowed by the security check,
                    //the textual representation of the IP address.

                    return ip.getCanonicalHostName();
                }
            });

            try {
                fqdn = future.get(resolutionTimeoutMs, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                // fqdn stays null which means timeout
            } catch (InterruptedException e) {
                // Consider also it's a timeout, we gonna stop anyway
                logger.debug("Interrupted while trying to resolve ip {}.", new Object[]{ip});
            } catch (ExecutionException e) {
                // Too bad but let's say its also a timeout, log however an error
                logger.error("Error for ip {}, for record {}.", new Object[]{ip, record}, e);
                String msg = "Could not resolve ip: '" + ip + "' , for record: '" + record.toString() + "'.\n Cause: " + e.getMessage();
                record.addError(ProcessError.RUNTIME_ERROR.toString(), msg);
            }
            stop = System.currentTimeMillis();
            executor.shutdownNow();
        } else {
            // No timeout, directly use the synchronized call
            start = System.currentTimeMillis();
            fqdn = ip.getCanonicalHostName();
            stop = System.currentTimeMillis();
        }

        result.setFqdn(fqdn);
        result.setResolutionMs(stop - start);

        return result;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = super.getSupportedPropertyDescriptors();
        properties.add(CONFIG_FQDN_FIELD);
        properties.add(CONFIG_OVERWRITE_FQDN);
        properties.add(CONFIG_CACHE_SERVICE);
        properties.add(CONFIG_CACHE_MAX_TIME);
        properties.add(CONFIG_RESOLUTION_TIMEOUT);
        properties.add(CONFIG_DEBUG);
        return properties;
    }

    /**
     * Cached entity
     */
    private static class CacheEntry {
        // FQDN translated from the ip (or the ip if the FQDN could not be found)
        private String fqdn = null;
        // Time at which this cache entry has been stored in the cache service
        private long time = 0L;

        public CacheEntry(String fqdn, long time) {
            this.fqdn = fqdn;
            this.time = time;
        }

        public String getFqdn() {
            return fqdn;
        }

        public long getTime() {
            return time;
        }
    }
}


