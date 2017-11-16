/**
 * Copyright (C) 2017 Hurence
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.processor.enrichment;
import static com.hurence.logisland.service.iptogeo.IpToGeoService.*;

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.component.PropertyValue;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.service.cache.CacheService;
import com.hurence.logisland.service.iptogeo.IpToGeoService;
import com.hurence.logisland.validator.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@Tags({"geo", "enrich", "ip"})
@CapabilityDescription("Looks up geolocation information for an IP address. The attribute that contains the IP address to lookup must be provided in the **"
        + IpAbstractProcessor.PROP_IP_ADDRESS_FIELD + "** property. By default, the geo information are put in a hierarchical structure. " +
        "That is, if the name of the IP field is 'X', then the the geo attributes added by enrichment are added under a father field" +
        " named X_geo. \"_geo\" is the default hierarchical suffix that may be changed with the **" + IpToGeo.PROP_HIERARCHICAL_SUFFIX  +
        "** property. If one wants to put the geo fields at the same level as the IP field, then the **" + IpToGeo.PROP_HIERARCHICAL + "** property should be set to false and then the geo attributes are " +
        " created at the same level as him with the naming pattern X_geo_<geo_field>. \"_geo_\" is the default flat suffix but this may be changed with the **" +
        IpToGeo.PROP_FLAT_SUFFIX + "** property. The IpToGeo processor requires a reference to an Ip to Geo service. This must be defined in the **" +
        IpToGeo.PROP_IP_TO_GEO_SERVICE + "** property. The added geo fields are dependant on the underlying Ip to Geo service. The **" +
        IpToGeo.PROP_GEO_FIELDS + "** property must contain the list of geo fields that should be created if data is available for " +
        " the IP to resolve. This property defaults to \"*\" which means to add every available fields. If one only wants a subset of the fields, " +
        " one must define a comma separated list of fields as a value for the **" + IpToGeo.PROP_GEO_FIELDS + "** property. The list of the available geo fields" +
        " is in the description of the **" + IpToGeo.PROP_GEO_FIELDS + "** property."
)
public class IpToGeo extends IpAbstractProcessor {

    private static Logger logger = LoggerFactory.getLogger(IpToGeo.class);

    protected static final String PROP_IP_TO_GEO_SERVICE = "iptogeo.service";
    protected static final String PROP_GEO_FIELDS = "geo.fields";
    protected static final String PROP_HIERARCHICAL = "geo.hierarchical";
    protected static final String PROP_HIERARCHICAL_SUFFIX = "geo.hierarchical.suffix";
    protected static final String PROP_FLAT_SUFFIX = "geo.flat.suffix";
    protected static final String PROP_DEBUG = "debug";
    protected static final long DEFAULT_CACHE_VALIDITY_PERIOD = 0;
    protected long cacheValidityPeriodSec = DEFAULT_CACHE_VALIDITY_PERIOD;
    protected CacheService<String, IpToGeo.CacheEntry> cacheService;
    protected boolean debug = false;
    static final String DEBUG_FROM_CACHE_SUFFIX = "_from_cache";
    protected static final String PROP_CACHE_SERVICE = "cache.service";


    public static final PropertyDescriptor IP_TO_GEO_SERVICE = new PropertyDescriptor.Builder()
            .name(PROP_IP_TO_GEO_SERVICE)
            .description("The reference to the IP to Geo service to use.")
            .required(true)
            .identifiesControllerService(IpToGeoService.class)
            .build();

    public static final PropertyDescriptor GEO_FIELDS = new PropertyDescriptor.Builder()
            .name(PROP_GEO_FIELDS)
            .description("Comma separated list of geo information fields to add to the record. Defaults to '*', which means to include all available fields. If a list " +
                    "of fields is specified and the data is not available, the geo field is not created. The geo fields are dependant on the underlying defined Ip to Geo service. " +
                    "The currently only supported type of Ip to Geo service is the Maxmind Ip to Geo service. This means that the currently " +
                    "supported list of geo fields is the following:" +
                    "**continent**: the identified continent for this IP address. " +
                    "**continent_code**: the identified continent code for this IP address. " +
                    "**city**: the identified city for this IP address. " +
                    "**latitude**: the identified latitude for this IP address. " +
                    "**longitude**: the identified longitude for this IP address. " +
                    "**location**: the identified location for this IP address, defined as Geo-point expressed as a string with the format: 'latitude,longitude'. " +
                    "**accuracy_radius**: the approximate accuracy radius, in kilometers, around the latitude and longitude for the location. " +
                    "**time_zone**: the identified time zone for this IP address. " +
                    "**subdivision_N**: the identified subdivision for this IP address. N is a one-up number at the end of the attribute name, starting with 0. " +
                    "**subdivision_isocode_N**: the iso code matching the identified subdivision_N. " +
                    "**country**: the identified country for this IP address. " +
                    "**country_isocode**: the iso code for the identified country for this IP address. " +
                    "**postalcode**: the identified postal code for this IP address. " +
                    "**lookup_micros**: the number of microseconds that the geo lookup took. The Ip to Geo service must have the " + IpToGeoService.GEO_FIELD_LOOKUP_TIME_MICROS + " property enabled in order to have this field available."
            )
            .required(false)
            .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
            .defaultValue("*")
            .build();

    public static final PropertyDescriptor HIERARCHICAL = new PropertyDescriptor.Builder()
            .name(PROP_HIERARCHICAL)
            .description("Should the additional geo information fields be added under a hierarchical father field or not.")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor HIERARCHICAL_SUFFIX = new PropertyDescriptor.Builder()
            .name(PROP_HIERARCHICAL_SUFFIX)
            .description("Suffix to use for the field holding geo information. If " + PROP_HIERARCHICAL +
                    " is true, then use this suffix appended to the IP field name to define the father field name." +
                    " This may be used for instance to distinguish between geo fields with various locales using many" +
                    " Ip to Geo service instances.")
            .required(false)
            .defaultValue("_geo")
            .build();

    public static final PropertyDescriptor FLAT_SUFFIX = new PropertyDescriptor.Builder()
            .name(PROP_FLAT_SUFFIX)
            .description("Suffix to use for geo information fields when they are flat. If " + PROP_HIERARCHICAL +
                    " is false, then use this suffix appended to the IP field name but before the geo field name." +
                    " This may be used for instance to distinguish between geo fields with various locales using many" +
                    " Ip to Geo service instances.")
            .required(false)
            .defaultValue("_geo_")
            .build();

    public static final PropertyDescriptor CONFIG_CACHE_SERVICE = new PropertyDescriptor.Builder()
            .name(PROP_CACHE_SERVICE)
            .description("The name of the cache service to use.")
            .required(true)
            .identifiesControllerService(CacheService.class)
            .build();

    /* WARNING: This property is commented as right now we don't support live Geolite db update. */
//     public static final PropertyDescriptor CONFIG_CACHE_MAX_TIME = new PropertyDescriptor.Builder()
//            .name(PROP_CACHE_MAX_TIME)
//            .description("The amount of time, in seconds, for which a cached geoInfo value is valid in the cache service. After this delay, " +
//                    "the next new request to translate the same IP into geoInfo will trigger a new request in the Geolite db and the" +
//                    " result will overwrite the entry in the cache. This will facilitate the support in the future for live upgrade of the Geolite database." +
//                    " A value of 0 seconds disables this expiration mechanism. The default value is " + DEFAULT_CACHE_VALIDITY_PERIOD +
//                    " seconds, which corresponds to new requests triggered every day if a record with the same IP passes every" +
//                    " day in the processor."
//            )
//            .required(false)
//            .addValidator(StandardValidators.INTEGER_VALIDATOR)
//            .defaultValue(new Long(DEFAULT_CACHE_VALIDITY_PERIOD).toString())
//            .build();


    public static final PropertyDescriptor CONFIG_DEBUG = new PropertyDescriptor.Builder()
            .name(PROP_DEBUG)
            .description("If true, some additional debug fields are added. If the geoInfo field is named X," +
                    " a debug field named X" + DEBUG_FROM_CACHE_SUFFIX + " contains a boolean value" +
                    " to indicate the origin of the geoInfo field. The default value for this property is false (debug is disabled.")
            .required(false)
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    // Ip to Geo service to use to perform the translation requests
    private IpToGeoService ipToGeoService = null;
    // List of fields to add (* means all available fields)
    private String geoFields = "*";
    // Should we use all available fields or the list in geoFields?
    private boolean allFields = true;
    // Should the geo fields be added in a hierarchical view or as flat fields
    private boolean hierarchical = true;
    // Suffix to append to the ip field name for defining the the father field name if hierarchical is true
    private String hierarchicalSuffix = "_geo";
    // Suffix to append to the ip field name and before the geo field name if hierarchical is false
    private String flatSuffix = "_geo_";

    private boolean needSubdivision = false;
    private boolean needSubdivisionIsocode = false;

    // Supported field names. Key: geo field name, Value: the field type to use
    static Map<String, FieldType> supportedGeoFieldNames = new HashMap<String, FieldType>() {{
        put(GEO_FIELD_LOOKUP_TIME_MICROS, FieldType.INT);
        put(GEO_FIELD_CONTINENT, FieldType.STRING);
        put(GEO_FIELD_CONTINENT_CODE, FieldType.STRING);
        put(GEO_FIELD_CITY, FieldType.STRING);
        put(GEO_FIELD_LATITUDE, FieldType.DOUBLE);
        put(GEO_FIELD_LONGITUDE, FieldType.DOUBLE);
        put(GEO_FIELD_LOCATION, FieldType.STRING);
        put(GEO_FIELD_ACCURACY_RADIUS, FieldType.INT);
        put(GEO_FIELD_TIME_ZONE, FieldType.STRING);
        put(GEO_FIELD_SUBDIVISION, FieldType.STRING);
        put(GEO_FIELD_SUBDIVISION_ISOCODE, FieldType.STRING);
        put(GEO_FIELD_COUNTRY, FieldType.STRING);
        put(GEO_FIELD_COUNTRY_ISOCODE, FieldType.STRING);
        put(GEO_FIELD_POSTALCODE, FieldType.STRING);
    }};

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = super.getSupportedPropertyDescriptors();
        properties.add(IP_TO_GEO_SERVICE);
        properties.add(GEO_FIELDS);
        properties.add(HIERARCHICAL);
        properties.add(HIERARCHICAL_SUFFIX);
        properties.add(FLAT_SUFFIX);
        properties.add(CONFIG_CACHE_SERVICE);
//        properties.add(CONFIG_CACHE_MAX_TIME);
        properties.add(CONFIG_DEBUG);
        return properties;
    }

    @Override
    public boolean hasControllerService() {
        return true;
    }

    @Override
    public void init(final ProcessContext context) {

        /**
         * Get the Ip to Geo Service
         */

        ipToGeoService = context.getPropertyValue(IP_TO_GEO_SERVICE).asControllerService(IpToGeoService.class);
        if(ipToGeoService == null) {
            logger.error("IpToGeoService service is not initialized!");
        }

        PropertyValue propertyValue = context.getPropertyValue(GEO_FIELDS);
        if (propertyValue != null) {
            geoFields = propertyValue.asString();
        }

        allFields = geoFields.trim().equals("*");

        propertyValue = context.getPropertyValue(HIERARCHICAL);
        if (propertyValue != null) {
            hierarchical = propertyValue.asBoolean();
        }

        propertyValue = context.getPropertyValue(HIERARCHICAL_SUFFIX);
        if (propertyValue != null) {
            hierarchicalSuffix = propertyValue.asString();
        }

        propertyValue = context.getPropertyValue(FLAT_SUFFIX);
        if (propertyValue != null) {
            flatSuffix = propertyValue.asString();
        }

        cacheService = context.getPropertyValue(CONFIG_CACHE_SERVICE).asControllerService(CacheService.class);
        if(cacheService == null) {
            logger.error("Cache service is not initialized!");
        }
    }

    /**
     * Get the list of geo fields to add
     * @return the list of geo fields to add
     */
    private Set<String> getConfiguredGeoFieldNames() throws Exception
    {
        Set<String> result = new HashSet<String>();
        for (String field : geoFields.trim().split(","))
        {
            field = field.trim();
            if (supportedGeoFieldNames.containsKey(field))
            {
                result.add(field);
                if (field.equals(GEO_FIELD_SUBDIVISION))
                {
                    // Keep track of the fact that GEO_FIELD_SUBDIVISION is requested
                    needSubdivision = true;
                }
                if (field.equals(GEO_FIELD_SUBDIVISION_ISOCODE))
                {
                    // Keep track of the fact that GEO_FIELD_SUBDIVISION_ISOCODE is requested
                    needSubdivisionIsocode = true;
                }
            } else
            {
                throw new Exception("Unsupported geo field name: " + field);
            }
        }
        return result;
    }

    protected void processIp(Record record, String ip, ProcessContext context) {
        debug = context.getPropertyValue(CONFIG_DEBUG).asBoolean();
        // cacheValidityPeriodSec = (long)context.getPropertyValue(CONFIG_CACHE_MAX_TIME).asInteger();
        /**
         * Attempt to find info from the cache
         */
        IpToGeo.CacheEntry cacheEntry = null;
        try {
            cacheEntry = cacheService.get(ip);
        } catch (Exception e) {
            logger.trace("Could not use cache!");
        }
        /**
         * If something in the cache, get it and be sure it is not obsolete
         */
        Map<String, Object> geoInfo = null;
        boolean fromCache = true;
        if (cacheEntry != null) { // Something in the cache?
            geoInfo = cacheEntry.getGeoInfo();
            if (cacheValidityPeriodSec > 0) { // Cache validity period enabled?
                long cacheTime = cacheEntry.getTime();
                long now = System.currentTimeMillis();
                long cacheAge = now - cacheTime;
                if (cacheAge > (cacheValidityPeriodSec * 1000L)) { // Cache entry older than allowed max age?
                    geoInfo = null; // Cache entry expired, force triggering a new request
                }
            }
        }

        if (geoInfo == null) {
            fromCache = false;
            /**
             * Not in the cache or cache entry expired
             * Call the Ip to Geo service and fill responses as new fields
             */
            geoInfo = ipToGeoService.getGeoInfo(ip);

            /**
             * Remove unwanted fields if some specific fields configured
             */
            if (!allFields)
            {
                try {
                    filterFields(geoInfo);
                } catch (Exception e) {
                    logger.error(e.getMessage());
                    return;
                }
            }
            try {
                // Store the geoInfo into the cache
                cacheEntry = new CacheEntry(geoInfo, System.currentTimeMillis());
                cacheService.set(ip, cacheEntry);
             } catch (Exception e) {
            logger.trace("Could not put entry in the cache:" + e.getMessage());
            }
        }

        final String ipAttributeName = context.getProperty(IP_ADDRESS_FIELD);

        if (hierarchical)
        {
            /**
             * Add the geo fields under a father field named <ip_field><hierarchical_suffix>:
             * Let's say the ip field is src_ip, then we'll create a father field named src_ip_geo
             * under which we put all the geo fields:
             * src_ip: "123.125.42.15"
             * src_ip_geo: {
             *   geo_city: "London",
             *   geo_longitude: -0.0931,
             *   ...
             * }
             */
            record.setField(ipAttributeName + hierarchicalSuffix, FieldType.MAP, geoInfo);
            if (debug)
            {
                // Add some debug fields
                record.setField(ipAttributeName + hierarchicalSuffix + DEBUG_FROM_CACHE_SUFFIX, FieldType.BOOLEAN, fromCache);
            }
        } else
        {
            /**
             * Add the geo fields as fields whose names are derived from the ip field:
             * <ip_field><flat_suffix>_geo_city, <ip_field><flat_suffix>_geo_longitude....
             */
            for (Map.Entry<String, Object> entry : geoInfo.entrySet())
            {
                addRecordField(record,
                        ipAttributeName + flatSuffix + entry.getKey(),
                        entry.getKey(),
                        entry.getValue());
            }
            if (debug)
            {
                // Add some debug fields
                record.setField(ipAttributeName + flatSuffix + DEBUG_FROM_CACHE_SUFFIX, FieldType.BOOLEAN, fromCache);
            }
        }

    }

    /**
     * Filter fields returned by the Ip to Geo service according to the configured ones
     * @param geoInfo Map containing the fields returned by the Ip to Geo service
     * @throws Exception
     */
    private void filterFields(Map<String, Object> geoInfo) throws Exception
    {
        Set<String> requestedFields = getConfiguredGeoFieldNames();

        for(Iterator<Map.Entry<String, Object>> iterator = geoInfo.entrySet().iterator();
            iterator.hasNext(); ) {
            Map.Entry<String, Object> entry = iterator.next();
            String geoFieldName = entry.getKey();
            if(!requestedFields.contains(geoFieldName)) {
                if (needSubdivision || needSubdivisionIsocode)
                {
                    // Requested Subdivision or SubdivisionIsocode or Both
                    if (needSubdivision && needSubdivisionIsocode)
                    {
                        // Requested Both Subdivision and SubdivisionIsocode
                        if (!geoFieldName.startsWith(GEO_FIELD_SUBDIVISION))
                        {
                            iterator.remove();
                        }
                    } else if (needSubdivision)
                    {
                        // Requested Subdivision only
                        if (!geoFieldName.startsWith(GEO_FIELD_SUBDIVISION) ||
                                geoFieldName.startsWith(GEO_FIELD_SUBDIVISION_ISOCODE))
                        {
                            iterator.remove();
                        }
                    }
                    else
                    {
                        // Requested SubdivisionIsocode only
                        if (!geoFieldName.startsWith(GEO_FIELD_SUBDIVISION_ISOCODE))
                        {
                            iterator.remove();
                        }
                    }
                } else
                {
                    // Not a requested field, remove it
                    iterator.remove();
                }
            }
        }
    }

    /**
     * Add the provided geo field to the record
     * @param record Record to update
     * @param attributeName Geo field name
     * @param value Geo field value
     */
    private void addRecordField(Record record, String attributeName, String geoFieldName, Object value)
    {

        FieldType fieldType = supportedGeoFieldNames.get(geoFieldName);
        if (fieldType == null) // Handle subdivision and subdivision_isocode fields (geo_subdivision_0 is not geo_subdivision)
        {
            fieldType = FieldType.STRING;
        }
        record.setField(attributeName, fieldType, value);
    }

    /**
     * Cached entity
     */
    private static class CacheEntry
    {
        // geoInfo translated from the ip (or the ip if the geoInfo could not be found)
        private Map<String, Object> geoInfo = null;
        // Time at which this cache entry has been stored in the cache service
        private long time = 0L;

        public CacheEntry(Map<String, Object> geoInfo, long time)
        {
            this.geoInfo = geoInfo;
            this.time = time;
        }

        public Map<String, Object> getGeoInfo()
        {
            return geoInfo;
        }

        public long getTime()
        {
            return time;
        }
    }
}
