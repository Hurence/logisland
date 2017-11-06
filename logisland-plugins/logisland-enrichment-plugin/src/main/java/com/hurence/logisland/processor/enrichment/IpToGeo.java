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
import com.hurence.logisland.annotation.behavior.WritesAttribute;
import com.hurence.logisland.annotation.behavior.WritesAttributes;
import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.component.PropertyValue;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.service.iptogeo.IpToGeoService;
import com.hurence.logisland.validator.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@Tags({"geo", "enrich", "ip"})
@CapabilityDescription("Looks up geolocation information for an IP address. The attribute that contains the IP address to lookup is provided by the "
        + "'IP Address Attribute' property. If the name of the attribute provided is 'X', then the the attributes added by enrichment "
        + "will take the form X_geo_<fieldName>")
@WritesAttributes({
        @WritesAttribute(attribute = "lookup_micros", description = "The number of microseconds that the geo lookup took"),
        @WritesAttribute(attribute = "continent", description = "The continent identified for this IP address"),
        @WritesAttribute(attribute = "continent_code", description = "The continent code identified for this IP address"),
        @WritesAttribute(attribute = "city", description = "The city identified for the IP address"),
        @WritesAttribute(attribute = "latitude", description = "The latitude identified for this IP address"),
        @WritesAttribute(attribute = "longitude", description = "The longitude identified for this IP address"),
        @WritesAttribute(attribute = "location", description = "The location identified for this IP address, defined as Geo-point expressed as a string with the format: \"lat,lon\""),
        @WritesAttribute(attribute = "accuracy_radius", description = "The approximate accuracy radius, in kilometers, around the latitude and longitude for the location"),
        @WritesAttribute(attribute = "time_zone", description = "The time zone identified for this IP address"),
        @WritesAttribute(attribute = "subdivision_N",
                description = "Each subdivision that is identified for this IP address is added with a one-up number appended to the attribute name, starting with 0"),
        @WritesAttribute(attribute = "subdivision_isocode_N", description = "The ISO code for the subdivision that is identified by X_subdivision_N"),
        @WritesAttribute(attribute = "country", description = "The country identified for this IP address"),
        @WritesAttribute(attribute = "country_isocode", description = "The ISO Code for the country identified"),
        @WritesAttribute(attribute = "postalcode", description = "The postal code for the country identified"),})
public class IpToGeo extends IpAbstractProcessor {

    private static Logger logger = LoggerFactory.getLogger(IpToGeo.class);
    private boolean debug = false;

    public static final PropertyDescriptor IP_TO_GEO_SERVICE = new PropertyDescriptor.Builder()
            .name("iptogeo.service")
            .displayName("The IP to Geo service to use")
            .description("The reference to the IP to Geo service to use.")
            .required(true)
            .identifiesControllerService(IpToGeoService.class)
            .build();

    public static final PropertyDescriptor GEO_FIELDS = new PropertyDescriptor.Builder()
            .name("geo.fields")
            .displayName("List of geo information fields to add")
            .description("Comma separated list of geo information fields to add to the record. * for all available. If a list" +
                    " of fields is specified and the data is not available, the geo field is not created.")
            .required(false)
            .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
            .defaultValue("*")
            .build();

    public static final PropertyDescriptor HIERARCHICAL = new PropertyDescriptor.Builder()
            .name("geo.hierarchical")
            .displayName("Add geo fields under a hierarchical attribute or not.")
            .description("Should the additional geo information fields be added under a hierarchical father field or not.")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor HIERARCHICAL_SUFFIX = new PropertyDescriptor.Builder()
            .name("geo.hierarchical.suffix")
            .displayName("Suffix to use for the field holding geo information")
            .description("If hierarchical is true, then use this suffix appended to the ip field name to define the father" +
                    " field name.")
            .required(false)
            .defaultValue("_geo")
            .build();

    public static final PropertyDescriptor FLAT_SUFFIX = new PropertyDescriptor.Builder()
            .name("geo.flat.suffix")
            .displayName("Suffix to use for geo information fields when they are flat")
            .description("If hierarchical is false, then use this suffix appended to the ip field name but before the geo field name." +
                    " This may be used for instance to distinguish between geo fields with various locales using many ip to geo service instances.")
            .required(false)
            .defaultValue("_geo_")
            .build();

    // IP to GEO service to use to perform the translation requests
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

        /**
         * Call the Ip to Geo service and fill responses as new fields
         */
        Map<String, Object> geoInfo = ipToGeoService.getGeoInfo(ip);

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
        }
    }

    /**
     * Filter fields returned by the IP to GEO service according to the configured ones
     * @param geoInfo Map containing the fields returned by the IP to GEO service
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
}
