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
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.service.iptogeo.IpToGeoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

@Tags({"geo", "enrich", "ip"})
@CapabilityDescription("Looks up geolocation information for an IP address and adds the geo information to FlowFile attributes. The "
        + "geo data is provided as a MaxMind database. The attribute that contains the IP address to lookup is provided by the "
        + "'IP Address Attribute' property. If the name of the attribute provided is 'X', then the the attributes added by enrichment "
        + "will take the form X_geo_<fieldName>")
@WritesAttributes({
        @WritesAttribute(attribute = "X_geo_lookup_micros", description = "The number of microseconds that the geo lookup took"),
        @WritesAttribute(attribute = "X_geo_city", description = "The city identified for the IP address"),
        @WritesAttribute(attribute = "X_geo_latitude", description = "The latitude identified for this IP address"),
        @WritesAttribute(attribute = "X_geo_longitude", description = "The longitude identified for this IP address"),
        @WritesAttribute(attribute = "X_geo_location", description = "The location identified for this IP address, defined as Geo-point expressed as a string with the format: \"lat,lon\""),
        @WritesAttribute(attribute = "X_geo_subdivision_N",
                description = "Each subdivision that is identified for this IP address is added with a one-up number appended to the attribute name, starting with 0"),
        @WritesAttribute(attribute = "X_geo_subdivision_isocode_N", description = "The ISO code for the subdivision that is identified by X_geo_subdivision_N"),
        @WritesAttribute(attribute = "X_geo_country", description = "The country identified for this IP address"),
        @WritesAttribute(attribute = "X_geo_country_isocode", description = "The ISO Code for the country identified"),
        @WritesAttribute(attribute = "X_geo_postalcode", description = "The postal code for the country identified"),})
public class IpToGeo extends IpAbstractProcessor {

    private static Logger logger = LoggerFactory.getLogger(IpToGeo.class);
    private boolean debug = false;

    public static final PropertyDescriptor IP_TO_GEO_SERVICE = new PropertyDescriptor.Builder()
            .name("iptogeo.service")
            .description("The IP to Geo service to use.")
            .required(true)
            .identifiesControllerService(IpToGeoService.class)
            .build();

    private IpToGeoService ipToGeoService = null;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = super.getSupportedPropertyDescriptors();
        properties.add(IP_TO_GEO_SERVICE);
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
    }

    protected void processIp(Record record, String ip, ProcessContext context) {

        /**
         * Call the Ip to Geo service and fill responses as new fields
         */
        Map<String, String> geoInfo = ipToGeoService.getGeoInfo(ip);

        final String ipAttributeName = context.getProperty(IP_ADDRESS_FIELD);

        String value = geoInfo.get(GEO_FIELD_LOOKUP_TIME_MICROS);
        if (value != null)
        {
            record.setField(
                    new StringBuilder(ipAttributeName).append(SEPARATOR).append(GEO_FIELD_LOOKUP_TIME_MICROS).toString(),
                    FieldType.STRING,
                    value);
        }

        value = geoInfo.get(GEO_FIELD_CITY);
        if (value != null)
        {
            record.setField(
                    new StringBuilder(ipAttributeName).append(SEPARATOR).append(GEO_FIELD_CITY).toString(),
                    FieldType.STRING,
                    value);
        }

        value = geoInfo.get(GEO_FIELD_LATITUDE);
        if (value != null)
        {
            record.setField(
                    new StringBuilder(ipAttributeName).append(SEPARATOR).append(GEO_FIELD_LATITUDE).toString(),
                    FieldType.STRING,
                    value);
        }


        value = geoInfo.get(GEO_FIELD_LONGITUDE);
        if (value != null)
        {
            record.setField(
                    new StringBuilder(ipAttributeName).append(SEPARATOR).append(GEO_FIELD_LONGITUDE).toString(),
                    FieldType.STRING,
                    value);
        }

        value = geoInfo.get(GEO_FIELD_LOCATION);
        if (value != null)
        {
            record.setField(
                    new StringBuilder(ipAttributeName).append(SEPARATOR).append(GEO_FIELD_LOCATION).toString(),
                    FieldType.STRING,
                    value);
        }

        int i = 0;
        while(true)
        {
            value = geoInfo.get(GEO_FIELD_SUBDIVISION + i);
            if (value != null)
            {
                record.setField(
                        new StringBuilder(ipAttributeName).append(SEPARATOR).append(GEO_FIELD_SUBDIVISION + i).toString(),
                        FieldType.STRING,
                        value);
            } else
            {
                break;
            }
            value = geoInfo.get(GEO_FIELD_SUBDIVISION_ISOCODE + i);
            if (value != null)
            {
                record.setField(
                        new StringBuilder(ipAttributeName).append(SEPARATOR).append(GEO_FIELD_SUBDIVISION_ISOCODE + i).toString(),
                        FieldType.STRING,
                        value);
            }
            i++;
        }

        value = geoInfo.get(GEO_FIELD_COUNTRY);
        if (value != null)
        {
            record.setField(
                    new StringBuilder(ipAttributeName).append(SEPARATOR).append(GEO_FIELD_COUNTRY).toString(),
                    FieldType.STRING,
                    value);
        }

        value = geoInfo.get(GEO_FIELD_COUNTRY_ISOCODE);
        if (value != null)
        {
            record.setField(
                    new StringBuilder(ipAttributeName).append(SEPARATOR).append(GEO_FIELD_COUNTRY_ISOCODE).toString(),
                    FieldType.STRING,
                    value);
        }

        value = geoInfo.get(GEO_FIELD_POSTALCODE);
        if (value != null)
        {
            record.setField(
                    new StringBuilder(ipAttributeName).append(SEPARATOR).append(GEO_FIELD_POSTALCODE).toString(),
                    FieldType.STRING,
                    value);
        }
    }
}
