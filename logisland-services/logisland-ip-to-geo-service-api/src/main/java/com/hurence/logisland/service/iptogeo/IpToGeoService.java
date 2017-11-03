/**
 * Copyright (C) 2017 Hurence (support@hurence.com)
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
package com.hurence.logisland.service.iptogeo;

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.controller.ControllerService;
import java.util.Map;

@Tags({"ip", "service", "geo"})
@CapabilityDescription("Looks up geolocation information for an IP address and gets the geo information for it. An IP address" +
        " is passed as parameter to the getGeoInfo method. The returned map is filled with found information with the fields defined here" +
        " as keys.")
public interface IpToGeoService extends ControllerService {

    public static final String SEPARATOR = "_";

    public static final String PREFIX = "geo";

    /**
     * The number of microseconds that the geo lookup took
     */
    public static final String GEO_FIELD_LOOKUP_TIME_MICROS = PREFIX + SEPARATOR + "lookup" + SEPARATOR + "micros";
    /**
     * The continent identified for the IP address
     */
    public static final String GEO_FIELD_CONTINENT = PREFIX + SEPARATOR + "continent";
    /**
     * The continent code identified for the IP address
     */
    public static final String GEO_FIELD_CONTINENT_CODE = PREFIX + SEPARATOR + "continent" + SEPARATOR + "code";
    /**
     * The city identified for the IP address
     */
    public static final String GEO_FIELD_CITY = PREFIX + SEPARATOR + "city";
    /**
     * The longitude identified for the IP address
     */
    public static final String GEO_FIELD_LATITUDE = PREFIX + SEPARATOR + "latitude";
    /**
     * The longitude identified for the IP address
     */
    public static final String GEO_FIELD_LONGITUDE = PREFIX + SEPARATOR + "longitude";
    /**
     * The location identified for the IP address, defined as Geo-point expressed as a string with the format: "lat,lon"
     */
    public static final String GEO_FIELD_LOCATION = PREFIX + SEPARATOR + "location";
    /**
     * The timezone for the IP address
     */
    public static final String GEO_FIELD_TIME_ZONE = PREFIX + SEPARATOR + "time"  + SEPARATOR + "zone";
    /**
     * The approximate accuracy radius, in kilometers, around the latitude and longitude for the geographical entity
     */
    public static final String GEO_FIELD_ACCURACY_RADIUS = PREFIX + SEPARATOR + "accuracy" + SEPARATOR + "radius";
    /**
     * Each subdivision that is identified for the IP address is added with a one-up number
     * appended to the attribute name, starting with 0
     */
    public static final String GEO_FIELD_SUBDIVISION = PREFIX + SEPARATOR + "subdivision";
    /**
     * The ISO code for the subdivision that is identified by GEO_FIELD_SUBDIVISION
     */
    public static final String GEO_FIELD_SUBDIVISION_ISOCODE = PREFIX + SEPARATOR + "subdivision" + SEPARATOR + "isocode";
    /**
     * The country identified for the IP address
     */
    public static final String GEO_FIELD_COUNTRY = PREFIX + SEPARATOR + "country";
    /**
     * The ISO Code for the identified country
     */
    public static final String GEO_FIELD_COUNTRY_ISOCODE = PREFIX + SEPARATOR + "country" + SEPARATOR + "isocode";
    /**
     * The postal code for the identified country
     */
    public static final String GEO_FIELD_POSTALCODE = PREFIX + SEPARATOR + "postalcode";

    /**
     * Gets geo informations matching the passed ip address.
     * @param ip Ip to search
     * @return A Map containing matching geo information for the passed IP (if found). Possible keys a defined in the
     * IpToGeoService service as static fields starting with the GEO_FIELD prefix.
     */
    public Map<String, Object> getGeoInfo(String ip);
}
