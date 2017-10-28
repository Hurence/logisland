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
@CapabilityDescription("Looks up geolocation information for an IP address and gets the geo information for it.")
public interface IpToGeoService extends ControllerService {

    public static final String SEPARATOR = "_";

    /**
     * The number of microseconds that the geo lookup took
     */
    public static final String GEO_FIELD_LOOKUP_TIME_MICROS = "geo" + SEPARATOR + "lookup" + SEPARATOR + "micros";
    /**
     * The city identified for the IP address
     */
    public static final String GEO_FIELD_CITY = "geo" + SEPARATOR + "city";
    /**
     * The longitude identified for the IP address
     */
    public static final String GEO_FIELD_LATITUDE = "geo" + SEPARATOR + "latitude";
    /**
     * The longitude identified for the IP address
     */
    public static final String GEO_FIELD_LONGITUDE = "geo" + SEPARATOR + "longitude";
    /**
     * The location identified for the IP address, defined as Geo-point expressed as a string with the format: "lat,lon"
     */
    public static final String GEO_FIELD_LOCATION = "geo" + SEPARATOR + "location";
    /**
     * Each subdivision that is identified for the IP address is added with a one-up number
     * appended to the attribute name, starting with 0
     */
    public static final String GEO_FIELD_SUBDIVISION = "geo" + SEPARATOR + "subdivision" + SEPARATOR;
    /**
     * The ISO code for the subdivision that is identified by GEO_FIELD_SUBDIVISION
     */
    public static final String GEO_FIELD_SUBDIVISION_ISOCODE = "geo" + SEPARATOR + "subdivision" + SEPARATOR + "isocode" + SEPARATOR;
    /**
     * The country identified for the IP address
     */
    public static final String GEO_FIELD_COUNTRY = "geo" + SEPARATOR + "country";
    /**
     * The ISO Code for the identified country
     */
    public static final String GEO_FIELD_COUNTRY_ISOCODE = "geo" + SEPARATOR + "country" + SEPARATOR + "isocode";
    /**
     * The postal code for the identified country
     */
    public static final String GEO_FIELD_POSTALCODE = "geo" + SEPARATOR + "postalcode";

    /**
     * Gets geo informations matching the passed ip address.
     * @param ip Ip to search
     * @return A Map containing matching geo information for the passed IP (if found). Possible keys a defined in the
     * IpToGeoService service as static fields starting with the GEO_FIELD prefix.
     */
    public Map<String, String> getGeoInfo(String ip);
}
