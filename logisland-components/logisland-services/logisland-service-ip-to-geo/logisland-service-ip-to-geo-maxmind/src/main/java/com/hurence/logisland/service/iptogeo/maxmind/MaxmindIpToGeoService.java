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
package com.hurence.logisland.service.iptogeo.maxmind;

import com.hurence.logisland.annotation.documentation.Category;
import com.hurence.logisland.annotation.documentation.ComponentCategory;
import com.hurence.logisland.service.iptogeo.IpToGeoService;
import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.annotation.lifecycle.OnEnabled;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.AbstractControllerService;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.validator.StandardValidators;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.*;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.net.URI;

@Category(ComponentCategory.ENRICHMENT)
@Tags({"ip", "service", "geo", "maxmind"})
@CapabilityDescription("Implementation of the IP 2 GEO Service using maxmind lite db file")
public class MaxmindIpToGeoService extends AbstractControllerService implements IpToGeoService {

    public static final PropertyDescriptor MAXMIND_DATABASE_FILE_URI = new PropertyDescriptor.Builder()
            .name("maxmind.database.uri")
            .displayName("URL to the Maxmind Geo Database File")
            .description("Path to the Maxmind Geo Enrichment Database File.")
            .required(false)
            .addValidator(StandardValidators.URI_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAXMIND_DATABASE_FILE_PATH = new PropertyDescriptor.Builder()
            .name("maxmind.database.path")
            .displayName("Local path to the Maxmind Geo Database File")
            .description("Local Path to the Maxmind Geo Enrichment Database File.")
            .required(false)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    public static final PropertyDescriptor LOCALE = new PropertyDescriptor.Builder()
            .name("locale")
            .displayName("Locale to use for geo information")
            .description("Locale to use for geo information. Defaults to 'en'.")
            .required(false)
            .defaultValue("en")
            .build();

    public static final PropertyDescriptor LOOKUP_TIME = new PropertyDescriptor.Builder()
            .name("lookup.time")
            .displayName("Add a " + GEO_FIELD_LOOKUP_TIME_MICROS + " field giving the lookup time in microseconds")
            .description("Should the additional " + GEO_FIELD_LOOKUP_TIME_MICROS + " field be returned or not.")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .build();

    protected String dbUri = null;
    protected String dbPath = null;
    protected String locale = "en";
    protected boolean lookupTime = false;

    final AtomicReference<DatabaseReader> databaseReaderRef = new AtomicReference<>(null);

    @Override
    @OnEnabled
    public void init(ControllerServiceInitializationContext context) throws InitializationException {
        super.init(context);
        try {

            if (context.getPropertyValue(MAXMIND_DATABASE_FILE_URI).isSet()) {
                dbUri = context.getPropertyValue(MAXMIND_DATABASE_FILE_URI).asString();
            }

            if (context.getPropertyValue(MAXMIND_DATABASE_FILE_PATH).isSet()) {
                dbPath = context.getPropertyValue(MAXMIND_DATABASE_FILE_PATH).asString();
            }

            if ( (dbUri == null) && (dbPath == null) ) {
                throw new Exception("You must declare " + MAXMIND_DATABASE_FILE_URI.getName() +
                        " or " + MAXMIND_DATABASE_FILE_PATH.getName());
            }

            if (dbUri != null)
            {
                initFromUri(dbUri);
            }

            if (dbPath != null)
            {
                initFromPath(dbPath);
            }

            if (context.getPropertyValue(LOCALE).isSet()) {
                locale = context.getPropertyValue(LOCALE).asString();
            }

            if (context.getPropertyValue(LOOKUP_TIME).isSet()) {
                lookupTime = context.getPropertyValue(LOOKUP_TIME).asBoolean();
            }
        } catch (Exception e){
            getLogger().error("Could not load maxmind database file: {}", new Object[]{e.getMessage()});
            throw new InitializationException(e);
        }
    }

    private void initFromPath(String dbPath) throws Exception
    {
        final File dbFile = new File(dbPath);
        long start = System.currentTimeMillis();
        final DatabaseReader databaseReader = createReader(new DatabaseReader.Builder(dbFile));
        getLogger().info("Reading Maxmind DB file from local filesystem at: " + dbFile.getAbsolutePath());
        long stop = System.currentTimeMillis();
        getLogger().info("Completed loading of Maxmind Geo Database in {} milliseconds.", new Object[]{stop - start});
        databaseReaderRef.set(databaseReader);
    }

    private void initFromUri(String dbUri) throws Exception
    {
        Configuration conf = new Configuration();

        String hdfsUri = conf.get("fs.defaultFS");
        getLogger().info("Base default FS: " + hdfsUri);

        // Set HADOOP user to same as current suer
        String hadoopUser = System.getProperty("user.name");
        System.setProperty("HADOOP_USER_NAME", hadoopUser);
        System.setProperty("hadoop.home.dir", "/");

        // Get the HDFS filesystem
        FileSystem fs = FileSystem.get(URI.create(hdfsUri), conf);

        // Create a path to config file and init input stream
        Path hdfsReadpath = new Path(dbUri);
        getLogger().info("Reading Maxmind DB file from URI at: " + dbUri);
        FSDataInputStream inputStream = fs.open(hdfsReadpath);

        long start = System.currentTimeMillis();
        final DatabaseReader databaseReader = createReader(new DatabaseReader.Builder(inputStream));
        long stop = System.currentTimeMillis();
        getLogger().info("Completed loading of Maxmind Geo Database in {} milliseconds.", new Object[]{stop - start});
        databaseReaderRef.set(databaseReader);

        inputStream.close();
    }

    /**
     * Creates a DB reader with the provided builder
     * @param builder Builder to use.
     * @return The new DB reader
     */
    private DatabaseReader createReader(DatabaseReader.Builder builder) throws Exception
    {
        // Use a maxmind provided cache system with the passed capacity and use the passed locale
        return builder.locales(Arrays.asList(locale)).build();
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(MAXMIND_DATABASE_FILE_URI);
        props.add(MAXMIND_DATABASE_FILE_PATH);
        props.add(LOCALE);
        props.add(LOOKUP_TIME);
        return Collections.unmodifiableList(props);
    }

    /**
     * Gets geo informations matching the passed ip address.
     * @param ip Ip to search
     * @return A Map containing matching geo information for the passed IP (if found). Possible keys a defined in the
     * IpToGeoService service as static fields starting with the GEO_FIELD prefix.
     */
    public Map<String, Object> getGeoInfo(String ip)
    {
        Map<String, Object> result = new HashMap<String, Object>();

        final DatabaseReader dbReader = databaseReaderRef.get();

        CityResponse response = null;
        InetAddress inetAddress = null;

        try {
            inetAddress = InetAddress.getByName(ip);
        } catch (final IOException ioe) {
            getLogger().error("Could not resolve to ip address for {}", new Object[]{ip});
            return result;
        }


        long start = 0L;
        if (lookupTime) {
            start = System.currentTimeMillis();
        }
        try {
            response = dbReader.city(inetAddress);
        } catch (final IOException ex) {
            getLogger().error("Could not find geo data for {} ({}) due to {}", new Object[]{inetAddress, ip, ex.getMessage()});
            return result;
        } catch (final GeoIp2Exception ex) {
            // Mainly when the address is not in the database
            getLogger().debug("Could not find geo data for {} ({}) due to {}", new Object[]{inetAddress, ip, ex.getMessage()});
            return result;
        }
        long stop = 0L;
        if (lookupTime) {
            stop = System.currentTimeMillis();
        }

        if (response == null) {
            getLogger().debug("Could not find geo data for {} due to null result", new Object[]{ip});
            return result;
        }

        if (lookupTime) {
            result.put(GEO_FIELD_LOOKUP_TIME_MICROS, (int) ((stop - start) * 1000L));
        }

        // Continent
        Continent continent = response.getContinent();
        if (continent != null) {
            String continentName = continent.getName();
            if (continentName != null) {
                result.put(GEO_FIELD_CONTINENT, continentName);
            }
            String continentCode = continent.getCode();
            if (continentCode != null) {
                result.put(GEO_FIELD_CONTINENT_CODE, continentCode);
            }
        }

        // City
        City city = response.getCity();
        if (city != null) {
            String cityName = city.getName();
            if (cityName != null) {
                result.put(GEO_FIELD_CITY, cityName);
            }
        }

        // Location
        Location location = response.getLocation();
        if (location != null) {
            Double latitude = location.getLatitude();
            if (latitude != null) {
                result.put(GEO_FIELD_LATITUDE, latitude);
            }
            Double longitude = location.getLongitude();
            if (longitude != null) {
                result.put(GEO_FIELD_LONGITUDE, longitude);
            }
            if ((latitude != null) &&  (longitude != null)) {
                String geopoint_location = latitude.toString() + "," + longitude.toString();
                result.put(GEO_FIELD_LOCATION, geopoint_location);
            }
            Integer accuracyRadius = location.getAccuracyRadius();
            if (accuracyRadius != null) {
                result.put(GEO_FIELD_ACCURACY_RADIUS, accuracyRadius);
            }
            String timeZone = location.getTimeZone();
            if (timeZone != null) {
                result.put(GEO_FIELD_TIME_ZONE, timeZone);
            }
        }

        // Subdivisions
        List<Subdivision> subdivisions = response.getSubdivisions();
        if (subdivisions != null) {
            int i = 0;
            for (final Subdivision subd : subdivisions) {
                String subdName = subd.getName();
                if (subdName != null) {
                    result.put(GEO_FIELD_SUBDIVISION + SEPARATOR + i, subdName);
                }
                String subdIsoCode = subd.getIsoCode();
                if (subdIsoCode != null) {
                    result.put(GEO_FIELD_SUBDIVISION_ISOCODE + SEPARATOR + i, subdIsoCode);
                }
                i++;
            }
        }

        // Country
        Country country = response.getCountry();
        if (country != null) {
            String countryName = country.getName();
            if (countryName != null) {
                result.put(GEO_FIELD_COUNTRY, countryName);
            }
            String countryIsoCode = country.getIsoCode();
            if (countryIsoCode != null) {
                result.put(GEO_FIELD_COUNTRY_ISOCODE, countryIsoCode);
            }
        }

        // Postal code
        Postal postal = response.getPostal();
        if (postal != null) {
            String postalCode = postal.getCode();
            if (postalCode != null) {
                result.put(GEO_FIELD_POSTALCODE, postalCode);
            }
        }

        return result;
    }
}
