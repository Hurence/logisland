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
package com.hurence.logisland.service.iptogeo.maxmind;

import com.hurence.logisland.service.iptogeo.IpToGeoService;
import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.annotation.lifecycle.OnEnabled;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.AbstractControllerService;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.validator.StandardValidators;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.Subdivision;
import com.hurence.logisland.component.PropertyValue;

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

    protected String dbUri = null;
    protected String dbPath = null;

    final AtomicReference<DatabaseReader> databaseReaderRef = new AtomicReference<>(null);

    @Override
    @OnEnabled
    public void init(ControllerServiceInitializationContext context) throws InitializationException {
        try {
            // Custom validator insures only one of both modes is set

            PropertyValue propertyValue = context.getPropertyValue(MAXMIND_DATABASE_FILE_URI);
            if (propertyValue != null) {
                dbUri = propertyValue.asString();
            }

            propertyValue = context.getPropertyValue(MAXMIND_DATABASE_FILE_PATH);
            if (propertyValue != null) {
                dbPath = propertyValue.asString();
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
        } catch (Exception e){
            getLogger().error("Could not load maxmind database file: {}", new Object[]{e.getMessage()});
            throw new InitializationException(e);
        }
    }

    private void initFromPath(String dbPath) throws Exception
    {
        final File dbFile = new File(dbPath);
        long start = System.currentTimeMillis();
        final DatabaseReader databaseReader = new DatabaseReader.Builder(dbFile).build();
        getLogger().info("Reading Maxmind DB file from local filesystem at: " + dbFile.getAbsolutePath());
        long stop = System.currentTimeMillis();
        getLogger().info("Completed loading of Maxmind Geo Database in {} milliseconds.", new Object[]{stop - start});
        databaseReaderRef.set(databaseReader);
    }

    private void initFromUri(String dbUri) throws Exception
    {
        Configuration conf = new Configuration();

        String hdfsUri = conf.get("fs.defaultFS");
        getLogger().info("Default HDFS URI: " + hdfsUri);

        // Set HADOOP user to same as current suer
        String hadoopUser = System.getProperty("user.name");
        System.setProperty("HADOOP_USER_NAME", hadoopUser);
        System.setProperty("hadoop.home.dir", "/");

        // Get the HDFS filesystem
        FileSystem fs = FileSystem.get(URI.create(hdfsUri), conf);

        // Create a path to config file and init input stream
        Path hdfsReadpath = new Path(dbUri);
        getLogger().info("Reading Maxmind DB file from HDFS at: " + dbUri);
        FSDataInputStream inputStream = fs.open(hdfsReadpath);

        long start = System.currentTimeMillis();
        final DatabaseReader databaseReader = new DatabaseReader.Builder(inputStream).build();
        long stop = System.currentTimeMillis();
        getLogger().info("Completed loading of Maxmind Geo Database in {} milliseconds.", new Object[]{stop - start});
        databaseReaderRef.set(databaseReader);
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(MAXMIND_DATABASE_FILE_URI);
        props.add(MAXMIND_DATABASE_FILE_PATH);
        return Collections.unmodifiableList(props);
    }

    /**
     * Gets geo informations matching the passed ip address.
     * @param ip Ip to search
     * @return A Map containing matching geo information for the passed IP (if found). Possible keys a defined in the
     * IpToGeoService service as static fields starting with the GEO_FIELD prefix.
     */
    public Map<String, String> getGeoInfo(String ip)
    {
        Map<String, String> result = new HashMap<String, String>();

        final DatabaseReader dbReader = databaseReaderRef.get();

        CityResponse response = null;
        InetAddress inetAddress = null;

        try {
            inetAddress = InetAddress.getByName(ip);
        } catch (final IOException ioe) {
            getLogger().error("Could not resolve to ip address for {}", new Object[]{ip});
            return result;
        }

        long start = System.currentTimeMillis();
        try {
            response = dbReader.city(inetAddress);
        } catch (final IOException | GeoIp2Exception ex) {
            getLogger().error("Could not find geo data for {} ({}) due to {}", new Object[]{inetAddress, ip, ex.getMessage()});
            return result;
        }
        long stop = System.currentTimeMillis();

        if (response == null) {
            getLogger().error("Could not find geo data for {} due to null result", new Object[]{ip});
            return result;
        }

        result.put(GEO_FIELD_LOOKUP_TIME_MICROS, (new Long((stop - start)*1000L)).toString());

        result.put(GEO_FIELD_CITY, response.getCity().getName());
        result.put(GEO_FIELD_LATITUDE, response.getLocation().getLatitude().toString());
        result.put(GEO_FIELD_LONGITUDE, response.getLocation().getLongitude().toString());

        int i = 0;
        for (final Subdivision subd : response.getSubdivisions()) {
            result.put(GEO_FIELD_SUBDIVISION + i, subd.getName());
            result.put(GEO_FIELD_SUBDIVISION_ISOCODE + i, subd.getIsoCode());
            i++;
        }

        result.put(GEO_FIELD_COUNTRY, response.getCountry().getName());
        result.put(GEO_FIELD_COUNTRY_ISOCODE, response.getCountry().getIsoCode());
        result.put(GEO_FIELD_POSTALCODE, response.getPostal().getCode());

        return result;
    }
}
