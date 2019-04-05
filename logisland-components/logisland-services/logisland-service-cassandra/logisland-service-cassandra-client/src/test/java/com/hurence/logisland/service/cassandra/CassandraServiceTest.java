/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
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
package com.hurence.logisland.service.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.service.cassandra.RecordConverter.CassandraType;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.*;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;

import static com.hurence.logisland.service.cassandra.CassandraControllerService.END_OF_TEST;
import static org.junit.Assert.assertEquals;

@RunWith(DataProviderRunner.class)
@Ignore
public class CassandraServiceTest {

    // Embedded cassandra server maven plugin instance connect info

    private final static String CASSANDRA_HOST = "localhost";
    private final static String CASSANDRA_PORT = "19042";

    // Use these ones instead if you want to use the "docker cassandra start" instead of embedded cassandra server maven plugin instance
    // Or use "mvn -DfailIfNoTests=false [clean] test -Dtest=CassandraServiceTest" If want to use embedded
    // cassandra server maven plugin running only this test out of the IDE

    // If you want to run these test with IDE tou can use cassandra docker images, running it with this command :
    // sudo docker run -i -p 19042:9042 cassandra

    private final static String TEST_KEYSPACE_A = "testkeyspace_a";
    private final static String TEST_KEYSPACE_B = "testkeyspace_b";

    private static CqlSession session;

    @DataProvider
    public static Object[][] testBulkPutProvider() {

        /**
         * Table0 (simplest, text)
         *
         * testText
         *
         * hello
         * this
         * is
         * a
         * simple
         * table
         * with
         * some
         * text
         * values
         * A last one with some spaces, UPPERCASES and a dot as well as accent and special characters: &é"'(-è_çà),;:=%ù$ãẽĩõũ.
         */

        String tableName0 = TEST_KEYSPACE_A + ".table0";
        String createTableCql0 = "CREATE TABLE IF NOT EXISTS " + tableName0
                + " (testText text PRIMARY KEY);";

        List<Map<Field, CassandraType>> table0 = new ArrayList<Map<Field, CassandraType>>();
        Map<Field, CassandraType> row = new HashMap<Field, CassandraType>();

        row.put(new Field("testText", FieldType.STRING, "hello"), CassandraType.TEXT);
        table0.add(row);

        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testText", FieldType.STRING, "this"), CassandraType.TEXT);
        table0.add(row);

        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testText", FieldType.STRING, "is"), CassandraType.TEXT);
        table0.add(row);

        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testText", FieldType.STRING, "a"), CassandraType.TEXT);
        table0.add(row);

        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testText", FieldType.STRING, "simple"), CassandraType.TEXT);
        table0.add(row);

        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testText", FieldType.STRING, "table"), CassandraType.TEXT);
        table0.add(row);

        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testText", FieldType.STRING, "with"), CassandraType.TEXT);
        table0.add(row);

        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testText", FieldType.STRING, "some"), CassandraType.TEXT);
        table0.add(row);

        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testText", FieldType.STRING, "text"), CassandraType.TEXT);
        table0.add(row);

        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testText", FieldType.STRING, "values"), CassandraType.TEXT);
        table0.add(row);

        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testText", FieldType.STRING, "A last one with some spaces, UPPERCASES and a dot as well as accent and special characters: &é\"'(-è_çà),;:=%ù$ãẽĩõũâêîôû."), CassandraType.TEXT);
        table0.add(row);

        /**
         * Table1 (simple, simple primary key, uuid)
         *
         * testUuid                             testInt
         *
         * d6328472-b571-4f61-a82e-fe4344228291 215461
         * d6328472-b571-4f61-a82e-fe4344228292 215462
         */

        String tableName1 = TEST_KEYSPACE_A + ".table1";
        String createTableCql1 = "CREATE TABLE IF NOT EXISTS " + tableName1
                + " (testUuid uuid PRIMARY KEY, testInt int);";

        List<Map<Field, CassandraType>> table1 = new ArrayList<Map<Field, CassandraType>>();
        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testUuid", FieldType.STRING, "d6328472-b571-4f61-a82e-fe4344228291"), CassandraType.UUID);
        row.put(new Field("testInt", FieldType.INT, 215461), CassandraType.INT);
        table1.add(row);

        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testUuid", FieldType.STRING, "d6328472-b571-4f61-a82e-fe4344228292"), CassandraType.UUID);
        row.put(new Field("testInt", FieldType.INT, 215462), CassandraType.INT);
        table1.add(row);

        /**
         * Table2 (simple, composite primary key)
         *
         * testUuid                             testInt testFloat     testSmallint
         *
         * d6328472-b571-4f61-a82e-fe4344228291 215461  123.456       12546
         * d6328472-b571-4f61-a82e-fe4344228292 215462  456789.123456 -4568
         */

        String tableName2 = TEST_KEYSPACE_A + ".table2";
        String createTableCql2 = "CREATE TABLE IF NOT EXISTS " + tableName2
                + " (testUuid uuid, testInt int, testFloat float, testSmallint smallint, PRIMARY KEY (testUuid,testInt,testSmallint));";

        List<Map<Field, CassandraType>> table2 = new ArrayList<Map<Field, CassandraType>>();
        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testUuid", FieldType.STRING, "d6328472-b571-4f61-a82e-fe4344228291"), CassandraType.UUID);
        row.put(new Field("testInt", FieldType.INT, 215461), CassandraType.INT);
        row.put(new Field("testFloat", FieldType.FLOAT, (float) 123.456), CassandraType.FLOAT);
        row.put(new Field("testSmallint", FieldType.INT, 12546), CassandraType.SMALLINT);
        table2.add(row);

        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testUuid", FieldType.STRING, "d6328472-b571-4f61-a82e-fe4344228292"), CassandraType.UUID);
        row.put(new Field("testInt", FieldType.INT, 215462), CassandraType.INT);
        row.put(new Field("testFloat", FieldType.FLOAT, (float) 456789.123456), CassandraType.FLOAT);
        row.put(new Field("testSmallint", FieldType.INT, -4568), CassandraType.SMALLINT);
        table2.add(row);

        /**
         * Table3 (all integers)
         *
         * testTinyint testSmallint testInt testBigint     testVarint
         *
         * 123         12546        1563489 9623545688581  11123545688
         * -127        -4568        -954123 -8623463688247 -10128544682
         */

        String tableName3 = TEST_KEYSPACE_A + ".table3";
        String createTableCql3 = "CREATE TABLE IF NOT EXISTS " + tableName3
                + " (testTinyint tinyint PRIMARY KEY, testSmallint smallint, testInt int, testBigint bigint, testVarint varint);";

        List<Map<Field, CassandraType>> table3 = new ArrayList<Map<Field, CassandraType>>();
        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testTinyint", FieldType.INT, 123), CassandraType.TINYINT);
        row.put(new Field("testSmallint", FieldType.INT, (short) 12546), CassandraType.SMALLINT);
        row.put(new Field("testInt", FieldType.INT, 1563489), CassandraType.INT);
        row.put(new Field("testBigint", FieldType.LONG, 9623545688581L), CassandraType.BIGINT);
        row.put(new Field("testVarint", FieldType.LONG, new BigInteger("11123545688")), CassandraType.VARINT);
        table3.add(row);

        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testTinyint", FieldType.INT, -127), CassandraType.TINYINT);
        row.put(new Field("testSmallint", FieldType.INT, (short) -4568), CassandraType.SMALLINT);
        row.put(new Field("testInt", FieldType.INT, -954123), CassandraType.INT);
        row.put(new Field("testBigint", FieldType.LONG, -8623463688247L), CassandraType.BIGINT);
        row.put(new Field("testVarint", FieldType.LONG, new BigInteger("-10128544682")), CassandraType.VARINT);
        table3.add(row);

        /**
         * Table4 (all decimals)
         *
         * testFloat        testDouble              testDecimal
         *
         * 5984632.254893   14569874235.1254857623  477552233116699.4885451212353
         * -4712568.6423844 -74125448522.9985544221 -542212145454577.2151321145451
         */

        String tableName4 = TEST_KEYSPACE_B + ".table4";
        String createTableCql4 = "CREATE TABLE IF NOT EXISTS " + tableName4
                + " (testFloat float PRIMARY KEY, testDouble double, testDecimal decimal);";

        List<Map<Field, CassandraType>> table4 = new ArrayList<Map<Field, CassandraType>>();
        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testFloat", FieldType.FLOAT, (float) 5984632.254893), CassandraType.FLOAT);
        row.put(new Field("testDouble", FieldType.DOUBLE, 14569874235.1254857623), CassandraType.DOUBLE);
        row.put(new Field("testDecimal", FieldType.DOUBLE, new BigDecimal("477552233116699.4885451212353")), CassandraType.DECIMAL);
        table4.add(row);

        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testFloat", FieldType.FLOAT, (float) -4712568.6423844), CassandraType.FLOAT);
        row.put(new Field("testDouble", FieldType.DOUBLE, -74125448522.31225), CassandraType.DOUBLE);
        row.put(new Field("testDecimal", FieldType.DOUBLE, new BigDecimal("-342212145454577.24565")), CassandraType.DECIMAL);
        table4.add(row);

        /**
         * Table5 (blob and boolean)
         *
         * testTinyint testBoolean testBlob
         *
         * 1           true        this is a blob
         * -2          false       {0x0a, 0x02, 0xff}
         */

        String tableName5 = TEST_KEYSPACE_B + ".table5";
        String createTableCql5 = "CREATE TABLE IF NOT EXISTS " + tableName5
                + " (testTinyint tinyint PRIMARY KEY, testBoolean boolean, testBlob blob);";

        List<Map<Field, CassandraType>> table5 = new ArrayList<Map<Field, CassandraType>>();
        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testTinyint", FieldType.INT, 1), CassandraType.TINYINT);
        row.put(new Field("testBoolean", FieldType.BOOLEAN, true), CassandraType.BOOLEAN);
        row.put(new Field("testBlob", FieldType.BYTES, "this is a blob".getBytes()), CassandraType.BLOB);
        table5.add(row);

        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testTinyint", FieldType.INT, -2), CassandraType.TINYINT);
        row.put(new Field("testBoolean", FieldType.BOOLEAN, false), CassandraType.BOOLEAN);
        row.put(new Field("testBlob", FieldType.BYTES, new byte[]{0xa, 0x2, (byte) 0xff}), CassandraType.BLOB);
        table5.add(row);

        /**
         * Table6 (all date and times)
         *
         * testTimestamp                testDate   testTime
         *
         * 1299038700000                7892       57000000000
         * 2011-02-03T04:05:00.000+0000 2011-02-03 08:12:54.123456789
         */

        String tableName6 = TEST_KEYSPACE_B + ".table6";
        String createTableCql6 = "CREATE TABLE IF NOT EXISTS " + tableName6
                + " (testTimestamp timestamp PRIMARY KEY, testDate date, testTime time);";

        List<Map<Field, CassandraType>> table6 = new ArrayList<Map<Field, CassandraType>>();
        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testTimestamp", FieldType.LONG, 1299038700000L), CassandraType.TIMESTAMP);
        row.put(new Field("testDate", FieldType.LONG, 7892L), CassandraType.DATE);
        row.put(new Field("testTime", FieldType.LONG, 57000000000L), CassandraType.TIME);
        table6.add(row);

        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testTimestamp", FieldType.STRING, "2011-02-03T04:05:00.000+0000"), CassandraType.TIMESTAMP);
        row.put(new Field("testDate", FieldType.STRING, "2011-02-03"), CassandraType.DATE);
        row.put(new Field("testTime", FieldType.STRING, "08:12:54.523456789"), CassandraType.TIME);
        table6.add(row);

        Object[][] inputs = {
                {table0, tableName0, createTableCql0},
                {table1, tableName1, createTableCql1},
                {table2, tableName2, createTableCql2},
                {table3, tableName3, createTableCql3},
                {table4, tableName4, createTableCql4},
                {table5, tableName5, createTableCql5},
                {table6, tableName6, createTableCql6}
        };

        return inputs;
    }

    @DataProvider
    public static Object[][] test2CollectionsBulkPutProvider() {

        /**
         * Table0 (simplest, text)
         *
         * testText
         *
         * hello
         * this
         * is
         * a
         * simple
         * table
         * with
         * some
         * text
         * values
         * A last one with some spaces, UPPERCASES and a dot as well as accent and special characters: &é"'(-è_çà),;:=%ù$ãẽĩõũ.
         */

        String tableName0 = TEST_KEYSPACE_A + ".table0";
        String createTableCql0 = "CREATE TABLE IF NOT EXISTS " + tableName0
                + " (testText text PRIMARY KEY);";

        List<Map<Field, CassandraType>> table0 = new ArrayList<Map<Field, CassandraType>>();
        Map<Field, CassandraType> row = new HashMap<Field, CassandraType>();

        row.put(new Field("testText", FieldType.STRING, "hello"), CassandraType.TEXT);
        table0.add(row);

        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testText", FieldType.STRING, "this"), CassandraType.TEXT);
        table0.add(row);

        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testText", FieldType.STRING, "is"), CassandraType.TEXT);
        table0.add(row);

        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testText", FieldType.STRING, "a"), CassandraType.TEXT);
        table0.add(row);

        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testText", FieldType.STRING, "simple"), CassandraType.TEXT);
        table0.add(row);

        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testText", FieldType.STRING, "table"), CassandraType.TEXT);
        table0.add(row);

        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testText", FieldType.STRING, "with"), CassandraType.TEXT);
        table0.add(row);

        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testText", FieldType.STRING, "some"), CassandraType.TEXT);
        table0.add(row);

        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testText", FieldType.STRING, "text"), CassandraType.TEXT);
        table0.add(row);

        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testText", FieldType.STRING, "values"), CassandraType.TEXT);
        table0.add(row);

        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testText", FieldType.STRING, "A last one with some spaces, UPPERCASES and a dot as well as accent and special characters: &é\"'(-è_çà),;:=%ù$ãẽĩõũâêîôû."), CassandraType.TEXT);
        table0.add(row);

        /**
         * Table1 (simple, simple primary key, uuid)
         *
         * testUuid                             testInt
         *
         * d6328472-b571-4f61-a82e-fe4344228291 215461
         * d6328472-b571-4f61-a82e-fe4344228292 215462
         */

        String tableName1 = TEST_KEYSPACE_A + ".table1";
        String createTableCql1 = "CREATE TABLE IF NOT EXISTS " + tableName1
                + " (testUuid uuid PRIMARY KEY, testInt int);";

        List<Map<Field, CassandraType>> table1 = new ArrayList<Map<Field, CassandraType>>();
        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testUuid", FieldType.STRING, "d6328472-b571-4f61-a82e-fe4344228291"), CassandraType.UUID);
        row.put(new Field("testInt", FieldType.INT, 215461), CassandraType.INT);
        table1.add(row);

        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testUuid", FieldType.STRING, "d6328472-b571-4f61-a82e-fe4344228292"), CassandraType.UUID);
        row.put(new Field("testInt", FieldType.INT, 215462), CassandraType.INT);
        table1.add(row);

        /**
         * Table2 (simple, composite primary key)
         *
         * testUuid                             testInt testFloat     testSmallint
         *
         * d6328472-b571-4f61-a82e-fe4344228291 215461  123.456       12546
         * d6328472-b571-4f61-a82e-fe4344228292 215462  456789.123456 -4568
         */

        String tableName2 = TEST_KEYSPACE_A + ".table2";
        String createTableCql2 = "CREATE TABLE IF NOT EXISTS " + tableName2
                + " (testUuid uuid, testInt int, testFloat float, testSmallint smallint, PRIMARY KEY (testUuid,testInt,testSmallint));";

        List<Map<Field, CassandraType>> table2 = new ArrayList<Map<Field, CassandraType>>();
        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testUuid", FieldType.STRING, "d6328472-b571-4f61-a82e-fe4344228291"), CassandraType.UUID);
        row.put(new Field("testInt", FieldType.INT, 215461), CassandraType.INT);
        row.put(new Field("testFloat", FieldType.FLOAT, (float) 123.456), CassandraType.FLOAT);
        row.put(new Field("testSmallint", FieldType.INT, 12546), CassandraType.SMALLINT);
        table2.add(row);

        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testUuid", FieldType.STRING, "d6328472-b571-4f61-a82e-fe4344228292"), CassandraType.UUID);
        row.put(new Field("testInt", FieldType.INT, 215462), CassandraType.INT);
        row.put(new Field("testFloat", FieldType.FLOAT, (float) 456789.123456), CassandraType.FLOAT);
        row.put(new Field("testSmallint", FieldType.INT, -4568), CassandraType.SMALLINT);
        table2.add(row);

        /**
         * Table3 (all integers)
         *
         * testTinyint testSmallint testInt testBigint     testVarint
         *
         * 123         12546        1563489 9623545688581  11123545688
         * -127        -4568        -954123 -8623463688247 -10128544682
         */

        String tableName3 = TEST_KEYSPACE_A + ".table3";
        String createTableCql3 = "CREATE TABLE IF NOT EXISTS " + tableName3
                + " (testTinyint tinyint PRIMARY KEY, testSmallint smallint, testInt int, testBigint bigint, testVarint varint);";

        List<Map<Field, CassandraType>> table3 = new ArrayList<Map<Field, CassandraType>>();
        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testTinyint", FieldType.INT, 123), CassandraType.TINYINT);
        row.put(new Field("testSmallint", FieldType.INT, (short) 12546), CassandraType.SMALLINT);
        row.put(new Field("testInt", FieldType.INT, 1563489), CassandraType.INT);
        row.put(new Field("testBigint", FieldType.LONG, 9623545688581L), CassandraType.BIGINT);
        row.put(new Field("testVarint", FieldType.LONG, new BigInteger("11123545688")), CassandraType.VARINT);
        table3.add(row);

        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testTinyint", FieldType.INT, -127), CassandraType.TINYINT);
        row.put(new Field("testSmallint", FieldType.INT, (short) -4568), CassandraType.SMALLINT);
        row.put(new Field("testInt", FieldType.INT, -954123), CassandraType.INT);
        row.put(new Field("testBigint", FieldType.LONG, -8623463688247L), CassandraType.BIGINT);
        row.put(new Field("testVarint", FieldType.LONG, new BigInteger("-10128544682")), CassandraType.VARINT);
        table3.add(row);

        /**
         * Table4 (all decimals)
         *
         * testFloat        testDouble              testDecimal
         *
         * 5984632.254893   14569874235.1254857623  477552233116699.4885451212353
         * -4712568.6423844 -74125448522.9985544221 -542212145454577.2151321145451
         */

        String tableName4 = TEST_KEYSPACE_B + ".table4";
        String createTableCql4 = "CREATE TABLE IF NOT EXISTS " + tableName4
                + " (testFloat float PRIMARY KEY, testDouble double, testDecimal decimal);";

        List<Map<Field, CassandraType>> table4 = new ArrayList<Map<Field, CassandraType>>();
        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testFloat", FieldType.FLOAT, (float) 5984632.254893), CassandraType.FLOAT);
        row.put(new Field("testDouble", FieldType.DOUBLE, 14569874235.1254857623), CassandraType.DOUBLE);
        row.put(new Field("testDecimal", FieldType.DOUBLE, new BigDecimal("477552233116699.4885451212353")), CassandraType.DECIMAL);
        table4.add(row);

        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testFloat", FieldType.FLOAT, (float) -4712568.6423844), CassandraType.FLOAT);
        row.put(new Field("testDouble", FieldType.DOUBLE, -74125448522.31225), CassandraType.DOUBLE);
        row.put(new Field("testDecimal", FieldType.DOUBLE, new BigDecimal("-342212145454577.24565")), CassandraType.DECIMAL);
        table4.add(row);

        /**
         * Table5 (blob and boolean)
         *
         * testTinyint testBoolean testBlob
         *
         * 1           true        this is a blob
         * -2          false       {0x0a, 0x02, 0xff}
         */

        String tableName5 = TEST_KEYSPACE_B + ".table5";
        String createTableCql5 = "CREATE TABLE IF NOT EXISTS " + tableName5
                + " (testTinyint tinyint PRIMARY KEY, testBoolean boolean, testBlob blob);";

        List<Map<Field, CassandraType>> table5 = new ArrayList<Map<Field, CassandraType>>();
        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testTinyint", FieldType.INT, 1), CassandraType.TINYINT);
        row.put(new Field("testBoolean", FieldType.BOOLEAN, true), CassandraType.BOOLEAN);
        row.put(new Field("testBlob", FieldType.BYTES, "this is a blob".getBytes()), CassandraType.BLOB);
        table5.add(row);

        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testTinyint", FieldType.INT, -2), CassandraType.TINYINT);
        row.put(new Field("testBoolean", FieldType.BOOLEAN, false), CassandraType.BOOLEAN);
        row.put(new Field("testBlob", FieldType.BYTES, new byte[]{0xa, 0x2, (byte) 0xff}), CassandraType.BLOB);
        table5.add(row);

        Object[][] inputs = {
                {table0, tableName0, createTableCql0, table1, tableName1, createTableCql1},
                {table2, tableName2, createTableCql2, table3, tableName3, createTableCql3},
                {table4, tableName4, createTableCql4, table5, tableName5, createTableCql5}
        };

        return inputs;
    }

    private static void echo(String msg) {
        // Uncomment for debug
//        System.out.println(msg);
    }

    @BeforeClass
    public static void connect() {
        CqlSessionBuilder builder = CqlSession.builder();
        builder.addContactPoint(new InetSocketAddress(CASSANDRA_HOST, Integer.valueOf(CASSANDRA_PORT)));
//        builder.withLocalDatacenter("datacenter1");
//        builder.
        session = builder.build();
        echo("Connected to Cassandra");
    }

    @AfterClass
    public static void disconnect() {
        if (session != null) {
            session.close();
        }
        echo("Disconnected from Cassandra");
    }

    @Before
    public void cleanupCassandra() {
        /**
         * Delete keyspaces
         */

        StringBuffer sb = new StringBuffer("DROP KEYSPACE IF EXISTS " + TEST_KEYSPACE_A);
        String statement = sb.toString();
        ResultSet resultSet = session.execute(sb.toString());
        if (!resultSet.wasApplied()) {
            Assert.fail("Statement not applied: " + statement);
        }

        sb = new StringBuffer("DROP KEYSPACE IF EXISTS " + TEST_KEYSPACE_B);
        statement = sb.toString();
        resultSet = session.execute(sb.toString());
        if (!resultSet.wasApplied()) {
            Assert.fail("Statement not applied: " + statement);
        }

        /**
         * Create keyspaces
         */

        sb = new StringBuffer("CREATE KEYSPACE IF NOT EXISTS " + TEST_KEYSPACE_A
                + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};");
        statement = sb.toString();
        resultSet = session.execute(sb.toString());
        if (!resultSet.wasApplied()) {
            Assert.fail("Statement not applied: " + statement);
        }

        sb = new StringBuffer("CREATE KEYSPACE IF NOT EXISTS " + TEST_KEYSPACE_B
                + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};");
        statement = sb.toString();
        resultSet = session.execute(sb.toString());
        if (!resultSet.wasApplied()) {
            Assert.fail("Statement not applied: " + statement);
        }

        echo("Cassandra DB cleared and prepared");
    }

    @Test
    @UseDataProvider("testBulkPutProvider")
    public void testBulkPut(List<Map<Field, CassandraType>> insertedAndExpectedRows, String tableName, String createTableCql)
            throws InitializationException {

        /**
         * Create the table
         */
        ResultSet resultSet = session.execute(createTableCql);
        if (!resultSet.wasApplied()) {
            Assert.fail("Statement not applied: " + createTableCql);
        }


        final TestRunner runner = TestRunners.newTestRunner("com.hurence.logisland.processor.datastore.BulkPut");

        final CassandraControllerService service = new CassandraControllerService();
        runner.addControllerService("cassandra_service", service);
        runner.setProperty(service, CassandraControllerService.HOSTS.getName(), CASSANDRA_HOST);
        runner.setProperty(service, CassandraControllerService.PORT.getName(), CASSANDRA_PORT);
        runner.setProperty(service, CassandraControllerService.FLUSH_INTERVAL.getName(), "1000");
        runner.setProperty(service, CassandraControllerService.BATCH_SIZE.getName(), "500");
        runner.enableControllerService(service);

        runner.assertNotValid();

        runner.setProperty("default.collection", "just required");
        runner.setProperty("datastore.client.service", "com.hurence.logisland.processor.datastore.BulkPut");
        runner.assertValid();

        /**
         * Bulk insert records
         */
        bulkInsert(service, insertedAndExpectedRows, tableName);

        service.bulkPut(END_OF_TEST, new StandardRecord()); // Signal end of test
        service.waitForFlush();

        /**
         * Check table content
         */
        checkCassandraTable(session, insertedAndExpectedRows, tableName);

        runner.disableControllerService(service); // Disconnect service from cassandra
    }


    @Test
    @UseDataProvider("test2CollectionsBulkPutProvider")
    public void test2CollectionsBulkPut(
            List<Map<Field, CassandraType>> insertedAndExpectedRows1, String tableName1, String createTableCql1,
            List<Map<Field, CassandraType>> insertedAndExpectedRows2, String tableName2, String createTableCql2
    ) throws InitializationException {

        /**
         * Create the table 1
         */
        ResultSet resultSet = session.execute(createTableCql1);
        if (!resultSet.wasApplied()) {
            Assert.fail("Statement not applied: " + createTableCql1);
        }

        /**
         * Create the table 2
         */
        resultSet = session.execute(createTableCql2);
        if (!resultSet.wasApplied()) {
            Assert.fail("Statement not applied: " + createTableCql2);
        }


        final TestRunner runner = TestRunners.newTestRunner("com.hurence.logisland.processor.datastore.BulkPut");

        final CassandraControllerService service = new CassandraControllerService();
        runner.addControllerService("cassandra_service", service);
        runner.setProperty(service, CassandraControllerService.HOSTS.getName(), CASSANDRA_HOST);
        runner.setProperty(service, CassandraControllerService.PORT.getName(), CASSANDRA_PORT);
        runner.setProperty(service, CassandraControllerService.FLUSH_INTERVAL.getName(), "1000");
        runner.setProperty(service, CassandraControllerService.BATCH_SIZE.getName(), "500");

        runner.assertValid(service);
        runner.assertNotValid();

        runner.setProperty("default.collection", "just required");
        runner.setProperty("datastore.client.service", "com.hurence.logisland.processor.datastore.BulkPut");
        runner.assertValid();
        runner.enableControllerService(service);

        // Bulk insert records for table 1
        bulkInsert(service, insertedAndExpectedRows1, tableName1);

        // Bulk insert records for table 2
        bulkInsert(service, insertedAndExpectedRows2, tableName2);

        service.bulkPut(END_OF_TEST, new StandardRecord()); // Signal end of test
        service.waitForFlush();

        // Check table 1 content
        checkCassandraTable(session, insertedAndExpectedRows1, tableName1);

        //  Check table 2 content
        checkCassandraTable(session, insertedAndExpectedRows2, tableName2);

        runner.disableControllerService(service); // Disconnect service from cassandra
    }

    // Adds the provided list of records to the cassandra service
    private void bulkInsert(CassandraControllerService service, List<Map<Field, CassandraType>> rows, String tableName) {
        rows.forEach(
                row -> {
                    service.bulkPut(tableName, rowToRecord(row));
                }
        );
    }

    // Create a record from a map of fields
    private Record rowToRecord(Map<Field, CassandraType> row) {

        Record record = new StandardRecord();

        row.forEach(
                (field, cassandraType) -> {
                    record.setField(field);
                }
        );

        return record;
    }

    // Checks that table contains the expected lines
    private void checkCassandraTable(CqlSession session, List<Map<Field, CassandraType>> expectedRows, String tableName) {
        StringBuffer sb = new StringBuffer("SELECT * FROM " + tableName);
        String statement = sb.toString();
        ResultSet resultSet = session.execute(sb.toString());
        if (!resultSet.wasApplied()) {
            Assert.fail("Statement not applied: " + statement);
        }

        assertEquals("Number of found lines in table " + tableName + " is not equal to expected ones", expectedRows.size(), resultSet.getAvailableWithoutFetching());

        // Now check that lines are all expected ones
        Iterator<Row> iterator = resultSet.iterator();
        while (iterator.hasNext()) {
            Row row = iterator.next();
            echo("Trying to find matching expected row for row: " + row);
            findRow(row, expectedRows, tableName);
        }
    }

    // Checks that a cassandra row is in the expected ones list
    private void findRow(Row actualRow, List<Map<Field, CassandraType>> expectedRows, String tableName) {
        for (Map<Field, CassandraType> expectedRow : expectedRows) {

            // Does the returned cassandra row match this current expected one?
            try {
                FieldType fieldType;
                for (Map.Entry<Field, CassandraType> entry : expectedRow.entrySet()) {
                    Field field = entry.getKey();
                    String fieldName = field.getName();
                    CassandraType cassandraType = entry.getValue();
                    switch (cassandraType) {
                        case UUID:
                            UUID actualUuid = actualRow.getUuid(fieldName);
                            UUID expectedUuid = UUID.fromString(field.asString());
                            if (!expectedUuid.equals(actualUuid)) {
                                throw new Exception("In table " + tableName + ", uuid values for field " + fieldName +
                                        " differ for expected row: " + expectedRow + ". Cassandra uuid value is " + actualUuid);
                            }
                            break;
                        case TEXT:
                            String actualString = actualRow.getString(fieldName);
                            String expectedString = field.asString();
                            if (!expectedString.equals(actualString)) {
                                throw new Exception("In table " + tableName + ", text values for field " + fieldName +
                                        " differ for expected row: " + expectedRow + ". Cassandra text value is " + actualString);
                            }
                            break;
                        case DATE:
                            LocalDate actualDate = actualRow.getLocalDate(fieldName);
                            LocalDate expectedDate;
                            fieldType = field.getType();
                            if (fieldType == FieldType.STRING) {
                                String expectedDateString = field.asString();
                                /**
                                 * yyyy-mm-dd (so '2011-02-03')
                                 */
                                expectedDate = RecordConverter.cassandraDateToLocalDate(expectedDateString);
                            } else {
                                expectedDate = LocalDate.ofEpochDay(field.asLong().intValue());
                            }
                            if (!expectedDate.equals(actualDate)) {
                                throw new Exception("In table " + tableName + ", date values for field " + fieldName +
                                        " differ for expected row: " + expectedRow + ". Cassandra date value is " + actualDate);
                            }
                            break;
                        case TIME:
                            LocalTime actualTime = actualRow.getLocalTime(fieldName);
                            LocalTime expectedTime;
                            fieldType = field.getType();
                            if (fieldType == FieldType.STRING) {
                                String expectedTimeString = field.asString();
                                /**
                                 * hh:mm:ss[.fffffffff] (where the sub-second precision is optional and if provided, can be less than the nanosecond).
                                 * So for instance, the following are valid inputs for a time:
                                 *
                                 *     '08:12:54'
                                 *     '08:12:54.123'
                                 *     '08:12:54.123456'
                                 *     '08:12:54.123456789'
                                 */
                                expectedTime = RecordConverter.cassandraTimeToNanosecondsSinceMidnight(expectedTimeString);
                            } else {
                                expectedTime = LocalTime.ofNanoOfDay(field.asLong());
                            }
                            if (!expectedTime.equals(actualTime)) {
                                throw new Exception("In table " + tableName + ", time values for field " + fieldName +
                                        " differ for expected row: " + expectedRow + ". Cassandra time value is " + actualTime);
                            }
                            break;
                        case TIMESTAMP:
                            Instant actualTimestamp = actualRow.getInstant(fieldName);
                            Instant expectedTimestamp;
                            fieldType = field.getType();
                            if (fieldType == FieldType.STRING) {
                                String expectedTimestampString = field.asString();
                                /**
                                 * String that represents an ISO 8601 date. For instance, all of the values below are valid timestamp values for Mar 2, 2011, at 04:05:00 AM, GMT:
                                 *
                                 *     1299038700000
                                 *     '2011-02-03 04:05+0000'
                                 *     '2011-02-03 04:05:00+0000'
                                 *     '2011-02-03 04:05:00.000+0000'
                                 *     '2011-02-03T04:05+0000'
                                 *     '2011-02-03T04:05:00+0000'
                                 *     '2011-02-03T04:05:00.000+0000'
                                 */
                                expectedTimestamp = RecordConverter.cassandraTimestampToDate(expectedTimestampString);
                            } else {
                                expectedTimestamp = Instant.ofEpochMilli(field.asLong());
                            }
                            if (!expectedTimestamp.equals(actualTimestamp)) {
                                throw new Exception("In table " + tableName + ", timestamp values for field " + fieldName +
                                        " differ for expected row: " + expectedRow + ". Cassandra timestamp value is " + actualTimestamp);
                            }
                            break;
                        case TINYINT:
                            Byte actualByte = actualRow.getByte(fieldName);
                            Byte expectedByte = new Byte(field.asInteger().toString());
                            if (!expectedByte.equals(actualByte)) {
                                throw new Exception("In table " + tableName + ", tinyint values for field " + fieldName +
                                        " differ for expected row: " + expectedRow + ". Cassandra tinyint value is " + actualByte);
                            }
                            break;
                        case SMALLINT:
                            Short actualShort = actualRow.getShort(fieldName);
                            Short expectedShort = new Short(field.asInteger().toString());
                            if (!expectedShort.equals(actualShort)) {
                                throw new Exception("In table " + tableName + ", smallint values for field " + fieldName +
                                        " differ for expected row: " + expectedRow + ". Cassandra smallint value is " + actualShort);
                            }
                            break;
                        case INT:
                            Integer actualInteger = actualRow.getInt(fieldName);
                            Integer expectedInteger = field.asInteger();
                            if (!expectedInteger.equals(actualInteger)) {
                                throw new Exception("In table " + tableName + ", int values for field " + fieldName +
                                        " differ for expected row: " + expectedRow + ". Cassandra int value is " + actualInteger);
                            }
                            break;
                        case BIGINT:
                            Long actualLong = actualRow.getLong(fieldName);
                            Long expectedLong = field.asLong();
                            if (!expectedLong.equals(actualLong)) {
                                throw new Exception("In table " + tableName + ", bigint values for field " + fieldName +
                                        " differ for expected row: " + expectedRow + ". Cassandra bigint value is " + actualLong);
                            }
                            break;
                        case VARINT:
                            BigInteger actualBigInteger = actualRow.getBigInteger(fieldName);
                            BigInteger expectedBigInteger = BigInteger.valueOf(field.asLong());
                            if (!expectedBigInteger.equals(actualBigInteger)) {
                                throw new Exception("In table " + tableName + ", varint values for field " + fieldName +
                                        " differ for expected row: " + expectedRow + ". Cassandra varint value is " + actualBigInteger);
                            }
                            break;
                        case FLOAT:
                            Float actualFloat = actualRow.getFloat(fieldName);
                            Float expectedFloat = field.asFloat();
                            if (!expectedFloat.equals(actualFloat)) {
                                throw new Exception("In table " + tableName + ", float values for field " + fieldName +
                                        " differ for expected row: " + expectedRow + ". Cassandra float value is " + actualFloat);
                            }
                            break;
                        case DOUBLE:
                            Double actualDouble = actualRow.getDouble(fieldName);
                            Double expectedDouble = field.asDouble();
                            if (!expectedDouble.equals(actualDouble)) {
                                throw new Exception("In table " + tableName + ", double values for field " + fieldName +
                                        " differ for expected row: " + expectedRow + ". Cassandra double value is " + actualDouble);
                            }
                            break;
                        case DECIMAL:
                            BigDecimal actualBigDecimal = actualRow.getBigDecimal(fieldName);
                            BigDecimal expectedBigDecimal = BigDecimal.valueOf(field.asDouble());
                            if (!expectedBigDecimal.equals(actualBigDecimal)) {
                                echo("actualBigDecimal=" + actualBigDecimal);
                                echo("expectedBigDecimal=" + expectedBigDecimal);
                                throw new Exception("In table " + tableName + ", decimal values for field " + fieldName +
                                        " differ for expected row: " + expectedRow + ". Cassandra decimal value is " + actualBigDecimal);
                            }
                            break;
                        case BOOLEAN:
                            Boolean actualBoolean = actualRow.getBoolean(fieldName);
                            Boolean expectedBoolean = field.asBoolean();
                            if (!expectedBoolean.equals(actualBoolean)) {
                                throw new Exception("In table " + tableName + ", boolean values for field " + fieldName +
                                        " differ for expected row: " + expectedRow + ". Cassandra boolean value is " + actualBoolean);
                            }
                            break;
                        case BLOB:
                            ByteBuffer actualByteBuffer = actualRow.getByteBuffer(fieldName);
                            Object rawValue = field.getRawValue();
                            byte[] bytes = (byte[]) rawValue;
                            ByteBuffer expectedByteBuffer = ByteBuffer.wrap(bytes);
                            if (!expectedByteBuffer.equals(actualByteBuffer)) {
                                throw new Exception("In table " + tableName + ", blob values for field " + fieldName +
                                        " differ for expected row: " + expectedRow + ". Cassandra blob value is " + actualByteBuffer);
                            }
                            break;
                        default:
                            Assert.fail("Unsupported cassandra type: " + cassandraType);
                    }
                }

                // Ok found this row in the expected ones
                echo("Found a matching row for " + actualRow);
                return;
            } catch (Exception e) {
                // Does not match this row, let's try the next one
                echo("Not this row: " + e.getMessage());
            }
        }

        Assert.fail("Unable to find this row in the expected ones: " + actualRow);
    }

    @Test
    public void testNullValues()
            throws InitializationException {

        /**
         * Table1 (simple, simple primary key, uuid)
         *
         * testUuid                             testInt1 testInt2 testInt3 testUuid1 testUuid2
         *
         * d6328472-b571-4f61-a82e-fe4344228291 1234     null     5678     null      d6328472-b571-4f61-a82e-fe4344228292
         */

        String tableName1 = TEST_KEYSPACE_A + ".tableWithNulls";
        String createTableCql1 = "CREATE TABLE IF NOT EXISTS " + tableName1
                + " (testUuid uuid PRIMARY KEY, testInt1 int, testInt2 int, testInt3 int, testUuid1 uuid, testUuid2 uuid);";

        List<Map<Field, CassandraType>> table1 = new ArrayList<Map<Field, CassandraType>>();

        Map<Field, CassandraType> row = new HashMap<Field, CassandraType>();
        row = new HashMap<Field, CassandraType>();
        row.put(new Field("testUuid", FieldType.STRING, "d6328472-b571-4f61-a82e-fe4344228291"), CassandraType.UUID);
        row.put(new Field("testInt1", FieldType.INT, 1234), CassandraType.INT);
        row.put(new Field("testInt2", FieldType.INT, null), CassandraType.INT);
        row.put(new Field("testInt3", FieldType.INT, 5678), CassandraType.INT);
        row.put(new Field("testUuid1", FieldType.STRING, null), CassandraType.UUID);
        row.put(new Field("testUuid2", FieldType.STRING, "d6328472-b571-4f61-a82e-fe4344228292"), CassandraType.UUID);
        table1.add(row);

        /**
         * Create the table
         */
        ResultSet resultSet = session.execute(createTableCql1);
        if (!resultSet.wasApplied()) {
            Assert.fail("Statement not applied: " + createTableCql1);
        }


        final TestRunner runner = TestRunners.newTestRunner("com.hurence.logisland.processor.datastore.BulkPut");

        final CassandraControllerService service = new CassandraControllerService();
        runner.addControllerService("cassandra_service", service);
        runner.setProperty(service, CassandraControllerService.HOSTS.getName(), CASSANDRA_HOST);
        runner.setProperty(service, CassandraControllerService.PORT.getName(), CASSANDRA_PORT);
        runner.setProperty(service, CassandraControllerService.FLUSH_INTERVAL.getName(), "1000");
        runner.setProperty(service, CassandraControllerService.BATCH_SIZE.getName(), "500");

        runner.assertValid(service);
        runner.enableControllerService(service);

        /**
         * Bulk insert records
         */
        bulkInsert(service, table1, tableName1);

        service.bulkPut(END_OF_TEST, new StandardRecord()); // Signal end of test
        service.waitForFlush();

        /**
         * Check table content
         */

        StringBuffer sb = new StringBuffer("SELECT * FROM " + tableName1);
        String statement = sb.toString();
        resultSet = session.execute(sb.toString());
        if (!resultSet.wasApplied()) {
            Assert.fail("Statement not applied: " + statement);
        }

        assertEquals("Number of found lines in table " + tableName1 + " is not equal to expected ones", table1.size(), resultSet.getAvailableWithoutFetching());

        // Now check that lines are all expected ones
        Iterator<Row> iterator = resultSet.iterator();
        Iterator<Map<Field, CassandraType>> expectedIterator = table1.iterator();
        while (iterator.hasNext()) {
            Row resultRow = iterator.next();
            Map<Field, CassandraType> expectedRow = expectedIterator.next();
            echo("Trying to find matching expected row for row: " + row);
            checkRowWithNullsEquals(resultRow, expectedRow);
        }

        runner.disableControllerService(service); // Disconnect service from cassandra
    }

    /**
     * Checks that provided rows are equal with respect to potential null values
     *
     * @param actualRow
     * @param expectedRow
     */
    private void checkRowWithNullsEquals(Row actualRow, Map<Field, CassandraType> expectedRow) {

        // Does the returned cassandra row match the current expected one?
        for (Map.Entry<Field, CassandraType> entry : expectedRow.entrySet()) {
            Field field = entry.getKey();
            String fieldName = field.getName();
            CassandraType cassandraType = entry.getValue();
            Object expectedRawValue = field.getRawValue();
            switch (cassandraType) {
                case UUID:
                    UUID actualUuid = actualRow.getUuid(fieldName);
                    if (expectedRawValue == null)
                    {
                        // Null string stay null string
                        if (actualUuid != null) {
                            Assert.fail(actualRow + " result row is not equal to expected row " + expectedRow);
                        }
                    } else {
                        UUID expectedUuid = UUID.fromString(field.asString());
                        if (!expectedUuid.equals(actualUuid)) {
                            Assert.fail(actualRow + " result row is not equal to expected row " + expectedRow);
                        }
                    }
                    break;
                case INT:
                    Integer actualInteger = actualRow.getInt(fieldName);
                    if (expectedRawValue == null) {
                        // Null number values gives 0
                        if (!actualInteger.equals(0)) {
                            Assert.fail(actualRow + " result row is not equal to expected row " + expectedRow);
                        }
                    } else {
                        Integer expectedInteger = field.asInteger();
                        if (!expectedInteger.equals(actualInteger)) {
                            Assert.fail(actualRow + " result row is not equal to expected row " + expectedRow);
                        }
                    }
                    break;
                default:
                    Assert.fail("Unsupported cassandra type: " + cassandraType);
            }
        }
    }
}
