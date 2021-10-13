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
package com.hurence.logisland.processor;

import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class DebugStreamTest {

    @Test
    public void testLogOfDebugStream() {
        Collection<Record> records = new ArrayList<>();


        records.add(new StandardRecord()
                .setField("a", FieldType.STRING, "a1")
                .setField("b", FieldType.STRING, "b1")
                .setField("c", FieldType.LONG, 1));

        records.add(new StandardRecord()
                .setField("a", FieldType.STRING, "a2")
                .setField("b", FieldType.STRING, "b2")
                .setField("c", FieldType.LONG, 2));

        records.add(new StandardRecord()
                .setField("a", FieldType.STRING, "a3")
                .setField("b", FieldType.STRING, "b3")
                .setField("c", FieldType.LONG, 3));

        TestRunner testRunner = TestRunners.newTestRunner(new DebugStream());
        testRunner.setProcessorIdentifier("debug_1");
        testRunner.assertValid();
        testRunner.enqueue(records);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(3);
        testRunner.assertOutputErrorCount(0);
        testRunner.enqueue(records);
        testRunner.run();
        testRunner.assertOutputRecordsCount(6);
        testRunner.enqueue(records);
        testRunner.run();
        testRunner.assertOutputRecordsCount(9);
        testRunner.enqueue(records);
        testRunner.run();
        testRunner.assertOutputRecordsCount(12);
        testRunner.enqueue(records);
        testRunner.run();
        testRunner.assertOutputRecordsCount(15);
        testRunner.enqueue(records);
        testRunner.run();
        testRunner.assertOutputRecordsCount(18);
        testRunner.enqueue(records);
        testRunner.run();
        testRunner.assertOutputRecordsCount(21);
    }

    @Test
    public void testWithNestedRecord() {
        Record record1 = new StandardRecord()
                .setField("a", FieldType.STRING, "a1")
                .setField("b", FieldType.STRING, "b1")
                .setField("c", FieldType.LONG, 1);
        Record record2 = new StandardRecord()
                .setField("a", FieldType.STRING, "a2")
                .setField("b", FieldType.STRING, "b2")
                .setField("nested", FieldType.RECORD, record1);
        Record record3 = new StandardRecord()
                .setField("a", FieldType.STRING, "a3")
                .setField("b", FieldType.STRING, "b3")
                .setField("nested", FieldType.RECORD, record2);
        Record record4 = new StandardRecord()
                .setField("a", FieldType.STRING, "a3")
                .setField("b", FieldType.STRING, "b3")
                .setField("nested", FieldType.RECORD, record3)
                .setField("nested_simple", FieldType.RECORD, record1);

        TestRunner testRunner = TestRunners.newTestRunner(new DebugStream());
        testRunner.setProcessorIdentifier("debug_1");
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(record1, record2, record3, record4));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(4);
        testRunner.assertOutputErrorCount(0);

        //last one Should output something like this
//        Record{
//            Field{name='a', type=string, rawValue=a3},
//            Field{name='b', type=string, rawValue=b3},
//            Field{name='nested', type=record, rawValue=Record{
//                Field{name='a', type=string, rawValue=a3},
//                Field{name='b', type=string, rawValue=b3},
//                Field{name='nested', type=record, rawValue=Record{
//                    Field{name='a', type=string, rawValue=a2},
//                    Field{name='b', type=string, rawValue=b2},
//                    Field{name='nested', type=record, rawValue=Record{
//                        Field{name='a', type=string, rawValue=a1},
//                        Field{name='b', type=string, rawValue=b1},
//                        Field{name='c', type=long, rawValue=1},
//                        Field{name='record_id', type=string, rawValue=23e5fb95-4048-4321-8913-15aebaedfe7b},
//                        Field{name='record_time', type=long, rawValue=1607969587760},
//                        Field{name='record_type', type=string, rawValue=generic}
//                    }},
//                    Field{name='record_id', type=string, rawValue=106cd6db-0873-43f3-9ead-dc05ae70ffc5},
//                    Field{name='record_time', type=long, rawValue=1607969587760},
//                    Field{name='record_type', type=string, rawValue=generic}
//                }},
//                Field{name='record_id', type=string, rawValue=32cb5007-f9e2-4247-9588-5f0f1d57157f},
//                Field{name='record_time', type=long, rawValue=1607969587760},
//                Field{name='record_type', type=string, rawValue=generic}
//            }},
//            Field{name='nested_simple', type=record, rawValue=Record{
//                Field{name='a', type=string, rawValue=a1},
//                Field{name='b', type=string, rawValue=b1},
//                Field{name='c', type=long, rawValue=1},
//                Field{name='record_id', type=string, rawValue=23e5fb95-4048-4321-8913-15aebaedfe7b},
//                Field{name='record_time', type=long, rawValue=1607969587760},
//                Field{name='record_type', type=string, rawValue=generic}
//            }},
//            Field{name='record_id', type=string, rawValue=b2ca6894-91db-44a7-a150-885474036849},
//            Field{name='record_time', type=long, rawValue=1607969587760},
//            Field{name='record_type', type=string, rawValue=generic}
//        }

    }

    @Test
    public void testWithAllBasictype() {
        Map<String, Long> map = new HashMap<>();
        map.put("hi", 4L);
        map.put("hi2", 5L);
        Map<String, Object> mapObject = new HashMap<>();
        mapObject.put("un", new Date());
        mapObject.put("CoordinateWithoutBean", new CoordinateWithoutBean(3, 55 ));
        mapObject.put("CoordinateWithBean", new CoordinateWithBean(3, 55 ));
        Record record1 = new StandardRecord()
                .setField("STRING", FieldType.STRING, "a1")
                .setField("INT", FieldType.INT, 1)
                .setField("BOOLEAN", FieldType.BOOLEAN, true)
                .setField("BYTES", FieldType.BYTES, new byte[]{1, 2, 3})//TODO how to show correctly bytes in debugstream
                .setField("DATETIME", FieldType.DATETIME, new Date())
                .setField("DOUBLE", FieldType.DOUBLE, 1d)
                .setField("FLOAT", FieldType.FLOAT, 1f)
                .setField("ENUM", FieldType.ENUM, TOTO.UN)
                .setField("LONG", FieldType.LONG, 1L)
                .setField("MAP", FieldType.MAP, map)
                .setField("MAP2", FieldType.MAP, mapObject)
                .setField("NULLNOTNULL", FieldType.NULL, 1L)
                .setField("NULLNOTNULL2", FieldType.NULL, "not null")
                .setField("NULL", FieldType.NULL, null)
                .setField("OBJECT", FieldType.OBJECT, "not null")
                .setField("OBJECT null", FieldType.OBJECT, null)
                .setField("OBJECT CoordinateWithoutBean", FieldType.OBJECT, new CoordinateWithoutBean(2, 5))
                .setField("OBJECT CoordinateWithBean", FieldType.OBJECT, new CoordinateWithBean(2, 5));

        TestRunner testRunner = TestRunners.newTestRunner(new DebugStream());
        testRunner.setProcessorIdentifier("debug_1");
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(record1));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);
        testRunner.assertOutputErrorCount(0);

        //Should output something like this
//        "Record{\n" +
//           Field{name='BOOLEAN', type=boolean, rawValue=true},\n" +
//           Field{name='BYTES', type=bytes, rawValue=[B@5a39699c},\n" +
//           Field{name='DATETIME', type=datetime, rawValue=Mon Dec 14 17:49:37 CET 2020},\n" +
//           Field{name='DOUBLE', type=double, rawValue=1.0},\n" +
//           Field{name='ENUM', type=enum, rawValue=UN},\n" +
//           Field{name='FLOAT', type=float, rawValue=1.0},\n" +
//           Field{name='INT', type=int, rawValue=1},\n" +
//           Field{name='LONG', type=long, rawValue=1},\n" +
//           Field{name='MAP', type=map, rawValue={hi2=5, hi=4}},\n" +
//           Field{name='MAP2', type=map, rawValue={CoordinateWithBean=cordinate{x=3, y=55}, CoordinateWithoutBean=cordinate{x=3, y=55}, un=Mon Dec 14 17:49:37 CET 2020}},\n" +
//           Field{name='NULL', type=null, rawValue=null},\n" +
//           Field{name='NULLNOTNULL', type=null, rawValue=1},\n" +
//           Field{name='NULLNOTNULL2', type=null, rawValue=not null},\n" +
//           Field{name='OBJECT', type=object, rawValue=not null},\n" +
//           Field{name='OBJECT CoordinateWithBean', type=object, rawValue=cordinate{x=2, y=5}},\n" +
//           Field{name='OBJECT CoordinateWithoutBean', type=object, rawValue=cordinate{x=2, y=5}},\n" +
//           Field{name='OBJECT null', type=object, rawValue=null},\n" +
//           Field{name='STRING', type=string, rawValue=a1},\n" +
//           Field{name='record_id', type=string, rawValue=66b12461-1a31-4763-885c-a50a07deedd5},\n" +
//           Field{name='record_time', type=long, rawValue=1607964577381},\n" +
//           Field{name='record_type', type=string, rawValue=generic}\n" +
//       }";


    }

    @Test
    public void testWithArrayComplextype() {
        Record record1 = new StandardRecord()
                .setField("listString", FieldType.ARRAY, Arrays.asList("a,b,c"))
                .setField("listInt", FieldType.ARRAY, Arrays.asList(1,2,3))
                .setField("listObj", FieldType.ARRAY, Arrays.asList(new CoordinateWithoutBean(1,2), new CoordinateWithoutBean(12,22)))
                .setField("listBytes", FieldType.ARRAY, Arrays.asList((byte)1, (byte)2, (byte)55))
                .setField("arrayString", FieldType.ARRAY, new String[]{"a,b,c"})
                .setField("arrayInt", FieldType.ARRAY, new int[]{1,2,3})
                .setField("arrayObj", FieldType.ARRAY, new Object[]{new CoordinateWithoutBean(1,2), new CoordinateWithoutBean(12,22)})
                .setField("arrayBytes", FieldType.ARRAY, new byte[]{1, 2, 55});

        TestRunner testRunner = TestRunners.newTestRunner(new DebugStream());
        testRunner.setProcessorIdentifier("debug_1");
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(record1));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);
        testRunner.assertOutputErrorCount(0);

        //Should output something like this
    }

    @Test
    public void testWithEmptyRecord() {
        Record record1 = new StandardRecord();

        TestRunner testRunner = TestRunners.newTestRunner(new DebugStream());
        testRunner.setProcessorIdentifier("debug_1");
        testRunner.assertValid();
        testRunner.enqueue(Collections.singletonList(record1));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);
        testRunner.assertOutputErrorCount(0);

        //Should output something like this
//        Record{
//            Field{name='record_id', type=string, rawValue=96f6bf08-f822-4787-b79e-0bf0fc03ceb4},
//            Field{name='record_time', type=long, rawValue=1607964909920},
//            Field{name='record_type', type=string, rawValue=generic}
//        }
    }

    private enum TOTO {
        UN,DEUX
    }
    private class CoordinateWithoutBean {
        int x;
        int y;

        public CoordinateWithoutBean(int x, int y) {
            this.x = x;
            this.y = y;
        }

        @Override
        public String toString() {
            return "cordinate{" +
                    "x=" + x +
                    ", y=" + y +
                    '}';
        }
    }

    private class CoordinateWithBean {
        int x;
        int y;

        public CoordinateWithBean(int x, int y) {
            this.x = x;
            this.y = y;
        }

        @Override
        public String toString() {
            return "cordinate{" +
                    "x=" + x +
                    ", y=" + y +
                    '}';
        }

        public int getX() {
            return x;
        }

        public CoordinateWithBean setX(int x) {
            this.x = x;
            return this;
        }

        public int getY() {
            return y;
        }

        public CoordinateWithBean setY(int y) {
            this.y = y;
            return this;
        }
    }
}
