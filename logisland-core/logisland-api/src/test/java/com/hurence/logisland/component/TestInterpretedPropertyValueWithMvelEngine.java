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
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hurence.logisland.component;

import com.hurence.logisland.expressionlanguage.InterpreterEngineFactory;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author tom
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestInterpretedPropertyValueWithMvelEngine {


    @Test
    public void validate01_Init_MVEL_and_Simple_EL() {

        InterpreterEngineFactory.setInterpreter("mvel");

        String rawValue = "${countryCode}";
        InterpretedPropertyValue ipv = new InterpretedPropertyValue(rawValue, null, null);

        final String docId1 = "id1";
        final String company = "mycompany.com";
        final Record inputRecord1 = new StandardRecord("es_multiget")
                .setStringField("codeProduct", docId1)
                .setStringField("category", "123456")
                .setStringField("price", "89")
                .setStringField("company", company)
                .setStringField("countryCode","fr");

        PropertyValue pv = ipv.evaluate(inputRecord1);
        String interpretedValue = pv.asString();
        Assert.assertTrue(interpretedValue.equals("fr"));
    }

    @Test
    public void validate03_MVEL_Advanced_EL() {

        InterpreterEngineFactory.setInterpreter("mvel");

        String rawValue = "${\"coverage_\"+countryCode}";
        InterpretedPropertyValue ipv = new InterpretedPropertyValue(rawValue, null, null);

        final String docId1 = "id1";
        final String company = "mycompany.com";
        final Record inputRecord1 = new StandardRecord("es_multiget")
                .setStringField("codeProduct", docId1)
                .setStringField("category", "123456")
                .setStringField("price", "89")
                .setStringField("company", company)
                .setStringField("countryCode","fr");

        PropertyValue pv = ipv.evaluate(inputRecord1);
        String interpretedValue = pv.asString();
        Assert.assertTrue(interpretedValue.equals("coverage_fr"));
    }

    @Test
    public void validate_MVEL_condition_EL() {

        InterpreterEngineFactory.setInterpreter("mvel");

        String rawValue = "${countryCode == 'fr' && 32 > price}";
        InterpretedPropertyValue ipv = new InterpretedPropertyValue(rawValue, null, null);

        final String docId1 = "id1";
        final String company = "mycompany.com";
        final Record inputRecord1 = new StandardRecord("es_multiget")
                .setStringField("codeProduct", docId1)
                .setStringField("category", "123456")
                .setStringField("price", "89")
                .setStringField("company", company)
                .setStringField("countryCode","fr");

        PropertyValue pv = ipv.evaluate(inputRecord1);
        Assert.assertFalse(pv.asBoolean());
        inputRecord1.setStringField("price","31");
        pv = ipv.evaluate(inputRecord1);
        Assert.assertTrue(pv.asBoolean());
    }


    @Test
    public void validate_MVEL_empty_test() {

        InterpreterEngineFactory.setInterpreter("mvel");

        String rawValue = "${countryCode == empty}";
        InterpretedPropertyValue ipv = new InterpretedPropertyValue(rawValue, null, null);

        final String docId1 = "id1";
        final String company = "mycompany.com";
        final Record inputRecord1 = new StandardRecord("es_multiget")
                .setStringField("codeProduct", docId1)
                .setStringField("category", "123456")
                .setStringField("price", "89")
                .setStringField("company", company)
                .setStringField("countryCode","fr");

        PropertyValue pv = ipv.evaluate(inputRecord1);
        Assert.assertFalse(pv.asBoolean());
        inputRecord1.setStringField("countryCode","");
        pv = ipv.evaluate(inputRecord1);
        Assert.assertTrue(pv.asBoolean());
        inputRecord1.setStringField("countryCode",null);
        pv = ipv.evaluate(inputRecord1);
        Assert.assertTrue(pv.asBoolean());
    }

    @Test
    public void validate_MVEL_manipulate_map() {

        InterpreterEngineFactory.setInterpreter("mvel");

        String rawValue = "${map.containsKey('key') && map.get(3) == true}";
        InterpretedPropertyValue ipv = new InterpretedPropertyValue(rawValue, null, null);

        final Map<Object, Object> map = new HashMap<>();
        map.put("key", "value");
        map.put(3, true);

        Assert.assertTrue(map.containsKey("key") && (boolean)map.get(3));

        final Record inputRecord1 = new StandardRecord("es_multiget")
                .setField("map", FieldType.MAP, map);

        PropertyValue pv = ipv.evaluate(inputRecord1);

        Assert.assertTrue(pv.asBoolean());
        map.put(3, false);
        pv = ipv.evaluate(inputRecord1);
        Assert.assertFalse(pv.asBoolean());
    }

//    @Test
//    public void validate_MVEL_manipulate_object() {
//
//        final class Person {
//            final String name;
//            final String lastName;
//            final int age;
//
//            public Person(String name, String lastName, int age) {
//                this.name = name;
//                this.lastName = lastName;
//                this.age = age;
//            }
//        }
//
//        InterpreterEngineFactory.setInterpreter("mvel");
//
//        String rawValue = "${map.get(\"greg\").age}";
//        InterpretedPropertyValue ipv = new InterpretedPropertyValue(rawValue, null, null);
//
//        final Person person = new Person("grégoire", "seguin-henry", 29);
//        final Map<Object, Object> map = new HashMap<>();
//        map.put("greg", person);
//
//        final Record inputRecord1 = new StandardRecord("es_multiget")
//                .setField("map", FieldType.MAP, map);
//
//        PropertyValue pv = ipv.evaluate(inputRecord1);
//        Assert.assertEquals(29, pv.asInteger().intValue());
//    }

    @Test
    public void validate_MVEL_manipulate_list() {

        InterpreterEngineFactory.setInterpreter("mvel");

        String rawValue = "${list.get(0) - list.get(1)}";
        InterpretedPropertyValue ipv = new InterpretedPropertyValue(rawValue, null, null);

        final List<Float> list = new ArrayList<>();
        list.add(3.5f);
        list.add(3.2f);
        Assert.assertEquals(0.3f, list.get(0) - list.get(1), 0.01f);

        final Record inputRecord1 = new StandardRecord("es_multiget")
                .setField("list", FieldType.ARRAY, list);

        PropertyValue pv = ipv.evaluate(inputRecord1);
        Assert.assertEquals(0.3f, pv.asFloat().floatValue(), 0.01f);
    }


    @Test
    public void validate_MVEL_manipulate_record() {

        InterpreterEngineFactory.setInterpreter("mvel");

        String rawValue = "${subRecord.getField(\"a string\").asString() == \"hello world !\"}";
        InterpretedPropertyValue ipv = new InterpretedPropertyValue(rawValue, null, null);

        final Record subRecord =  new StandardRecord("sub_record")
                .setField("a string", FieldType.STRING, "hello world !")
                .setField("a double", FieldType.DOUBLE, 4.46d);

        final Record inputRecord1 = new StandardRecord("es_multiget")
                .setField("subRecord", FieldType.RECORD, subRecord);

        Assert.assertTrue(subRecord.getField("a string").asString().equals("hello world !"));

        PropertyValue pv = ipv.evaluate(inputRecord1);
        Assert.assertTrue(pv.asBoolean());
        subRecord.setField("a string", FieldType.STRING, "not hello world anymore !");
        pv = ipv.evaluate(inputRecord1);
        Assert.assertFalse(pv.asBoolean());
    }

}