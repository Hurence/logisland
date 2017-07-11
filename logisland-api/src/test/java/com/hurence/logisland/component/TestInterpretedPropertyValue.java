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
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;

/**
 * @author tom
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestInterpretedPropertyValue {


    @Test
    public void validate01_Init_MVEL_and_Simple_EL() {

        InterpreterEngineFactory.setInterpreter("mvel");

        String rawValue = "${countryCode}";
        InterpretedPropertyValue ipv = new InterpretedPropertyValue(null, rawValue, null, null);

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
    public void validate02_MVEL_Simple_EL() {

        InterpreterEngineFactory.setInterpreter("mvel");

        String rawValue = "${countryCode}";
        InterpretedPropertyValue ipv = new InterpretedPropertyValue(null, rawValue, null, null);

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
        InterpretedPropertyValue ipv = new InterpretedPropertyValue(null, rawValue, null, null);

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
    public void validate04_Init_Javascript_and_Simple_EL() {

        InterpreterEngineFactory.setInterpreter("javascript");

        String rawValue = "${countryCode}";
        InterpretedPropertyValue ipv = new InterpretedPropertyValue(null, rawValue, null, null);

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
    public void validate05_Javascript_and_Simple_EL() {

        InterpreterEngineFactory.setInterpreter("javascript");

        String rawValue = "${countryCode}";
        InterpretedPropertyValue ipv = new InterpretedPropertyValue(null, rawValue, null, null);

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
    public void validate06_Advanced_Javascript_EL() {

        InterpreterEngineFactory.setInterpreter("javascript");

        String rawValue = "${(typeof countryCode == 'undefined') ? \"coverage_fr\" : \"coverage_\"+countryCode }";
        InterpretedPropertyValue ipv = new InterpretedPropertyValue(null, rawValue, null, null);

        final String docId1 = "id1";
        final String company = "mycompany.com";
        final Record inputRecord1 = new StandardRecord("es_multiget")
                .setStringField("codeProduct", docId1)
                .setStringField("category", "123456")
                .setStringField("price", "89")
                .setStringField("company", company);

        PropertyValue pv = ipv.evaluate(inputRecord1);
        String interpretedValue = pv.asString();
        Assert.assertTrue(interpretedValue.equals("coverage_fr"));

        inputRecord1.setStringField("countryCode","en");
        pv = ipv.evaluate(inputRecord1);
        interpretedValue = pv.asString();
        Assert.assertTrue(interpretedValue.equals("coverage_en"));
    }
}