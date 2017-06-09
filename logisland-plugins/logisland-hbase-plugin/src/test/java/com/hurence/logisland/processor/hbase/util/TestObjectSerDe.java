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

package com.hurence.logisland.processor.hbase.util;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class TestObjectSerDe {

   @Test
    public void testDeserializeSuccessful() throws IOException {
       final ObjectSerDe serDe = new ObjectSerDe();

       final String myObject = "myObject";
       final ByteArrayOutputStream bOut = new ByteArrayOutputStream();
       final ObjectOutputStream out = new ObjectOutputStream(bOut);
       out.writeObject(myObject);

       byte[] myObjectBytes = bOut.toByteArray();
       Assert.assertNotNull(myObjectBytes);
       Assert.assertTrue(myObjectBytes.length > 0);

       final Object deserialized = serDe.deserialize(myObjectBytes);
       Assert.assertTrue(deserialized instanceof String);
       Assert.assertEquals(myObject, deserialized);
   }

    @Test
    public void testDeserializeNull() throws IOException {
        final ObjectSerDe serDe = new ObjectSerDe();
        final Object deserialized = serDe.deserialize(null);
        Assert.assertNull(deserialized);
    }

    @Test
    public void testSerialize() throws IOException, ClassNotFoundException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final String myObject = "myObject";

        final ObjectSerDe serDe = new ObjectSerDe();
        serDe.serialize(myObject, out);

        final ByteArrayInputStream bIn = new ByteArrayInputStream(out.toByteArray());
        final ObjectInputStream in = new ObjectInputStream(bIn);

        final Object deserialized = in.readObject();
        Assert.assertTrue(deserialized instanceof String);
        Assert.assertEquals(myObject, deserialized);
    }

}
