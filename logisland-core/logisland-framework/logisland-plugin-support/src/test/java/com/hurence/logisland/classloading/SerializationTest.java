/*
 *  * Copyright (C) 2018 Hurence (support@hurence.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.hurence.logisland.classloading;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.junit.Test;

import java.io.Serializable;

public class SerializationTest {


    public interface MyInterface extends Serializable {
        String getStringz();

        byte[] getBytez();
    }

    public static class BigSerializableClass implements MyInterface {

        private String stringz;
        private byte[] bytez = new byte[1024];

        @Override
        public String getStringz() {
            return stringz;
        }

        public void setStringz(String stringz) {
            this.stringz = stringz;
        }

        @Override
        public byte[] getBytez() {
            return bytez;
        }

        public void setBytez(byte[] bytez) {
            this.bytez = bytez;
        }

    }


    @Test
    public void testCustomSerialization() throws Exception {

        BigSerializableClass instance = new BigSerializableClass();
        instance.setStringz(RandomStringUtils.random(1024));
        System.out.println(instance.getStringz());

        MyInterface proxy = PluginProxy.create(instance);
        System.out.println(proxy.getStringz());

        MyInterface serdeser = SerializationUtils.deserialize(SerializationUtils.serialize(proxy));
        System.out.println(serdeser.getStringz());
        BigSerializableClass unwrapped = PluginProxy.unwrap(serdeser);
        System.out.println(unwrapped.getStringz());
        MyInterface rewrapped = PluginProxy.create(unwrapped);
        System.out.println(rewrapped.getStringz());


    }
}
