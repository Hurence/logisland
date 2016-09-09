/*
 Copyright 2016 Hurence

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package com.hurence.logisland.serializer;

import com.hurence.logisland.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class EventStringSerializer implements EventSerializer {

    private static Logger logger = LoggerFactory.getLogger(EventStringSerializer.class);


    @Override
    public void serialize(OutputStream out, Event event) throws EventSerdeException {

        try {
            out.write(event.toString().getBytes());
            out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    @Override
    public Event deserialize(InputStream in) throws EventSerdeException {

        throw new RuntimeException("not implemented yet");

    }
}