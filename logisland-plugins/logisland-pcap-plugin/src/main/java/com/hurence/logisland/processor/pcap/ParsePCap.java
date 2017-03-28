/**
 * Copyright (C) 2017 Hurence
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
package com.hurence.logisland.processor.pcap;

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.*;
import com.hurence.logisland.record.*;
import com.hurence.logisland.validator.StandardValidators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.EnumMap;

/**
 * PCap processor
 */
@Tags({"PCap", "security", "IDS", "NIDS"})
@CapabilityDescription(
        "The PCap processor is the LogIsland entry point to get and process network packets captured in pcap format."
        +""
        + "incoming record : record key = timestamp, record_value = raw pcap file in bytes"
        + "The pcap file contains a global header and a sequence of (packet header, packet data), meaning that several packets are stored in the same file"
        + "The output is a list of records where each record holds the information regarding one packet. Here is an example of the fields of one given packet extracted by the PCap processor from an incoming pcap record :"
        + "- timestamp : \n"
        + "- source ip : \n"
        + "- destination ip : \n"
        + "encapsulation")

public class ParsePCap extends AbstractProcessor {

    private static Logger logger = LoggerFactory.getLogger(ParsePCap.class);

    private boolean debug = false;
    
    private static final String KEY_DEBUG = "debug";
    
    public static final PropertyDescriptor DEBUG = new PropertyDescriptor.Builder()
            .name(KEY_DEBUG)
            .description("Enable debug. If enabled, the original JSON string is embedded in the record_value field of the record.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(false)
            .build();

    @Override
    public void init(final ProcessContext context)
    {
        logger.debug("Initializing PCap Processor");
    }
    
    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(DEBUG);

        return Collections.unmodifiableList(descriptors);
    }
  
    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records)
    {
        if (debug)
        {
            logger.debug("PCap Processor records input: " + records);
        }

        /**
         * Get the original PCap event as Bytes and do some parsing
         */
        List<Record> outputRecords = new ArrayList<>();
        records.forEach(record -> {
            try {
                final Long pcapTimestamp = record.getField(FieldDictionary.RECORD_KEY).asLong();
                //final byte[] value = record.getField(FieldDictionary.RECORD_VALUE).asBytes();
                final byte[] packetRawValue = (byte[])record.getField(FieldDictionary.RECORD_VALUE).getRawValue();

                List<PacketInfo> info = PcapHelper.toPacketInfo(packetRawValue);
                for (PacketInfo pi : info) {
                    StandardRecord outputRecord = new StandardRecord();

                    EnumMap<PCapConstants.Fields, Object> result = PcapHelper.packetToFields(pi);

                    outputRecord.setField(new Field(FieldDictionary.RECORD_KEY, FieldType.LONG, pcapTimestamp));

                    for (PCapConstants.Fields field : PCapConstants.Fields.values()) {
                        if (result.containsKey(field)) {
                            outputRecord.setField(new Field(field.getName(), FieldType.STRING, result.get(field)));
                        }
                    }
                    outputRecord.setField(new Field(FieldDictionary.RECORD_TYPE, FieldType.STRING, "network_packet"));
                    outputRecord.setField(new Field(FieldDictionary.RECORD_RAW_VALUE, FieldType.BYTES, packetRawValue));
                    outputRecords.add(outputRecord);
                }
            } catch (Exception e) {

            } finally {

            }

        });
        return outputRecords;
    }
    
    /**
     * Deeply clones the passed map regarding keys (so that one can modify keys of the original map without changing
     * the clone).
     * @param origMap Map to clone.
     * @return Cloned map.
     */
    private static Map<String, Object> cloneMap(Map<String, Object> origMap)
    {
        Map<String, Object> finalMap = new HashMap<String, Object>();
        origMap.forEach( (key, value) -> {
            if (value instanceof Map)
            {
                Map<String, Object> map = (Map<String, Object>)value;
                finalMap.put(key, (Object)cloneMap(map)); 
            } else
            {
                finalMap.put(key, value);
            }
        });
        return finalMap;
    }

    
    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {

        logger.debug("property {} value changed from {} to {}", descriptor.getName(), oldValue, newValue);
        
        /**
         * Handle the debug property
         */
        if (descriptor.getName().equals(KEY_DEBUG))
        {
          if (newValue != null)
          {
              if (newValue.equalsIgnoreCase("true"))
              {
                  debug = true;
              }
          } else
          {
              debug = false;
          }
        }
    }   
}
