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

/** This code is adapted from https://github.com/apache/incubator-metron/blob/master/metron-platform/metron-common/src/main/java/org/apache/metron/common/Constants.java */

package com.hurence.logisland.processor.pcap;

import com.hurence.logisland.record.FieldType;

import java.util.HashMap;
import java.util.Map;

public class PCapConstants {

  public static enum Fields {

    ////////////////////////
    // Global Header data //
    ////////////////////////

    GLOBAL_MAGICNUMBER("global_magic", FieldType.LONG)


    ////////////////////////
    // Packet Header data //
    ////////////////////////

    ,PCKT_TIMESTAMP_IN_NANOS("packet_timestamp_in_nanos", FieldType.LONG)


    ////////////////////
    // IP Header data //
    ////////////////////

    /* IP Header's 1st 32-bits word : */
    , IP_VERSION("ip_version", FieldType.INT)                                        /*  4 bits */
    , IP_INTERNETHEADERLENGTH("ip_internet_header_length", FieldType.INT)            /*  4 bits */
    , IP_TYPEOFSERVICE("ip_type_of_service", FieldType.INT)                          /*  8 bits */
    , IP_DATAGRAMTOTALLENGTH("ip_datagram_total_length", FieldType.INT)              /* 16 bits */

    /* IP Headers's 2nd 32-bits word : */
    , IP_IDENTIFICATION("ip_identification", FieldType.INT)                          /* 16 bits */
    , IP_FLAGS("ip_flags", FieldType.INT)                                            /*  3 bits */
    , IP_FRAGMENTOFFSET("ip_fragment_offset", FieldType.INT)                         /* 13 bits */

    /* IP Headers's 3rd 32-bits word : */
    , IP_TIMETOLIVE("ip_time_to_live", FieldType.INT)                                /*  8 bits */
    , IP_PROTOCOL("protocol", FieldType.INT)                                         /*  8 bits */
    , IP_CHECKSUM("ip_checksum", FieldType.INT)                                      /* 16 bits */

    /* IP Headers's 4th 32-bits word : */
    , IP_SRCIPADDRESS("src_ip", FieldType.STRING)                                    /* 32 bits */

    /* IP Headers's 5th 32-bits word : */
    , IP_DSTIPADDRESS("dst_ip", FieldType.STRING)                                    /* 32 bits */

    /* IP Headers's following 32-bits word(s) : */
    , IP_OPTIONS("ip_options", FieldType.BYTES)                                      /* variable size */
    , IP_PADDING("ip_padding", FieldType.BYTES)                                      /* variable size */



    /////////////////////
    // TCP Header data //
    /////////////////////

    /* TCP Header's 1st 32-bits word : */
    , TCP_SRCPORT("src_port", FieldType.INT)                                         /* 16 bits */
    , TCP_DSTPORT("dest_port", FieldType.INT)                                        /* 16 bits */

    /* TCP Header's 2nd 32-bits word : */
    , TCP_SEQUENCENUMBER("tcp_sequence_number", FieldType.INT)                       /* 32 bits */

    /* TCP Header's 3rd 32-bits word : */
    , TCP_ACKNOWLEDGMENTNUMBER("tcp_acknowledgment_number", FieldType.INT)           /* 32 bits */

    /* TCP Header's 4th 32-bits word : */
    , TCP_DATAOFFSET("tcp_data_offset", FieldType.INT)                               /*  4 bits */
    , TCP_FLAGS("tcp_flags", FieldType.INT)                                          /*  9 bits */
    , TCP_WINDOWSIZE("tcp_window_size", FieldType.INT)                               /* 16 bits */

    /* TCP Header's 5th 32-bits word : */
    , TCP_CHECKSUM("tcp_checksum", FieldType.INT)                                    /* 16 bits */
    , TCP_URGENTPOINTER("tcp_urgent_pointer", FieldType.INT)                         /* 16 bits */

    /* TCP Headers's following 32-bits word(s) : */
    , TCP_OPTIONS("tcp_options", FieldType.BYTES)                                    /* variable size */
    , TCP_PADDING("tcp_padding", FieldType.BYTES)                                    /* variable size */

    /* TCP Headers's other computed information : */
    , TCP_COMPUTED_SRCIP("tcp_computed_src_ip", FieldType.STRING)
    , TCP_COMPUTED_DSTIP("tcp_computed_dest_ip", FieldType.STRING)
    , TCP_COMPUTED_SEGMENTTOTALLENGTH("tcp_computed_segment_total_length", FieldType.INT)
    , TCP_COMPUTED_DATALENGTH("tcp_computed_data_length", FieldType.INT)
    , TCP_COMPUTED_REASSEMBLEDLENGTH("tcp_computed_reassembled_length", FieldType.INT)
    , TCP_COMPUTED_TRAFFICDIRECTION("tcp_computed_traffic_direction", FieldType.STRING)
    , TCP_COMPUTED_RELATIVEACK("tcp_computed_relative_ack", FieldType.INT)
    , TCP_COMPUTED_RELATIVESEQ("tcp_computed_relative_seq", FieldType.INT)

    ////////////
    // Others //
    ////////////

    //,ORIGINAL("original_string")
    //,INCLUDES_REVERSE_TRAFFIC("includes_reverse_traffic")
    ;


    private String name;
    private FieldType fieldType;

    Fields(String name) {
      this.name = name;
      this.fieldType = FieldType.STRING;
    }

    Fields(String name, FieldType fieldType) {
      this.name = name;
      this.fieldType = fieldType;
    }

    public String getName() {
      return name;
    }

    public FieldType getFieldType() {
      return fieldType;
    }

  }




}

