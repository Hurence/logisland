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

import java.util.HashMap;
import java.util.Map;

public class PCapConstants {

  public static enum Fields {

    ////////////////////////
    // Global Header data //
    ////////////////////////

    TIMESTAMP("timestamp")


    ////////////////////
    // IP Header data //
    ////////////////////

    /* IP Header's 1st 32-bits word : */
    , IP_VERSION("ip_version")                                        /*  4 bits */
    , IP_INTERNETHEADERLENGTH("ip_internet_header_length")            /*  4 bits */
    , IP_TYPEOFSERVICE("ip_type_of_service")                          /*  8 bits */
    , IP_DATAGRAMTOTALLENGTH("ip_datagram_total_length")              /* 16 bits */

    /* IP Headers's 2nd 32-bits word : */
    , IP_IDENTIFICATION("ip_identification")                          /* 16 bits */
    , IP_FLAGS("ip_flags")                                            /*  3 bits */
    , IP_FRAGMENTOFFSET("ip_fragment_offset")                         /* 13 bits */

    /* IP Headers's 3rd 32-bits word : */
    , IP_TIMETOLIVE("ip_time_to_live")                                /*  8 bits */
    , IP_PROTOCOL("protocol")                                         /*  8 bits */
    , IP_CHECKSUM("ip_checksum")                                      /* 16 bits */

    /* IP Headers's 4th 32-bits word : */
    , IP_SRCIPADDRESS("src_ip")                                       /* 32 bits */

    /* IP Headers's 5th 32-bits word : */
    , IP_DSTIPADDRESS("dst_ip")                                       /* 32 bits */

    /* IP Headers's following 32-bits word(s) : */
    , IP_OPTIONS("ip_options")                                        /* variable size */
    , IP_PADDING("ip_padding")                                        /* variable size */



    /////////////////////
    // TCP Header data //
    /////////////////////

    /* TCP Header's 1st 32-bits word : */
    , TCP_SRCPORT("src_port")                                         /* 16 bits */
    , TCP_DSTPORT("dest_port")                                        /* 16 bits */

    /* TCP Header's 2nd 32-bits word : */
    , TCP_SEQUENCENUMBER("tcp_sequence_number")                       /* 32 bits */

    /* TCP Header's 3rd 32-bits word : */
    , TCP_ACKNOWLEDGMENTNUMBER("tcp_acknowledgment_number")           /* 32 bits */

    /* TCP Header's 4th 32-bits word : */
    , TCP_DATAOFFSET("tcp_data_offset")                               /*  4 bits */
    , TCP_FLAGS("tcp_flags")                                          /*  9 bits */
    , TCP_WINDOWSIZE("tcp_window_size")                               /* 16 bits */

    /* TCP Header's 5th 32-bits word : */
    , TCP_CHECKSUM("tcp_checksum")                                    /* 16 bits */
    , TCP_URGENTPOINTER("tcp_urgent_pointer")                         /* 16 bits */

    /* TCP Headers's following 32-bits word(s) : */
    , TCP_OPTIONS("tcp_options")                                      /* variable size */
    , TCP_PADDING("tcp_padding")                                      /* variable size */

    /* TCP Headers's other computed information : */
    , TCP_COMPUTED_SRCIP("tcp_computed_src_ip")
    , TCP_COMPUTED_DSTIP("tcp_computed_dest_ip")
    , TCP_COMPUTED_SEGMENTTOTALLENGTH("tcp_computed_segment_total_length")
    , TCP_COMPUTED_DATALENGTH("tcp_computed_data_length")
    , TCP_COMPUTED_REASSEMBLEDLENGTH("tcp_computed_reassembled_length")
    , TCP_COMPUTED_TRAFFICDIRECTION("tcp_computed_traffic_direction")
    , TCP_COMPUTED_RELATIVEACK("tcp_computed_relative_ack")
    , TCP_COMPUTED_RELATIVESEQ("tcp_computed_relative_seq")

    ////////////
    // Others //
    ////////////

    //,ORIGINAL("original_string")
    //,INCLUDES_REVERSE_TRAFFIC("includes_reverse_traffic")
    ;


    private String name;

    Fields(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

  }




}

