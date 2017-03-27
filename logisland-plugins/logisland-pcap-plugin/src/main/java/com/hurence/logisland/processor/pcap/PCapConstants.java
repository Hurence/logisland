/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.processor.pcap;

import java.util.HashMap;
import java.util.Map;

public class PCapConstants {

  public static enum Fields {
     SRC_ADDR("ip_src_addr")
    ,SRC_PORT("ip_src_port")
    ,DST_ADDR("ip_dst_addr")
    ,DST_PORT("ip_dst_port")
    ,PROTOCOL("protocol")
    ,TIMESTAMP("timestamp")
    ,ORIGINAL("original_string")
    ,INCLUDES_REVERSE_TRAFFIC("includes_reverse_traffic")
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

