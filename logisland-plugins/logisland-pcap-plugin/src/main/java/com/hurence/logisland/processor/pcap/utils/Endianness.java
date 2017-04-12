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

/** This code is adapted from https://github.com/apache/incubator-metron/blob/master/metron-platform/metron-pcap/src/main/java/org/apache/metron/spout/pcap/Endianness.java */



package com.hurence.logisland.processor.pcap.utils;

import java.nio.ByteOrder;

public enum Endianness {
  LITTLE, BIG;

  public static Endianness getNativeEndianness() {
    if (ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN)) {
      return BIG;
    } else {
      return LITTLE;
    }
  }
}
