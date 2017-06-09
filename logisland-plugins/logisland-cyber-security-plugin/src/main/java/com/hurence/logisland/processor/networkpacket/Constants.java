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
/** This code is adapted from https://github.com/apache/incubator-metron/blob/master/metron-platform/metron-pcap/src/main/java/org/apache/metron/pcap/Constants.java */

package com.hurence.logisland.processor.networkpacket;


public interface Constants {


/** The protocol ipv4. */
public static final int PROTOCOL_IPV4 = 4;

/** The protocol tcp. */
public static final int PROTOCOL_TCP = 6;

/** The protocol udp. */
public static final int PROTOCOL_UDP = 17;

/** The document key separator. */
public static final char DOCUMENT_KEY_SEPARATOR = '-';

}
