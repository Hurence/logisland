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

/** This code is adapted from https://github.com/apache/incubator-metron/blob/master/metron-platform/metron-pcap/src/main/java/org/apache/metron/pcap/PcapHelper.java */

package com.hurence.logisland.processor.pcap;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.krakenapps.pcap.decoder.ethernet.EthernetType;
import org.krakenapps.pcap.decoder.ip.IpDecoder;
import org.krakenapps.pcap.decoder.ip.Ipv4Packet;
import org.krakenapps.pcap.decoder.tcp.TcpPacket;
import org.krakenapps.pcap.decoder.udp.UdpPacket;
import org.krakenapps.pcap.file.GlobalHeader;
import org.krakenapps.pcap.packet.PacketHeader;
import org.krakenapps.pcap.packet.PcapPacket;
import org.krakenapps.pcap.util.Buffer;
import org.krakenapps.pcap.util.ByteOrderConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.EnumMap;
import java.util.ArrayList;
import java.util.List;

import com.hurence.logisland.processor.pcap.utils.Endianness;

public class PcapHelper {

  private static Logger logger = LoggerFactory.getLogger(PcapHelper.class);

  public static final int PACKET_HEADER_SIZE = 4*Integer.BYTES;
  public static final int GLOBAL_HEADER_SIZE = 24;


  public static ThreadLocal<LogIslandEthernetDecoder> ETHERNET_DECODER = new ThreadLocal<LogIslandEthernetDecoder>() {
    @Override
    protected LogIslandEthernetDecoder initialValue() {
      return createDecoder();
    }
  };

  public static Long getTimestamp(String filename) {
    try {
      return Long.parseUnsignedLong(Iterables.get(Splitter.on('_').split(filename), 2));
    }
    catch(Exception e) {
      //something went wrong here.
      return null;
    }
  }

  public static String toFilename(String topic, long timestamp, String partition, String uuid)
  {
    return Joiner.on("_").join("pcap"
                              ,topic
                              , Long.toUnsignedString(timestamp)
                              ,partition
                              , uuid
                              );
  }

  public static boolean swapBytes(Endianness endianness) {
    return endianness == Endianness.LITTLE;
  }

  public static byte[] getPcapGlobalHeader(Endianness endianness) {
    if(swapBytes(endianness)) {
      //swap
      return new byte[] {
              (byte) 0xd4, (byte) 0xc3, (byte) 0xb2, (byte) 0xa1 //swapped magic number 0xa1b2c3d4
              , 0x02, 0x00 //swapped major version 2
              , 0x04, 0x00 //swapped minor version 4
              , 0x00, 0x00, 0x00, 0x00 //GMT to local tz offset (= 0)
              , 0x00, 0x00, 0x00, 0x00 //sigfigs (= 0)
              , (byte) 0xff, (byte) 0xff, 0x00, 0x00 //snaplen (=65535)
              , 0x01, 0x00, 0x00, 0x00 // swapped link layer header type (1 = ethernet)
                        };
    }
    else {
      //no need to swap
      return new byte[] {
              (byte) 0xa1, (byte) 0xb2, (byte) 0xc3, (byte) 0xd4 //magic number 0xa1b2c3d4
              , 0x00, 0x02 //major version 2
              , 0x00, 0x04 //minor version 4
              , 0x00, 0x00, 0x00, 0x00 //GMT to local tz offset (= 0)
              , 0x00, 0x00, 0x00, 0x00 //sigfigs (= 0)
              , 0x00, 0x00, (byte) 0xff, (byte) 0xff //snaplen (=65535)
              , 0x00, 0x00, 0x00, 0x01 // link layer header type (1 = ethernet)
                        };
    }
  }

  public static Long getTimestamp(byte[] pcap ) {
    return getTimestamp(pcap, 0, pcap.length);
  }

  public static Long getTimestamp(byte[] pcap, int offset, int length) {
    PcapByteInputStream pcapByteInputStream = null;
    try {
      pcapByteInputStream = new PcapByteInputStream(pcap, offset, length);
      PcapPacket packet = pcapByteInputStream.getPacket();
      GlobalHeader globalHeader = pcapByteInputStream.getGlobalHeader();
      PacketHeader packetHeader = packet.getPacketHeader();
      if ( globalHeader.getMagicNumber() == 0xa1b2c3d4 || globalHeader.getMagicNumber() == 0xd4c3b2a1 )
      {
        //Time is in micro assemble as nano
        logger.info("Times are in micro according to the magic number");
        return packetHeader.getTsSec() * 1000000000L + packetHeader.getTsUsec() * 1000L ;
      }
      else if ( globalHeader.getMagicNumber() == 0xa1b23c4d || globalHeader.getMagicNumber() == 0x4d3cb2a1 ) {
        //Time is in nano assemble as nano
        logger.info("Times are in nano according to the magic number");
        return packetHeader.getTsSec() * 1000000000L + packetHeader.getTsUsec() ;
      }
      //Default assume time is in micro assemble as nano
      logger.warn("Unknown magic number. Defaulting to micro");
      return packetHeader.getTsSec() * 1000000000L + packetHeader.getTsUsec() * 1000L ;
    }
    catch(IOException ioe) {
      //we cannot read the packet, so we return null here.
      logger.error("Unable to read packet", ioe);
    }
    finally {
      if(pcapByteInputStream != null) {
        try {
          pcapByteInputStream.close();
        } catch (IOException e) {
          logger.error("Unable to close stream", e);
        }
      }
    }
    return null;
  }
  public static byte[] addGlobalHeader(byte[] packet, Endianness endianness) {
    byte[] globalHeader = getPcapGlobalHeader(endianness);
    byte[] ret = new byte[packet.length + GLOBAL_HEADER_SIZE];
    int offset = 0;
    System.arraycopy(globalHeader, 0, ret, offset, GLOBAL_HEADER_SIZE);
    offset += globalHeader.length;
    System.arraycopy(packet, 0, ret, offset, packet.length);
    return ret;
  }

  public static byte[] addPacketHeader(long tsNano, byte[] packet, Endianness endianness) {
    boolean swapBytes = swapBytes(endianness);
    long micros = Long.divideUnsigned(tsNano, 1000);
    int secs = (int)(micros / 1000000);
    int usec = (int)(micros % 1000000);
    int capLen = packet.length;
    byte[] ret = new byte[PACKET_HEADER_SIZE + packet.length];
    int offset = 0;
    {
      byte[] b = Bytes.toBytes(swapBytes?ByteOrderConverter.swap(secs):secs);
      System.arraycopy(b, 0, ret, offset, Integer.BYTES);
      offset += Integer.BYTES;
    }
    {
      byte[] b = Bytes.toBytes(swapBytes?ByteOrderConverter.swap(usec):usec);
      System.arraycopy(b, 0, ret, offset, Integer.BYTES);
      offset += Integer.BYTES;
    }
    {
      byte[] b = Bytes.toBytes(swapBytes?ByteOrderConverter.swap(capLen):capLen);
      System.arraycopy(b, 0, ret, offset, Integer.BYTES);
      offset += Integer.BYTES;
    }
    {
      byte[] b = Bytes.toBytes(swapBytes?ByteOrderConverter.swap(capLen):capLen);
      System.arraycopy(b, 0, ret, offset, Integer.BYTES);
      offset += Integer.BYTES;
    }
    System.arraycopy(packet, 0, ret, offset, packet.length);
    return ret;
  }
  public static EnumMap<PCapConstants.Fields, Object> packetToFields(PacketInfo pi) {
    EnumMap<PCapConstants.Fields, Object> ret = new EnumMap(PCapConstants.Fields.class);

    if(pi != null) {

      /* Global Header Data : */
      ret.put(PCapConstants.Fields.TIMESTAMP, pi.getPacketTimeInNanos());

      /* IP Header Data : */
      if (pi.getIpv4Packet() != null) {
        ret.put(PCapConstants.Fields.PROTOCOL, pi.getIpv4Packet().getProtocol());
        ret.put(PCapConstants.Fields.IPDATAGRAMTOTALLENGTH, pi.getIpv4Packet().getTotalLength());
        ret.put(PCapConstants.Fields.IPFLAGS, pi.getIpv4Packet().getFlags());
        ret.put(PCapConstants.Fields.IPFRAGMENTOFFSET, pi.getIpv4Packet().getFragmentOffset());
        ret.put(PCapConstants.Fields.IPHEADERCHECKSUM, pi.getIpv4Packet().getHeaderChecksum());
        ret.put(PCapConstants.Fields.INTERNETHEADERLENGTH, pi.getIpv4Packet().getIhl());
        ret.put(PCapConstants.Fields.IPHEADERIDENTIFICATION, pi.getIpv4Packet().getId());
        ret.put(PCapConstants.Fields.TIMETOLIVE, pi.getIpv4Packet().getTtl());
        ret.put(PCapConstants.Fields.IPVERSION, pi.getIpv4Packet().getVersion());
        if (pi.getIpv4Packet().getOptions() != null) {
          ret.put(PCapConstants.Fields.IPHEADEROPTIONS, pi.getIpv4Packet().getOptions());
        }
        if (pi.getIpv4Packet().getPadding() != null) {
          ret.put(PCapConstants.Fields.IPHEADERPADDING, pi.getIpv4Packet().getPadding());
        }
      }

      /* TCP Header Data : */
      if (pi.getTcpPacket() != null) {
        if (pi.getTcpPacket().getSourceAddress() != null) {
          ret.put(PCapConstants.Fields.SRC_IP, pi.getTcpPacket().getSourceAddress().getHostAddress());
        }
        if (pi.getTcpPacket().getSource() != null) {
          ret.put(PCapConstants.Fields.SRC_PORT, pi.getTcpPacket().getSource().getPort());
        }
        if (pi.getTcpPacket().getDestinationAddress() != null) {
          ret.put(PCapConstants.Fields.DST_IP, pi.getTcpPacket().getDestinationAddress().getHostAddress());
        }
        if (pi.getTcpPacket().getDestination() != null) {
          ret.put(PCapConstants.Fields.DST_PORT, pi.getTcpPacket().getDestination().getPort());
        }
      }
    }
    return ret;
  }

  public static List<PacketInfo> toPacketInfo(byte[] packet) throws IOException {
    return toPacketInfo(ETHERNET_DECODER.get(), packet);
  }

  public static LogIslandEthernetDecoder createDecoder() {
    LogIslandEthernetDecoder ethernetDecoder = new LogIslandEthernetDecoder();
    IpDecoder ipDecoder = new IpDecoder();
    ethernetDecoder.register(EthernetType.IPV4, ipDecoder);
    return ethernetDecoder;
  }

/**
   * Parses the.
   *
   * @param pcap
   *          the pcap
   * @return the list * @throws IOException Signals that an I/O exception has
   *         occurred. * @throws IOException * @throws IOException * @throws
   *         IOException
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public static List<PacketInfo> toPacketInfo(LogIslandEthernetDecoder decoder, byte[] pcap) throws IOException {
    List<PacketInfo> packetInfoList = new ArrayList<>();

    PcapByteInputStream pcapByteInputStream = new PcapByteInputStream(pcap);

    GlobalHeader globalHeader = pcapByteInputStream.getGlobalHeader();
    while (true) {
      try

      {
        PcapPacket packet = pcapByteInputStream.getPacket();
        // int packetCounter = 0;
        // PacketHeader packetHeader = null;
        // Ipv4Packet ipv4Packet = null;
        TcpPacket tcpPacket = null;
        UdpPacket udpPacket = null;
        // Buffer packetDataBuffer = null;
        int sourcePort = 0;
        int destinationPort = 0;

        // LOG.trace("Got packet # " + ++packetCounter);

        // LOG.trace(packet.getPacketData());
        decoder.decode(packet);

        PacketHeader packetHeader = packet.getPacketHeader();
        Ipv4Packet ipv4Packet = Ipv4Packet.parse(packet.getPacketData());

        if (ipv4Packet.getProtocol() == Constants.PROTOCOL_TCP) {
          tcpPacket = TcpPacket.parse(ipv4Packet);

        }

        int totalLengthOfIPDatagram = ipv4Packet.getTotalLength();


        if (ipv4Packet.getProtocol() == Constants.PROTOCOL_UDP) {

          Buffer packetDataBuffer = ipv4Packet.getData();
          sourcePort = packetDataBuffer.getUnsignedShort();
          destinationPort = packetDataBuffer.getUnsignedShort();

          udpPacket = new UdpPacket(ipv4Packet, sourcePort, destinationPort);

          udpPacket.setLength(packetDataBuffer.getUnsignedShort());
          udpPacket.setChecksum(packetDataBuffer.getUnsignedShort());
          packetDataBuffer.discardReadBytes();
          udpPacket.setData(packetDataBuffer);
        }

        packetInfoList.add(new PacketInfo(globalHeader, packetHeader, packet,
            ipv4Packet, tcpPacket, udpPacket));
      } catch (NegativeArraySizeException ignored) {
        logger.debug("Ignorable exception while parsing packet.", ignored);
      } catch (EOFException eof) { // $codepro.audit.disable logExceptions
        // Ignore exception and break
        break;
      }
    }
    return packetInfoList;
  }

  public static List<JSONObject> toJSON(List<PacketInfo> packetInfoList) {
    List<JSONObject> messages = new ArrayList<>();
    for (PacketInfo packetInfo : packetInfoList) {
      JSONObject message = (JSONObject) JSONValue.parse(packetInfo.getJsonIndexDoc());
      messages.add(message);
    }
    return messages;
  }
}