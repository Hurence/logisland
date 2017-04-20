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
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.ProcessError;
import com.hurence.logisland.processor.pcap.utils.Endianness;
import com.hurence.logisland.record.*;
import com.hurence.logisland.validator.StandardValidators;
import org.krakenapps.pcap.decoder.ip.Ipv4Packet;
import org.krakenapps.pcap.decoder.tcp.TcpPacket;
import org.krakenapps.pcap.decoder.udp.UdpPacket;
import org.krakenapps.pcap.file.GlobalHeader;
import org.krakenapps.pcap.packet.PacketHeader;
import org.krakenapps.pcap.packet.PcapPacket;
import org.krakenapps.pcap.util.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.util.*;

import static com.hurence.logisland.processor.pcap.PcapHelper.ETHERNET_DECODER;

/**
 * PCap processor
 */
@Tags({"PCap", "security", "IDS", "NIDS"})
@CapabilityDescription(
        "The PCap processor is the LogIsland entry point to parse network packets captured in pcap format. "
        +"The PCap processor decodes the bytes of the incoming pcap record, where a Global pcap header followed by a sequence of [packet header, packet data] pairs are stored. "
        +"Each incoming pcap event is parsed into n packet records where packet headers data is made available in dedicated fields."
    )

public class ParsePCap extends AbstractProcessor {

    private static Logger logger = LoggerFactory.getLogger(ParsePCap.class);

    private boolean debug = false;

    public static final String BATCH_FLOW_MODE = "batch";
    public static final String STREAM_FLOW_MODE = "stream";


    public static final PropertyDescriptor DEBUG_PROPERTY = new PropertyDescriptor.Builder()
            .name("debug")
            .description("Enable debug.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(false)
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor FLOW_MODE = new PropertyDescriptor.Builder()
            .name("flow.mode")
            .description("Flow Mode. Indicate whether packets are provided in batch mode (via pcap files) or in stream mode (without headers). Allowed values are " + BATCH_FLOW_MODE + " and " + STREAM_FLOW_MODE + ".")
            .allowableValues(BATCH_FLOW_MODE,STREAM_FLOW_MODE)
            .required(true)
            .build();

    @Override
    public void init(final ProcessContext context)
    {
        logger.debug("Initializing PCap Processor");
    }
    
    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(DEBUG_PROPERTY);
        descriptors.add(FLOW_MODE);
        return Collections.unmodifiableList(descriptors);
    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {

        if (debug) {
            logger.debug("PCap Processor records input: " + records);
        }

        final String flowMode = context.getPropertyValue(FLOW_MODE).asString();

        long threadId = Thread.currentThread().getId();

        Endianness endianness = Endianness.getNativeEndianness();

        /**
         * Get the original PCap event as Bytes and do some parsing
         */
        List<Record> outputRecords = new ArrayList<>();
        records.forEach(record -> {

            byte[] pcapRawValue = (byte[]) record.getField(FieldDictionary.RECORD_VALUE).getRawValue();

            //logger.debug("Length : " + pcapRawValue.length);
            /*
            String pcapString = "";
            for(int i = 0; i<pcapRawValue.length; i++)
            {
                pcapString = pcapString + ", " + pcapRawValue[i];
            }
            logger.debug("pcapString : " + pcapString);
            */

            try {
                LogIslandEthernetDecoder decoder = ETHERNET_DECODER.get();

                PcapByteInputStream pcapByteInputStream = null;
                GlobalHeader globalHeader = null;

                // Switch case is better here :
                switch (flowMode) {
                    case STREAM_FLOW_MODE:
                        // Create Global Header :
                        final int magicNumber = (endianness == Endianness.LITTLE) ? 0xD4C3B2A1 : 0xA1B2C3D4;
                        final short majorVersion = 2;
                        final short minorVersion = 4;
                        final int timezone = 0;
                        final int sigfigs = 0;
                        final int snaplen = 65535;
                        final int network = 1; // Link-layer header type values : http://www.tcpdump.org/linktypes.html
                        globalHeader = new GlobalHeader(magicNumber, majorVersion, minorVersion, timezone, sigfigs, snaplen, network);

                        // Retrieve the timestamp provided by the probe in the kafka message key :
                        final Long pcapTimestampInNanos = 1000000L * record.getField(FieldDictionary.RECORD_TIME).asLong();
                        //logger.debug("pcapTimestampInNanos : " + pcapTimestampInNanos.toString());

                        // Encapsulate the packet raw data with the packet header and the global header :
                        pcapRawValue = PcapHelper.addGlobalHeader(PcapHelper.addPacketHeader(pcapTimestampInNanos, pcapRawValue, Endianness.getNativeEndianness()), Endianness.getNativeEndianness());
                        pcapByteInputStream = new PcapByteInputStream(pcapRawValue);
                        break;
                    case BATCH_FLOW_MODE:
                        pcapByteInputStream = new PcapByteInputStream(pcapRawValue);
                        globalHeader = pcapByteInputStream.getGlobalHeader();
                        break;
                    default:
                        throw new Exception("The flow mode is not configured correctly.");
                }

                //logger.debug("Magic Number = " + globalHeader.getMagicNumber());

                //logger.debug("Message 1 - Thread Id = " + threadId);
                if (globalHeader.getMagicNumber() != 0xA1B2C3D4 && globalHeader.getMagicNumber() != 0xD4C3B2A1) {
                    logger.debug("Invalid pcap file format : Unable to parse the global header magic number - Thread Id : " + threadId);
                    throw new InvalidPCapFileException("Invalid pcap file format : Unable to parse the global header magic number - Thread Id : " + threadId);
                }

                while (true) {
                    try {
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
                        StandardRecord outputRecord = new StandardRecord();

                        outputRecord.setField(new Field(FieldDictionary.RECORD_TYPE, FieldType.STRING, "pcap_packet"));

                        outputRecord.setField(new Field(FieldDictionary.PROCESSOR_NAME, FieldType.STRING, this.getClass().getSimpleName()));
                        //logger.debug("Start Parsing - Step 1 - Thread Id = " + threadId);
                        if (ipv4Packet.getVersion() == Constants.PROTOCOL_IPV4) {
                            //logger.debug("Start Parsing - Step 2 : IPv4 parsing");
                            if (ipv4Packet.getProtocol() == Constants.PROTOCOL_TCP) {
                                //logger.debug("Start Parsing - Step 3 : TCP parsing");
                                tcpPacket = TcpPacket.parse(ipv4Packet);
                            } else if (ipv4Packet.getProtocol() == Constants.PROTOCOL_UDP) {
                                Buffer packetDataBuffer = ipv4Packet.getData();
                                sourcePort = packetDataBuffer.getUnsignedShort();
                                destinationPort = packetDataBuffer.getUnsignedShort();

                                udpPacket = new UdpPacket(ipv4Packet, sourcePort, destinationPort);

                                udpPacket.setLength(packetDataBuffer.getUnsignedShort());
                                udpPacket.setChecksum(packetDataBuffer.getUnsignedShort());
                                packetDataBuffer.discardReadBytes();
                                udpPacket.setData(packetDataBuffer);
                            } else {
                                logger.debug("//////////////////////////"
                                        + "Not Implemented protocol inside ipv4 packet : only TCP and UDP protocols are handled so far."
                                        + "//////////////////////////");
                                outputRecord.addError(ProcessError.NOT_IMPLEMENTED_ERROR.getName(), "Not Implemented protocol inside ipv4 packet : only TCP and UDP protocols are handled so far.");
                            }
                        } else {
                            logger.debug("//////////////////////////"
                                    + "Not Implemented protocol : only IPv4 protocol (TCP & UDP) is handled so far."
                                    + "//////////////////////////");
                            outputRecord.addError(ProcessError.NOT_IMPLEMENTED_ERROR.getName(), "Not Implemented protocol : only IPv4 protocol (TCP & UDP) is handled so far.");
                        }

                        //logger.debug("Start new PacketInfo");
                        PacketInfo pi = new PacketInfo(globalHeader, packetHeader, packet, ipv4Packet, tcpPacket, udpPacket);
                        //logger.debug("Start PcapHelper.packetToFields");
                        EnumMap<PCapConstants.Fields, Object> result = PcapHelper.packetToFields(pi);
                        //logger.debug("Start setting Fields");
                        for (PCapConstants.Fields field : PCapConstants.Fields.values()) {
                            if (result.containsKey(field)) {
                                //logger.debug("Adding field " + field.getName());
                                outputRecord.setField(new Field(field.getName(), field.getFieldType(), result.get(field)));
                            }
                        }

                        logger.debug("One packet record has been successfully added.");
                        outputRecords.add(outputRecord);

                    } catch (NegativeArraySizeException ignored) {
                        logger.debug("Ignorable exception while parsing packet.", ignored);
                    } catch (EOFException eof) {
                        // Ignore exception and break : the while loop is left when eof is reached
                        //logger.debug("Exit from the while loop");
                        break;
                    }
                }
            }
            catch (InvalidPCapFileException e) {
                StandardRecord outputRecord = new StandardRecord();
                outputRecord.addError(ProcessError.INVALID_FILE_FORMAT_ERROR.getName(), e.getMessage());
                outputRecord.setField(new Field(FieldDictionary.RECORD_VALUE, FieldType.BYTES, pcapRawValue));
                logger.debug("InvalidPCapFileException : error record added successfully.");
                outputRecords.add(outputRecord);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        logger.debug(outputRecords.size() + " packet records has been generated by the parsePCap processor.");
        return outputRecords;
    }

}
