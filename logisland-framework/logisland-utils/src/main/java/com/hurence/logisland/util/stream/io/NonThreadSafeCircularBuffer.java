package com.hurence.logisland.util.stream.io;

import java.util.Arrays;

public class NonThreadSafeCircularBuffer {

    private final byte[] lookingFor;
    private final int[] buffer;
    private int insertionPointer = 0;
    private int bufferSize = 0;

    public NonThreadSafeCircularBuffer(final byte[] lookingFor) {
        this.lookingFor = lookingFor;
        buffer = new int[lookingFor.length];
        Arrays.fill(buffer, -1);
    }

    public byte[] getByteArray() {
        return lookingFor;
    }

    /**
     * Returns the oldest byte in the buffer
     *
     * @return the oldest byte
     */
    public int getOldestByte() {
        return buffer[insertionPointer];
    }

    public boolean isFilled() {
        return bufferSize >= buffer.length;
    }

    public boolean addAndCompare(final byte data) {
        buffer[insertionPointer] = data;
        insertionPointer = (insertionPointer + 1) % lookingFor.length;

        bufferSize++;
        if (bufferSize < lookingFor.length) {
            return false;
        }

        for (int i = 0; i < lookingFor.length; i++) {
            final byte compare = (byte) buffer[(insertionPointer + i) % lookingFor.length];
            if (compare != lookingFor[i]) {
                return false;
            }
        }

        return true;
    }
}
