package com.aliyun.datahub.client.http.provider.protobuf;

import com.aliyun.datahub.client.util.CrcUtils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class MessageOutputStream extends DataOutputStream {
    private boolean enablePbCrc;

    public MessageOutputStream(boolean enablePbCrc) {
        this(1024);
        this.enablePbCrc = enablePbCrc;
    }

    public MessageOutputStream(int capacity) {
        super(new FramedBuffer(capacity));
    }

    private FramedBuffer getFramedBuffer() {
        return (FramedBuffer)out;
    }

    /**
     * wrapped ByteArrayOutputStream
     */
    public byte[] toByteArray() {
        FramedBuffer buffer = getFramedBuffer();
        if (enablePbCrc) {
            buffer.doCrc();
        }
        return buffer.toByteArray();
    }

    public void writeTo(OutputStream out) throws IOException {
        getFramedBuffer().writeTo(out);
    }

    private static class FramedBuffer extends ByteArrayOutputStream {
        private static final int FRAME_HEADER_SIZE = 8;
        private static final int FRAME_PACK_PAD_SIZE = 12;

        public FramedBuffer(int capacity) {
            super(capacity + FRAME_HEADER_SIZE);
            reset();
        }

        @Override
        public synchronized void write(int b) {
            super.write(b);
        }

        @Override
        public synchronized void writeTo(OutputStream out) throws IOException {
            super.writeTo(out);
        }

        @Override
        public synchronized void reset() {
            write("DHUB".getBytes(), 0, 4);
            setCrc32(0);
            count = FRAME_HEADER_SIZE;
        }

        public void doCrc() {
            setCrc32(CrcUtils.getCrc32(buf, FRAME_PACK_PAD_SIZE, count - FRAME_PACK_PAD_SIZE));
        }

        public void setCrc32(int crc32) {
            buf[4] = (byte)((crc32 >>> 24) & 0xFF);
            buf[5] = (byte)((crc32 >>> 16) & 0xFF);
            buf[6] = (byte)((crc32 >>>  8) & 0xFF);
            buf[7] = (byte)(crc32 & 0xFF);
        }
    }
}