package com.aliyun.datahub.client.http.converter;

import com.aliyun.datahub.client.util.CrcUtils;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

public class MessageInputStream extends DataInputStream {
    public MessageInputStream(InputStream in, boolean enablePbCrc) throws IOException {
        super(new FramedBuffer(in, enablePbCrc));
    }

    private static class FramedBuffer extends InputStream {

        private ByteArrayInputStream innerBuffer;

        public FramedBuffer(InputStream inputStream, boolean enablePbCrc) throws IOException {
            DataInputStream dis = new DataInputStream(inputStream);
            byte[] buffer = new byte[4];
            int pos = 0;
            while (pos < 4) {
                int len = inputStream.read(buffer, pos, 4 - pos);
                if (len == -1) {
                    throw new IOException("Read pb fail");
                }
                pos += len;
            }
            if (pos != 4 || !Arrays.equals(buffer, "DHUB".getBytes())) {
                throw new IOException("Pb magic is invalid. magic" + new String(buffer));
            }

            int crc32 = dis.readInt();
            int dataLength =  dis.readInt();
            buffer = new byte[dataLength];

            pos = 0;
            while (pos < dataLength) {
                int len = dis.read(buffer, pos, dataLength - pos);
                if (len == -1) {
                    throw new IOException("Read pb fail");
                }
                pos += len;
            }

            if (enablePbCrc && crc32 != 0) {
                int compute = CrcUtils.getCrc32(buffer);
                if (crc32 != compute) {
                    throw new IOException("Check Crc fail. read:" + crc32 + ", compute:" + compute);
                }
            }

            innerBuffer = new ByteArrayInputStream(buffer);
        }

        public synchronized int read() {
            return innerBuffer.read();
        }

        public synchronized int read(byte[] b, int off, int len) {
            return innerBuffer.read(b, off, len);
        }

        public synchronized long skip(long n) {
            return innerBuffer.skip(n);
        }

        public synchronized int available() {
            return innerBuffer.available();
        }

        public boolean markSupported() {
            return innerBuffer.markSupported();
        }

        public void mark(int readAheadLimit) {
            innerBuffer.mark(readAheadLimit);
        }

        public synchronized void reset() {
            innerBuffer.reset();
        }
    }
}
