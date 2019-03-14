package com.aliyun.datahub.client.util;

import com.aliyun.datahub.client.exception.DatahubClientException;

import java.util.zip.Checksum;

public abstract class CrcUtils {
    private static final ThreadLocal<Checksum> checksumThreadLocal = new ThreadLocal<Checksum>() {
        @Override
        protected Checksum initialValue() {
            return new PureJavaCrc32C();
        }
    };

    public static int getCrc32(byte[] payload) throws DatahubClientException {
        return getCrc32(payload, 0, payload.length);
    }

    public static int getCrc32(byte[] payload, int offset, int length) throws DatahubClientException {
        if (payload == null) {
            throw new DatahubClientException("Bytes is null. can't cal crc value");
        }
        Checksum checksum = checksumThreadLocal.get();
        checksum.reset();
        checksum.update(payload, offset, length);
        return (int)checksum.getValue();
    }
}
