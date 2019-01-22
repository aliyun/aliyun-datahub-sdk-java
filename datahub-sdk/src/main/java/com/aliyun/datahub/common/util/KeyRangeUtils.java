package com.aliyun.datahub.common.util;

import com.aliyun.datahub.exception.DatahubClientException;

import java.math.BigInteger;
import java.security.MessageDigest;

public class KeyRangeUtils {
    public static String trivialSplit(String beginKey, String endKey) throws DatahubClientException {
        return bigIntToHash(hashKeyToBigInt(beginKey).add(hashKeyToBigInt(endKey)).divide(new BigInteger("2")));
    }

    public static BigInteger hashKeyToBigInt(String hashKey) throws DatahubClientException {
        if (hashKey.length() != 32) {
            throw new DatahubClientException("Invalid Hash Key Range.");
        }
        BigInteger keyRange = new BigInteger(hashKey, 16);
        return keyRange;
    }

    public static String bigIntToHash(BigInteger keyValue) {
        String hashKey = keyValue.toString(16);
        if (hashKey.length() > 32) {
            throw new DatahubClientException("Invalid value.");
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 32 - hashKey.length(); ++i) {
            sb.append("0");
        }
        sb.append(hashKey);
        return sb.toString().toUpperCase();
    }

    public static String md5Signature(String message) {

        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] array = md.digest(message.getBytes());
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < array.length; ++i) {
                sb.append(Integer.toHexString((array[i] & 0xFF) | 0x100).substring(1, 3));
            }
            return sb.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
        }
        return null;

    }
}
