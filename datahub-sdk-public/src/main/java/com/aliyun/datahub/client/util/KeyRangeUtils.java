package com.aliyun.datahub.client.util;


import java.math.BigInteger;

public abstract class KeyRangeUtils {
    public static String trivialSplit(String beginKey, String endKey) throws Exception {
        return bigIntToHash(
                    hashKeyToBigInt(beginKey).add(hashKeyToBigInt(endKey)).divide(new BigInteger("2")));
    }

    private static BigInteger hashKeyToBigInt(String hashKey) throws Exception {
        if (hashKey.length() != 32) {
            throw new Exception("Invalid Hash Key Range.");
        }
        return new BigInteger(hashKey, 16);
    }

    private static String bigIntToHash(BigInteger keyValue) throws Exception {
        String hashKey = keyValue.toString(16);
        if (hashKey.length() > 32) {
            throw new Exception("Invalid value.");
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 32 - hashKey.length(); ++i) {
            sb.append("0");
        }
        sb.append(hashKey);
        return sb.toString().toUpperCase();
    }
}
