package com.aliyun.datahub.client.util;

import java.math.BigDecimal;

public abstract class ValueCheckUtils {
    public static boolean checkString(Object value) {
        return (value instanceof String);
    }

    public static boolean checkBigint(Object value) {
        return ((value instanceof Long) || (value instanceof Integer));
    }

    public static boolean checkDouble(Object value) {
        return (value instanceof Double);
    }

    public static boolean checkBoolean(Object value) {
        return (value instanceof Boolean);
    }

    public static boolean checkTimestamp(Object value) {
        return checkBigint(value);
    }

    public static boolean checkDecimal(Object value) {
        return (value instanceof BigDecimal);
    }
}
