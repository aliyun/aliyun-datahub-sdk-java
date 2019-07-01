package com.aliyun.datahub.client.e2e.common;

import com.aliyun.datahub.client.exception.InvalidParameterException;

import java.util.Properties;

public class Configure {
    private static Properties properties = null;

    static {
        properties = new Properties();
        try {
            properties.load(Configure.class.getClassLoader().
                    getResourceAsStream("config.properties"));
        } catch (Exception e) {
            System.out.println("加载配置文件失败. error:" + e.getMessage());
            System.exit(1);
        }
    }

    public static String getString(String key) {
        String value =  properties.getProperty(key);
        if (value == null || value.length() == 0) {
            throw new InvalidParameterException(key + " is missed");
        }

        return value.trim();
    }

    public static String getString(String key, String defaultValue) {
        String value =  properties.getProperty(key);
        if (value == null || value.length() == 0) {
            return defaultValue;
        }

        return value.trim();
    }

    public static int getInteger(String key) {
        String value =  properties.getProperty(key);
        if (value == null || value.length() == 0) {
            throw new InvalidParameterException(key + " is missed");
        }

        return Integer.parseInt(value.trim());
    }

    public static int getInteger(String key, int defaultValue) {
        String value =  properties.getProperty(key);
        if (value == null || value.length() == 0) {
            return defaultValue;
        }

        return Integer.parseInt(value.trim());
    }

    public static boolean getBoolean(String key, boolean defaultValue) {
        String value =  properties.getProperty(key);
        if (value == null || value.length() == 0) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value);
    }
}
