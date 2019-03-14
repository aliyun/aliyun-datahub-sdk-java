package com.aliyun.datahub.client.model;

public enum FieldType {
    /**
     * 8-byte signed integer
     */
    BIGINT,

    /**
     * Double type
     */
    DOUBLE,

    /**
     * Boolean type, for instance: True/False, true/false, 0/1
     */
    BOOLEAN,
    /**
     * Timestamp, Max:253402271999000000 Min:-62135798400000000
     */
    TIMESTAMP,

    /**
     * String type
     */
    STRING,

    /**
     * Decimal type
     */
    DECIMAL;
}
