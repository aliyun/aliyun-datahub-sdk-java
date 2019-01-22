package com.aliyun.datahub.common.data;

import java.util.List;

public enum FieldType {

    /**
     * 8字节有符号整型
     */
    BIGINT,

    /**
     * 8字节有符号整型
     */
    DOUBLE,

    /**
     * 可以表示为True/False，true/false, 0/1
     */
    BOOLEAN,
    /**
     * 日期类型
     */
    TIMESTAMP,

    /**
     * 字符串类型
     */
    STRING,

    /**
     * 精度类型
     */
    DECIMAL;

    public static String getFullTypeString(FieldType type, List<FieldType> genericTypeList) {
        StringBuilder sb = new StringBuilder();
        sb.append(type.toString());
        if (genericTypeList != null && genericTypeList.size() != 0) {
            sb.append("<");
            for (FieldType genericType : genericTypeList) {
                sb.append(genericType.toString()).append(",");
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.append(">");
        }
        return sb.toString();
    }
}
