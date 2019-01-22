package com.aliyun.datahub.common.data;

public class Field {
    private String name;
    private FieldType type;
    private boolean notnull;

    /**
     * 构造Field对象
     *
     * @param name 列名
     * @param type 列类型
     */
    public Field(String name, FieldType type) {
        this.name = name.toLowerCase();
        this.type = type;
        this.notnull = false;
    }

    /**
     * 构造Field对象, 包含not null约束
     *
     * @param name 字段名
     * @param type 字段类型
     * @param notnull 约束条件, 是否允许not null
     */
    public Field(String name, FieldType type, boolean notnull) {
        this.name = name;
        this.type = type;
        this.notnull = notnull;
    }

    public String getName() {
        return name;
    }

    /**
     * 获得列类型
     *
     * @return 列类型 FieldType}
     */
    public FieldType getType() {
        return type;
    }

    /**
     * 获取notnull约束
     * @return 是否有notnull约束
     */
    public boolean getNotnull() {
        return notnull;
    }
}
