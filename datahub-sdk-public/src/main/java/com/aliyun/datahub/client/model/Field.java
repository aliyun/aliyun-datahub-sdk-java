package com.aliyun.datahub.client.model;

public class Field {
    private String name;
    private FieldType type;
    private boolean allowNull = true;

    public Field(String name, FieldType type) {
        this.name = name.toLowerCase();
        this.type = type;
    }

    public Field(String name, FieldType type, boolean allowNull) {
        this.name = name.toLowerCase();
        this.type = type;
        this.allowNull = allowNull;
    }

    public String getName() {
        return name;
    }

    public FieldType getType() {
        return type;
    }

    public boolean isAllowNull() {
        return allowNull;
    }
}
