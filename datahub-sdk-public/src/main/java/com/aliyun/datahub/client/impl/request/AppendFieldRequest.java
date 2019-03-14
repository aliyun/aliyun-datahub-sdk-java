package com.aliyun.datahub.client.impl.request;

import com.aliyun.datahub.client.model.FieldType;
import com.fasterxml.jackson.annotation.JsonProperty;

public class AppendFieldRequest extends BaseRequest {
    @JsonProperty("FieldName")
    private String fieldName;

    @JsonProperty("FieldType")
    private FieldType fieldType;

    public AppendFieldRequest() {
        setAction("AppendField");
    }

    public String getFieldName() {
        return fieldName;
    }

    public AppendFieldRequest setFieldName(String fieldName) {
        this.fieldName = fieldName;
        return this;
    }

    public FieldType getFieldType() {
        return fieldType;
    }

    public AppendFieldRequest setFieldType(FieldType fieldType) {
        this.fieldType = fieldType;
        return this;
    }
}
