package com.aliyun.datahub.client.impl.request;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AppendConnectorFieldRequest extends BaseRequest {
    @JsonProperty("FieldName")
    private String fieldName;

    public AppendConnectorFieldRequest() {
        setAction("appendfield");
    }

    public String getFieldName() {
        return fieldName;
    }

    public AppendConnectorFieldRequest setFieldName(String fieldName) {
        this.fieldName = fieldName;
        return this;
    }
}
