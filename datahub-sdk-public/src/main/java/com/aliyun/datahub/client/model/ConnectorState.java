package com.aliyun.datahub.client.model;

import com.fasterxml.jackson.annotation.JsonValue;

public enum ConnectorState {
    @Deprecated CREATED("CONNECTOR_CREATED"),
    STOPPED("CONNECTOR_PAUSED"),
    RUNNING("CONNECTOR_RUNNING");

    ConnectorState(String value) {
        this.value = value;
    }

    private String value;

    @JsonValue
    public String getValue() {
        return value;
    }

    public static ConnectorState fromString(String text) {
        for (ConnectorState e : ConnectorState.values()) {
            if (e.value.equalsIgnoreCase(text)) {
                return e;
            }
        }
        return null;
    }
}
