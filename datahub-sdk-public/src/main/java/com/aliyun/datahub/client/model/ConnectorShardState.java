package com.aliyun.datahub.client.model;

import com.fasterxml.jackson.annotation.JsonValue;

public enum ConnectorShardState {
    CREATED("CONTEXT_PLANNED"),
    EXECUTING("CONTEXT_EXECUTING"),

    @Deprecated
    HANGE("CONTEXT_HANG"),

    STOPPED("CONTEXT_PAUSED"),

    FINISHED("CONTEXT_FINISHED");

    ConnectorShardState(String value) {
        this.value = value;
    }

    private String value;

    @JsonValue
    public String getValue() {
        return value;
    }

    public static ConnectorShardState fromString(String text) {
        for (ConnectorShardState e : ConnectorShardState.values()) {
            if (e.value.equalsIgnoreCase(text)) {
                return e;
            }
        }
        return null;
    }
}
