package com.aliyun.datahub.client.model;

import com.fasterxml.jackson.annotation.JsonValue;

public enum SubscriptionState {
    OFFLINE,
    ONLINE;

    @JsonValue
    public int getValue() {
        return ordinal();
    }
}
