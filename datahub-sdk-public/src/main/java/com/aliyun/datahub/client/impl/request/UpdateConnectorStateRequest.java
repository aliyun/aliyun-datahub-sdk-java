package com.aliyun.datahub.client.impl.request;

import com.aliyun.datahub.client.model.ConnectorState;
import com.fasterxml.jackson.annotation.JsonProperty;

public class UpdateConnectorStateRequest extends BaseRequest {
    @JsonProperty("State")
    private ConnectorState state;

    public UpdateConnectorStateRequest() {
        setAction("updatestate");
    }

    public ConnectorState getState() {
        return state;
    }

    public UpdateConnectorStateRequest setState(ConnectorState state) {
        this.state = state;
        return this;
    }
}
