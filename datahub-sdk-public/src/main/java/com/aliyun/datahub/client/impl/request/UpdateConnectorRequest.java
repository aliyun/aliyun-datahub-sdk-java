package com.aliyun.datahub.client.impl.request;

import com.aliyun.datahub.client.model.SinkConfig;
import com.fasterxml.jackson.annotation.JsonProperty;

public class UpdateConnectorRequest extends BaseRequest {
    @JsonProperty("Config")
    private SinkConfig config;

    public UpdateConnectorRequest() {
        setAction("updateconfig");
    }

    public SinkConfig getConfig() {
        return config;
    }

    public UpdateConnectorRequest setConfig(SinkConfig config) {
        this.config = config;
        return this;
    }
}
