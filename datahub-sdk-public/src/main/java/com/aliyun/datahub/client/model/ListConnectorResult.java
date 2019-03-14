package com.aliyun.datahub.client.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class ListConnectorResult extends BaseResult {
    @JsonProperty("Connectors")
    private List<String> connectorNames;

    public List<String> getConnectorNames() {
        return connectorNames;
    }

    public void setConnectorNames(List<String> connectorNames) {
        this.connectorNames = connectorNames;
    }
}
