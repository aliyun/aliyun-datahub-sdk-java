package com.aliyun.datahub.client.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class GetConnectorShardStatusResult extends BaseResult {

    @JsonProperty("ShardStatusInfos")
    private Map<String, ConnectorShardStatusEntry> statusEntryMap;

    public Map<String, ConnectorShardStatusEntry> getStatusEntryMap() {
        return statusEntryMap;
    }

    public void setStatusEntryMap(Map<String, ConnectorShardStatusEntry> statusEntryMap) {
        this.statusEntryMap = statusEntryMap;
    }
}
