package com.aliyun.datahub.client.impl.request;

import com.aliyun.datahub.client.model.ConnectorType;
import com.aliyun.datahub.client.model.SinkConfig;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class CreateConnectorRequest extends BaseRequest {
    @JsonProperty("Type")
    private ConnectorType type;

    @JsonProperty("SinkStartTime")
    private long sinkStartTime;

    @JsonProperty("ColumnFields")
    private List<String> columnFields;

    @JsonProperty("Config")
    private SinkConfig config;

    public CreateConnectorRequest() {
        setAction("Create");
    }

    public ConnectorType getType() {
        return type;
    }

    public CreateConnectorRequest setType(ConnectorType type) {
        this.type = type;
        return this;
    }

    public long getSinkStartTime() {
        return sinkStartTime;
    }

    public CreateConnectorRequest setSinkStartTime(long sinkStartTime) {
        this.sinkStartTime = sinkStartTime;
        return this;
    }

    public List<String> getColumnFields() {
        return columnFields;
    }

    public CreateConnectorRequest setColumnFields(List<String> columnFields) {
        this.columnFields = columnFields;
        return this;
    }

    public SinkConfig getConfig() {
        return config;
    }

    public CreateConnectorRequest setConfig(SinkConfig config) {
        this.config = config;
        return this;
    }
}
