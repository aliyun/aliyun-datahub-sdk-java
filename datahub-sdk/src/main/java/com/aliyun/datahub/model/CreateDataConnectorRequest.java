package com.aliyun.datahub.model;

import com.aliyun.datahub.exception.InvalidParameterException;
import java.util.List;

public class CreateDataConnectorRequest {
    private String projectName;
    private String topicName;
    private ConnectorType type;
    private List<String> columnFields;
    private ConnectorConfig config;

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public ConnectorType getType() {
        return type;
    }

    public void setType(ConnectorType type) {
        this.type = type;
    }

    public List<String> getColumnFields() {
        return columnFields;
    }

    public void setColumnFields(List<String> columnFields) {
        this.columnFields = columnFields;
    }

    public ConnectorConfig getConfig() {
        return config;
    }

    public void setConfig(ConnectorConfig config) {
        this.config = config;
    }

    public CreateDataConnectorRequest(String projectName, String topicName,
                                      ConnectorType connectorType, List<String> columnFields,
                                      ConnectorConfig config) {
        super();
        checkParam(projectName, topicName, connectorType, config);
        if (columnFields == null) {
            throw new InvalidParameterException("columnFields is null");
        }

        this.projectName = projectName;
        this.topicName = topicName;
        this.columnFields = columnFields;
        this.config = config;
        this.type = connectorType;
    }

    public CreateDataConnectorRequest(String projectName, String topicName,
                                      ConnectorType connectorType, ConnectorConfig config) {
        super();
        checkParam(projectName, topicName, connectorType, config);
        this.projectName = projectName;
        this.topicName = topicName;
        this.type = connectorType;
        this.config = config;
        this.columnFields = null;
    }

    private void checkParam(String projectName, String topicName, ConnectorType connectorType, ConnectorConfig config) {
        if (projectName == null) {
            throw new InvalidParameterException("project name is null");
        }

        if (topicName == null) {
            throw new InvalidParameterException("topic name is null");
        }

        if (connectorType == null) {
            throw new InvalidParameterException("connectorType is null");
        }

        if (config == null) {
            throw new InvalidParameterException("config is null");
        }
    }
}
