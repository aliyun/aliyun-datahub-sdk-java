package com.aliyun.datahub.model;

import com.aliyun.datahub.exception.InvalidParameterException;

public class AppendDataConnectorFieldRequest {
    private String projectName;
    private String topicName;
    private ConnectorType connectorType;
    private String fieldName;

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

    public ConnectorType getConnectorType() {
        return connectorType;
    }

    public void setConnectorType(ConnectorType connectorType) {
        this.connectorType = connectorType;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public AppendDataConnectorFieldRequest(String projectName, String topicName,
                                           ConnectorType connectorType, String fieldName) {
        super();

        if (projectName == null) {
            throw new InvalidParameterException("project name is null");
        }

        if (topicName == null) {
            throw new InvalidParameterException("topic name is null");
        }

        if (connectorType == null) {
            throw new InvalidParameterException("connectorType is null");
        }

        if (fieldName == null) {
            throw new InvalidParameterException("fieldName is null");
        }

        this.projectName = projectName;
        this.topicName = topicName;
        this.connectorType = connectorType;
        this.fieldName = fieldName;
    }
}
