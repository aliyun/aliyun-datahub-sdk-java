package com.aliyun.datahub.model;

import com.aliyun.datahub.exception.InvalidParameterException;

import java.util.List;
@Deprecated
public class CreateConnectorRequest {
    private String projectName;
    private String topicName;
    private ConnectorType type;
    private List<String> columnFields;
    private OdpsDesc odpsDesc;

    public String getProjectName() {
        return projectName;
    }

    public String getTopicName() {
        return topicName;
    }

    public List<String> getColumnFields() {
        return columnFields;
    }

    public OdpsDesc getOdpsDesc() {
        return odpsDesc;
    }

    public CreateConnectorRequest(String projectName, String topicName,
                                  ConnectorType connectorType, List<String> columnFields,
                                  OdpsDesc odpsDesc) {
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

        if (columnFields == null) {
            throw new InvalidParameterException("columnFields is null");
        }

        if (odpsDesc == null) {
            throw new InvalidParameterException("odpsDesc is null");
        }

        this.projectName = projectName;
        this.topicName = topicName;
        this.columnFields = columnFields;
        this.odpsDesc = odpsDesc;
        //TODO
        this.type = connectorType;
    }

    public ConnectorType getType() {
        return type;
    }

    public void setType(ConnectorType type) {
        this.type = type;
    }
}
