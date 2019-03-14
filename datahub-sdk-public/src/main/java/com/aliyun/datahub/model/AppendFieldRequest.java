package com.aliyun.datahub.model;

import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.exception.InvalidParameterException;

public class AppendFieldRequest {
    private String projectName;
    private String topicName;
    private Field field;

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

    public Field getField() {
        return field;
    }

    public void setField(Field field) {
        if (field.getNotnull()) {
            throw new InvalidParameterException("append field must allow null value");
        }
        this.field = field;
    }

    public AppendFieldRequest(String projectName, String topicName, Field field) {

        if (projectName == null ) {
            throw new InvalidParameterException("project name is null");
        }

        if (topicName == null) {
            throw new InvalidParameterException("topic name is null");
        }

        if (field == null) {
            throw new InvalidParameterException("field is null");
        }

        if (field.getNotnull()) {
            throw new InvalidParameterException("append field must allow null value");
        }

        this.projectName = projectName;
        this.topicName = topicName;
        this.field = field;
    }
}
