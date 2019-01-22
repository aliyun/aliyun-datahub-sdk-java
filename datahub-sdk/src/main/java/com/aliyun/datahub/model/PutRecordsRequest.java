package com.aliyun.datahub.model;

import java.util.List;

public class PutRecordsRequest {

    private String projectName;
    private String topicName;
    private List<RecordEntry> records;

    public String getProjectName() {
        return projectName;
    }

    public String getTopicName() {
        return topicName;
    }

    public PutRecordsRequest(String projectName, String topicName, List<RecordEntry> records) {
        this.projectName = projectName;
        this.topicName = topicName;
        this.records = records;
    }

    public PutRecordsRequest(List<RecordEntry> records) {
        this.records = records;
    }

    public List<RecordEntry> getRecords() {
        return records;
    }

    public void setRecords(List<RecordEntry> records) {
        this.records = records;
    }

    public void addRecord(RecordEntry record) {
        this.records.add(record);
    }

}
