package com.aliyun.datahub.model;

import java.util.List;

public class PutBlobRecordsRequest {

    private String projectName;
    private String topicName;
    private List<BlobRecordEntry> records;

    public String getProjectName() {
        return projectName;
    }

    public String getTopicName() {
        return topicName;
    }

    public PutBlobRecordsRequest(String projectName, String topicName, List<BlobRecordEntry> records) {
        this.projectName = projectName;
        this.topicName = topicName;
        this.records = records;
    }

    public PutBlobRecordsRequest(List<BlobRecordEntry> records) {
        this.records = records;
    }

    public List<BlobRecordEntry> getRecords() {
        return records;
    }

    public void setRecords(List<BlobRecordEntry> records) {
        this.records = records;
    }

    public void addRecord(BlobRecordEntry record) {
        this.records.add(record);
    }

}
