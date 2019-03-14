package com.aliyun.datahub.model;

import com.aliyun.datahub.DatahubConstants;

import java.util.ArrayList;
import java.util.List;

public class PutRecordsRequest {

    private String projectName;
    private String topicName;
    private List<RecordEntry> records;
    private long requestLimitSize;
    private long approxSize;

    public PutRecordsRequest(String projectName, String topicName) {
        this.projectName = projectName;
        this.topicName = topicName;
        this.records = new ArrayList<RecordEntry>();
        this.requestLimitSize = DatahubConstants.DefaultLimitSize;
        this.approxSize = 0;
    }

    public PutRecordsRequest(String projectName, String topicName, List<RecordEntry> records) {
        this.projectName = projectName;
        this.topicName = topicName;
        this.records = records;
        this.requestLimitSize = DatahubConstants.DefaultLimitSize;
        this.approxSize = getRecordSize(records);
    }

    public PutRecordsRequest(String projectName, String topicName, List<RecordEntry> records, long limitSize) {
        this.projectName = projectName;
        this.topicName = topicName;
        this.records = records;
        this.requestLimitSize = limitSize;
        this.approxSize = getRecordSize(records);
    }

    private long getRecordSize(List<RecordEntry> recordEntries) {
        long size = 0;
        for (RecordEntry recordEntry: recordEntries) {
            size += recordEntry.getRecordSize();
        }
        return size;
    }

    public PutRecordsRequest(List<RecordEntry> records) {
        this.records = records;
    }

    public String getProjectName() {
        return projectName;
    }

    public String getTopicName() {
        return topicName;
    }

    public List<RecordEntry> getRecords() {
        return records;
    }

    public void setRecords(List<RecordEntry> records) {
        this.records = records;
    }

    @Deprecated
    public void addRecord(RecordEntry record) {
        this.records.add(record);
    }

    public boolean appendRecord(RecordEntry record) {
        long recordSize = record.getRecordSize();
        if (approxSize + recordSize > requestLimitSize * DatahubConstants.DefaultLimitPercent) {
            return false;
        }
        records.add(record);
        approxSize += recordSize;
        return true;
    }

    public long getRequestLimitSize() {
        return requestLimitSize;
    }

    public void setRequestLimitSize(long requestLimitSize) {
        this.requestLimitSize = requestLimitSize;
    }

    public long getApproxSize() {
        return approxSize;
    }
}
