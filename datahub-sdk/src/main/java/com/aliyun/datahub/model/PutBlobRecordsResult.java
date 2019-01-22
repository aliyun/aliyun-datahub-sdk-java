package com.aliyun.datahub.model;

import java.util.ArrayList;
import java.util.List;

public class PutBlobRecordsResult {

    private String requestId;

    private int failedRecordCount;

    private List<Integer> failedRecordIndex;

    private List<ErrorEntry> failedRecordError;

    private List<BlobRecordEntry> failedRecordList;

    public PutBlobRecordsResult() {
        this.failedRecordCount = 0;
        this.failedRecordIndex = new ArrayList<Integer>();
        this.failedRecordError = new ArrayList<ErrorEntry>();
        this.failedRecordList = new ArrayList<BlobRecordEntry>();
    }

    public int getFailedRecordCount() {
        return failedRecordCount;
    }

    public void setFailedRecordCount(int failedRecordCount) {
        this.failedRecordCount = failedRecordCount;
    }

    public void addFailedRecord(BlobRecordEntry recordEntry) {
        this.failedRecordList.add(recordEntry);
    }

    public List<BlobRecordEntry> getFailedRecords() {
        return this.failedRecordList;
    }

    public void addFailedIndex(int index) {
        this.failedRecordIndex.add(index);
    }

    public List<Integer> getFailedRecordIndex() {
        return this.failedRecordIndex;
    }

    public void addFailedError(ErrorEntry error) {
        this.failedRecordError.add(error);
    }

    public List<ErrorEntry> getFailedRecordError() {
        return this.failedRecordError;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }
}
