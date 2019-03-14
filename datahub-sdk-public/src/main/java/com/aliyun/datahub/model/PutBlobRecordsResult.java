package com.aliyun.datahub.model;

import com.aliyun.datahub.client.model.PutErrorEntry;
import com.aliyun.datahub.utils.ModelConvertToNew;
import com.aliyun.datahub.utils.ModelConvertToOld;

import java.util.ArrayList;
import java.util.List;

public class PutBlobRecordsResult {
    private com.aliyun.datahub.client.model.PutRecordsResult proxyResult;

    public PutBlobRecordsResult(com.aliyun.datahub.client.model.PutRecordsResult proxyResult) {
        this.proxyResult = proxyResult;
        setRequestId(proxyResult.getRequestId());
    }

    private String requestId;

    public PutBlobRecordsResult() {
        proxyResult = new com.aliyun.datahub.client.model.PutRecordsResult();
    }

    public int getFailedRecordCount() {
        return proxyResult.getFailedRecordCount();
    }

    public void setFailedRecordCount(int failedRecordCount) {
        proxyResult.setFailedRecordCount(failedRecordCount);
    }

    public void addFailedRecord(BlobRecordEntry recordEntry) {
        com.aliyun.datahub.client.model.RecordEntry newEntry = ModelConvertToNew.convertBlobRecordEntry(recordEntry);
        List<com.aliyun.datahub.client.model.RecordEntry> failedRecords = proxyResult.getFailedRecords();
        if (failedRecords == null) {
            failedRecords = new ArrayList<>();
            proxyResult.setFailedRecords(failedRecords);
        }
        failedRecords.add(newEntry);
    }

    public List<BlobRecordEntry> getFailedRecords() {
        List<BlobRecordEntry> recordEntries = new ArrayList<>();
        List<com.aliyun.datahub.client.model.RecordEntry> newEntryList = proxyResult.getFailedRecords();
        if (newEntryList != null) {
            for (com.aliyun.datahub.client.model.RecordEntry entry : newEntryList) {
                BlobRecordEntry oldEntry = ModelConvertToOld.convertBlobRecordEntry(entry);
                recordEntries.add(oldEntry);
            }
        }

        return recordEntries;
    }

    public void addFailedIndex(int index) {
        // nothing
    }

    public List<Integer> getFailedRecordIndex() {
        List<Integer> failedIndexs = new ArrayList<>();
        if (proxyResult.getPutErrorEntries() != null) {
            for (com.aliyun.datahub.client.model.PutErrorEntry entry : proxyResult.getPutErrorEntries()) {
                failedIndexs.add(entry.getIndex());
            }
        }
        return failedIndexs;
    }

    public void addFailedError(ErrorEntry error) {
        com.aliyun.datahub.client.model.PutErrorEntry putErrorEntry = new com.aliyun.datahub.client.model.PutErrorEntry();
        putErrorEntry.setErrorcode(error.getErrorcode());
        putErrorEntry.setMessage(error.getMessage());

        List<PutErrorEntry> putErrorEntryList = proxyResult.getPutErrorEntries();
        if (putErrorEntryList == null) {
            putErrorEntryList = new ArrayList<>();
            proxyResult.setPutErrorEntries(putErrorEntryList);
        }
        putErrorEntryList.add(putErrorEntry);
    }

    public List<ErrorEntry> getFailedRecordError() {
        List<ErrorEntry> oldEntryList = new ArrayList<>();
        if (proxyResult.getPutErrorEntries() != null) {
            for (com.aliyun.datahub.client.model.PutErrorEntry entry : proxyResult.getPutErrorEntries()) {
                ErrorEntry oldEntry = new ErrorEntry(entry.getErrorcode(), entry.getMessage());
                oldEntryList.add(oldEntry);
            }
        }
        return oldEntryList;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }
}
