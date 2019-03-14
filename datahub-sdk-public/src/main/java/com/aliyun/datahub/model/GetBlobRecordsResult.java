package com.aliyun.datahub.model;

import com.aliyun.datahub.utils.ModelConvertToNew;
import com.aliyun.datahub.utils.ModelConvertToOld;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 * Represents the output for <a>GetBlobRecords</a>.
 * 
 */
public class GetBlobRecordsResult extends Result {
    private com.aliyun.datahub.client.model.GetRecordsResult proxyResult;

    public GetBlobRecordsResult(com.aliyun.datahub.client.model.GetRecordsResult proxyResult) {
        this.proxyResult = proxyResult;
        setRequestId(proxyResult.getRequestId());
    }

    public GetBlobRecordsResult() {
        proxyResult = new com.aliyun.datahub.client.model.GetRecordsResult();
    }

//    private List<BlobRecordEntry> records;

    public List<BlobRecordEntry> getRecords() {
        List<BlobRecordEntry> recordEntryList = new ArrayList<>();
        if (proxyResult.getRecords() != null) {
            for (com.aliyun.datahub.client.model.RecordEntry entry : proxyResult.getRecords()) {
                BlobRecordEntry oldEntry = ModelConvertToOld.convertBlobRecordEntry(entry);
                recordEntryList.add(oldEntry);
            }
        }
        return recordEntryList;
    }

    public void setRecords(List<BlobRecordEntry> records) {
        List<com.aliyun.datahub.client.model.RecordEntry> newRecordEntryList = new ArrayList<>();
        for (BlobRecordEntry entry : records) {
            com.aliyun.datahub.client.model.RecordEntry newEntry = ModelConvertToNew.convertBlobRecordEntry(entry);
            newRecordEntryList.add(newEntry);
        }
        proxyResult.setRecords(newRecordEntryList);
    }

    public String getNextCursor() {
        return proxyResult.getNextCursor();
    }

    public void setNextCursor(String nextCursor) {
        proxyResult.setNextCursor(nextCursor);
    }

    public int getRecordCount() {
        return proxyResult.getRecordCount();
    }
    
    public long getStartSeq() {
        return proxyResult.getStartSequence();
    }

    public void setStartSeq(long startSeq) {
        proxyResult.setStartSequence(startSeq);
    }
}
