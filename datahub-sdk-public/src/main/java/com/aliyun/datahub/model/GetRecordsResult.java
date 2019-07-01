package com.aliyun.datahub.model;

import com.aliyun.datahub.utils.ModelConvertToNew;
import com.aliyun.datahub.utils.ModelConvertToOld;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 * Represents the output for <a>GetRecords</a>.
 * 
 */
public class GetRecordsResult extends Result {
    private com.aliyun.datahub.client.model.GetRecordsResult proxyResult;

    public GetRecordsResult(com.aliyun.datahub.client.model.GetRecordsResult proxyResult) {
        this.proxyResult = proxyResult;
        setRequestId(proxyResult.getRequestId());
    }

    public GetRecordsResult() {
        proxyResult = new com.aliyun.datahub.client.model.GetRecordsResult();
    }

    /**
     * The start sequence of the first record.
     * If no records in result object, then startSeq is -1.
     */

    public List<RecordEntry> getRecords() {
        List<RecordEntry> recordEntryList = new ArrayList<>();
        long recordIndex = 0;
        if (proxyResult.getRecords() != null) {
            for (com.aliyun.datahub.client.model.RecordEntry entry : proxyResult.getRecords()) {
                long sequence = proxyResult.getStartSequence() + recordIndex++;
                RecordEntry oldEntry = ModelConvertToOld.convertRecordEntry(entry, sequence);
                recordEntryList.add(oldEntry);
            }
        }
        return recordEntryList;
    }

    public void setRecords(List<RecordEntry> records) {
        List<com.aliyun.datahub.client.model.RecordEntry> newRecordEntryList = new ArrayList<>();
        for (RecordEntry entry : records) {
            com.aliyun.datahub.client.model.RecordEntry newEntry = ModelConvertToNew.convertRecordEntry(entry);
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
