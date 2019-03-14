package com.aliyun.datahub.model;

import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.common.data.RecordType;
import com.aliyun.datahub.utils.ModelConvertToNew;
import com.aliyun.datahub.utils.ModelConvertToOld;

public class GetTopicResult extends Result {
    private com.aliyun.datahub.client.model.GetTopicResult proxyResult;

    public GetTopicResult() {
        proxyResult = new com.aliyun.datahub.client.model.GetTopicResult();
    }

    public GetTopicResult(com.aliyun.datahub.client.model.GetTopicResult proxyResult) {
        this.proxyResult = proxyResult;
        setRequestId(proxyResult.getRequestId());
    }

    public String getProjectName() {
        return proxyResult.getProjectName();
    }

    public void setProjectName(String projectName) {
        proxyResult.setProjectName(projectName);
    }

    public String getTopicName() {
        return proxyResult.getTopicName();
    }

    public void setTopicName(String topicName) {
        proxyResult.setTopicName(topicName);
    }

    public int getShardCount() {
        return proxyResult.getShardCount();
    }

    public void setShardCount(int shardCount) {
        proxyResult.setShardCount(shardCount);
    }

    public int getLifeCycle() {
        return proxyResult.getLifeCycle();
    }

    public void setLifeCycle(int lifeCycle) {
        proxyResult.setLifeCycle(lifeCycle);
    }

    public RecordType getRecordType() {
        return RecordType.valueOf(proxyResult.getRecordType().name().toUpperCase());
    }

    public void setRecordType(RecordType recordType) {
        proxyResult.setRecordType(com.aliyun.datahub.client.model.RecordType.valueOf(recordType.name().toUpperCase()));
    }

    public RecordSchema getRecordSchema() {
        return ModelConvertToOld.convertRecordSchema(proxyResult.getRecordSchema());
    }

    public void setRecordSchema(RecordSchema schema) {
        proxyResult.setRecordSchema(ModelConvertToNew.convertRecordSchema(schema));
    }

    public String getComment() {
        return proxyResult.getComment();
    }

    public void setComment(String comment) {
        proxyResult.setComment(comment);
    }

    public long getCreateTime() {
        return proxyResult.getCreateTime();
    }

    public void setCreateTime(long createTime) {
        proxyResult.setCreateTime(createTime);
    }

    public long getLastModifyTime() {
        return proxyResult.getLastModifyTime();
    }

    public void setLastModifyTime(long lastModifyTime) {
        proxyResult.setLastModifyTime(lastModifyTime);
    }
}
