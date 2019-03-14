package com.aliyun.datahub.model;

import com.aliyun.datahub.client.util.JsonUtils;

public class MergeShardResult extends Result {
    private com.aliyun.datahub.client.model.MergeShardResult proxyResult;

    public MergeShardResult(com.aliyun.datahub.client.model.MergeShardResult proxyResult) {
        this.proxyResult = proxyResult;
        setRequestId(proxyResult.getRequestId());
    }

    public MergeShardResult() {
        proxyResult = new com.aliyun.datahub.client.model.MergeShardResult();
    }

    public ShardDesc getChildShard() {
        ShardDesc desc = new ShardDesc();
        desc.setShardId(proxyResult.getShardId());
        desc.setBeginHashKey(proxyResult.getBeginHashKey());
        desc.setEndHashKey(proxyResult.getEndHashKey());
        return desc;
    }

    public void setChildShard(ShardDesc desc) {
        proxyResult.setShardId(desc.getShardId());
        proxyResult.setBeginHashKey(desc.getBeginHashKey());
        proxyResult.setEndHashKey(desc.getEndHashKey());
    }

    @Override
    public String toString() {
        return JsonUtils.toJson(proxyResult);
    }
}
