package com.aliyun.datahub.model;

import com.aliyun.datahub.client.util.JsonUtils;

import java.util.ArrayList;
import java.util.List;

public class SplitShardResult extends Result {
    private com.aliyun.datahub.client.model.SplitShardResult proxyResult;

    public SplitShardResult(com.aliyun.datahub.client.model.SplitShardResult proxyResult) {
        this.proxyResult = proxyResult;
        setRequestId(proxyResult.getRequestId());
    }

    public SplitShardResult() {
        proxyResult = new com.aliyun.datahub.client.model.SplitShardResult();
    }

    public List<ShardDesc> getShards() {
        List<ShardDesc> shardDescList = new ArrayList<>();
        for (com.aliyun.datahub.client.model.ShardEntry shardEntry : proxyResult.getNewShards()) {
            ShardDesc shardDesc = new ShardDesc();
            shardDesc.setShardId(shardEntry.getShardId());
            shardDesc.setBeginHashKey(shardEntry.getBeginHashKey());
            shardDesc.setEndHashKey(shardEntry.getEndHashKey());
            shardDescList.add(shardDesc);
        }
        return shardDescList;
    }

    public void addShard(ShardDesc desc) {
        com.aliyun.datahub.client.model.ShardEntry entry = new com.aliyun.datahub.client.model.ShardEntry();
        entry.setShardId(desc.getShardId());
        entry.setBeginHashKey(desc.getBeginHashKey());
        entry.setEndHashKey(desc.getEndHashKey());

        List<com.aliyun.datahub.client.model.ShardEntry> shardEntryList = proxyResult.getNewShards();
        if (shardEntryList == null) {
            shardEntryList = new ArrayList<>();
            proxyResult.setNewShards(shardEntryList);
        }
        shardEntryList.add(entry);
    }

    @Override
    public String toString() {
        return JsonUtils.toJson(proxyResult);
    }
}
