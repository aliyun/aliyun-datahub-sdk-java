package com.aliyun.datahub.model;

import com.aliyun.datahub.client.util.JsonUtils;

import java.util.ArrayList;
import java.util.List;

public class ListShardResult extends Result {
    private com.aliyun.datahub.client.model.ListShardResult proxyResult;

    public ListShardResult(com.aliyun.datahub.client.model.ListShardResult proxyResult) {
        this.proxyResult = proxyResult;
        setRequestId(proxyResult.getRequestId());
    }

    public ListShardResult() {
        proxyResult = new com.aliyun.datahub.client.model.ListShardResult();
    }

    public List<ShardEntry> getShards() {
        List<ShardEntry> shardEntries = new ArrayList<>();
        for (com.aliyun.datahub.client.model.ShardEntry newEntry : proxyResult.getShards()) {
            ShardEntry oldEntry = new ShardEntry();
            oldEntry.setShardId(newEntry.getShardId());
            oldEntry.setParentShardIds(newEntry.getParentShardIds());
            oldEntry.setLeftShardId(newEntry.getLeftShardId());
            oldEntry.setRightShardId(newEntry.getRightShardId());
            oldEntry.setBeginHashKey(newEntry.getBeginHashKey());
            oldEntry.setEndHashKey(newEntry.getEndHashKey());
            oldEntry.setClosedTime(newEntry.getClosedTime());
            oldEntry.setState(ShardState.valueOf(newEntry.getState().name().toUpperCase()));
            shardEntries.add(oldEntry);
        }

        return shardEntries;
    }

    public void addShard(ShardEntry entry) {
        com.aliyun.datahub.client.model.ShardEntry newEntry = new com.aliyun.datahub.client.model.ShardEntry();
        newEntry.setShardId(entry.getShardId());
        newEntry.setParentShardIds(entry.getParentShardIds());
        newEntry.setLeftShardId(entry.getLeftShardId());
        newEntry.setRightShardId(entry.getRightShardId());
        newEntry.setBeginHashKey(entry.getBeginHashKey());
        newEntry.setEndHashKey(entry.getEndHashKey());
        newEntry.setClosedTime(entry.getClosedTime());
        newEntry.setState(com.aliyun.datahub.client.model.ShardState.valueOf(entry.getState().name().toUpperCase()));

        List<com.aliyun.datahub.client.model.ShardEntry> shardEntryList = proxyResult.getShards();
        if (shardEntryList == null) {
            shardEntryList = new ArrayList<>();
            proxyResult.setShards(shardEntryList);
        }

        shardEntryList.add(newEntry);
    }

    @Override
    public String toString() {
        return JsonUtils.toJson(proxyResult);
    }
}
