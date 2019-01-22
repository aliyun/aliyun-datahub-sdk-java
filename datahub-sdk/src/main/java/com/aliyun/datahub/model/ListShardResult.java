package com.aliyun.datahub.model;

import com.aliyun.datahub.DatahubConstants;
import com.aliyun.datahub.common.util.JacksonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;

public class ListShardResult {
    private List<ShardEntry> shards;

    public ListShardResult() {
        shards = new ArrayList<ShardEntry>();
    }

    public List<ShardEntry> getShards() {
        return this.shards;
    }

    public void addShard(ShardEntry entry) {
        shards.add(entry);
    }

    @Override
    public String toString() {
        ObjectMapper mapper = JacksonParser.getObjectMapper();
        ArrayNode shards = mapper.createArrayNode();
        for (ShardEntry entry : this.shards) {
            ObjectNode shard = mapper.createObjectNode();
            shard.put(DatahubConstants.ShardId, entry.getShardId());
            shard.put(DatahubConstants.State, entry.getState().toString());
            shard.put(DatahubConstants.ClosedTime, entry.getClosedTime());
            shard.put(DatahubConstants.BeginHashKey, entry.getBeginHashKey());
            shard.put(DatahubConstants.EndHashKey, entry.getEndHashKey());
            shard.put(DatahubConstants.LeftShardId, entry.getLeftShardId());
            shard.put(DatahubConstants.RightShardId, entry.getRightShardId());
            ArrayNode parents = mapper.createArrayNode();
            for (String parent : entry.getParentShardIds()) {
                parents.add(parent);
            }
            shard.put(DatahubConstants.ParentShardIds, parents);
            shards.add(shard);
        }
        return shards.toString();
    }
}
