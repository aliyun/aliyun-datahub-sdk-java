package com.aliyun.datahub.model;

import com.aliyun.datahub.DatahubConstants;
import com.aliyun.datahub.common.util.JacksonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;

public class SplitShardResult {
    private List<ShardDesc> shards;

    public SplitShardResult() {
        shards = new ArrayList<ShardDesc>();
    }

    public List<ShardDesc> getShards() {
        return this.shards;
    }

    public void addShard(ShardDesc desc) {
        shards.add(desc);
    }

    @Override
    public String toString() {
        ObjectMapper mapper = JacksonParser.getObjectMapper();
        ArrayNode shards = mapper.createArrayNode();
        for (ShardDesc desc : this.shards) {
            ObjectNode shard = mapper.createObjectNode();
            shard.put(DatahubConstants.ShardId, desc.getShardId());
            shard.put(DatahubConstants.BeginHashKey, desc.getBeginHashKey());
            shard.put(DatahubConstants.EndHashKey, desc.getEndHashKey());
            shards.add(shard);
        }
        return shards.toString();
    }
}
