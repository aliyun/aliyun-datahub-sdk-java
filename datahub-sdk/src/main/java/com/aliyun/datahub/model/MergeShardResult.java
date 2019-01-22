package com.aliyun.datahub.model;

import com.aliyun.datahub.DatahubConstants;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubClientException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;

public class MergeShardResult {
    private ShardDesc childShard;

    public MergeShardResult() {}

    public ShardDesc getChildShard() {
        return this.childShard;
    }

    public void setChildShard(ShardDesc desc) {
        childShard = desc;
    }

    @Override
    public String toString() {
        ObjectMapper mapper = JacksonParser.getObjectMapper();
        ObjectNode shard = mapper.createObjectNode();
        shard.put(DatahubConstants.ShardId, childShard.getShardId());
        shard.put(DatahubConstants.BeginHashKey, childShard.getBeginHashKey());
        shard.put(DatahubConstants.EndHashKey, childShard.getEndHashKey());
        try {
            return mapper.writeValueAsString(shard);
        } catch (IOException e) {
            throw new DatahubClientException("serialize error", e);
        }
    }
}
