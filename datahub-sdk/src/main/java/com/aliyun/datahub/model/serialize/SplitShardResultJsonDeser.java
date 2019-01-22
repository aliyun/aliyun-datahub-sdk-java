package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.DatahubConstants;
import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.SplitShardRequest;
import com.aliyun.datahub.model.SplitShardResult;
import com.aliyun.datahub.model.ShardDesc;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Iterator;

public class SplitShardResultJsonDeser implements Deserializer<SplitShardResult, SplitShardRequest, Response> {

    @Override
    public SplitShardResult deserialize(SplitShardRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }

        SplitShardResult rs = new SplitShardResult();
        ObjectMapper mapper = JacksonParser.getObjectMapper();

        JsonNode tree = null;
        try {
            tree = mapper.readTree(response.getBody());
            JsonNode node = tree.get("NewShards");
            if (node != null && !node.isNull()) {
                if (node.isArray()) {
                    Iterator<JsonNode> it = node.getElements();
                    while (it.hasNext()) {
                        JsonNode shard = it.next();
                        ShardDesc entry = new ShardDesc();
                        entry.setShardId(shard.get(DatahubConstants.ShardId).asText());
                        entry.setBeginHashKey(shard.get(DatahubConstants.BeginHashKey).asText());
                        entry.setEndHashKey(shard.get(DatahubConstants.EndHashKey).asText());
                        rs.addShard(entry);
                    }
                }
            } else {
                throw new DatahubServiceException("JsonParseError",
                        "NewShards missing in response body:" + response.getBody(), response);
            }
        } catch(IOException e) {
            DatahubServiceException ex = new DatahubServiceException("JsonParseError",
                    "Parse body failed:" + response.getBody(), response);
            throw ex;
        }

        return rs;
    }

    private static SplitShardResultJsonDeser instance;

    private SplitShardResultJsonDeser() {

    }

    public static SplitShardResultJsonDeser getInstance() {
        if (instance == null)
            instance = new SplitShardResultJsonDeser();
        return instance;
    }
}
