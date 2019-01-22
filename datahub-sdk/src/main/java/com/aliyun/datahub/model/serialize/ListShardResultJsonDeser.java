package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.DatahubConstants;
import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.ListShardRequest;
import com.aliyun.datahub.model.ListShardResult;
import com.aliyun.datahub.model.ShardState;
import com.aliyun.datahub.model.ShardEntry;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Iterator;

public class ListShardResultJsonDeser implements Deserializer<ListShardResult, ListShardRequest, Response> {

    @Override
    public ListShardResult deserialize(ListShardRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }

        ListShardResult rs = new ListShardResult();
        ObjectMapper mapper = JacksonParser.getObjectMapper();

        JsonNode tree = null;
        try {
            tree = mapper.readTree(response.getBody());
        } catch(IOException e) {
            throw new DatahubServiceException(
                    "JsonParseError", "Parse body failed:" + response.getBody(), response);
        }

        JsonNode node = tree.get("Shards");
        if (node != null && !node.isNull()) {
            if (node.isArray()) {
                Iterator<JsonNode> it = node.getElements();
                while (it.hasNext()) {
                    JsonNode shard = it.next();
                    ShardEntry entry = new ShardEntry();
                    entry.setShardId(shard.get(DatahubConstants.ShardId).asText());
                    entry.setState(ShardState.valueOf(shard.get(DatahubConstants.State).asText()));
                    entry.setBeginHashKey(shard.get(DatahubConstants.BeginHashKey).asText());
                    entry.setEndHashKey(shard.get(DatahubConstants.EndHashKey).asText());

                    JsonNode parents = shard.get(DatahubConstants.ParentShardIds);
                    if (parents != null && !parents.isNull()) {
                        if (node.isArray()) {
                            Iterator<JsonNode> pit = parents.getElements();
                            while (pit.hasNext()) {
                                JsonNode parent = pit.next();
                                entry.addParentShardIds(parent.asText());
                            }
                        }
                    }

                    JsonNode closedTime = shard.get(DatahubConstants.ClosedTime);
                    if (closedTime != null) {
                        entry.setClosedTime(closedTime.asLong());
                    }
                    if (entry.getClosedTime() == 0) {
                        if (!shard.get(DatahubConstants.LeftShardId).asText().equals(DatahubConstants.MAX_SHARD_ID)) {
                            entry.setLeftShardId(shard.get(DatahubConstants.LeftShardId).asText());
                        }
                        if (!shard.get(DatahubConstants.RightShardId).asText().equals(DatahubConstants.MAX_SHARD_ID)) {
                            entry.setRightShardId(shard.get(DatahubConstants.RightShardId).asText());
                        }
                    }
                    rs.addShard(entry);
                }
            }
        }
        return rs;
    }

    private static ListShardResultJsonDeser instance;

    private ListShardResultJsonDeser() {

    }

    public static ListShardResultJsonDeser getInstance() {
        if (instance == null)
            instance = new ListShardResultJsonDeser();
        return instance;
    }
}
