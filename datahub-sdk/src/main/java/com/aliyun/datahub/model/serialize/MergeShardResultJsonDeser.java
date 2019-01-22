package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.DatahubConstants;
import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.MergeShardRequest;
import com.aliyun.datahub.model.MergeShardResult;
import com.aliyun.datahub.model.ShardDesc;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

public class MergeShardResultJsonDeser implements Deserializer<MergeShardResult, MergeShardRequest, Response> {

    @Override
    public MergeShardResult deserialize(MergeShardRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }

        MergeShardResult rs = new MergeShardResult();
        ObjectMapper mapper = JacksonParser.getObjectMapper();

        JsonNode tree = null;
        try {
            tree = mapper.readTree(response.getBody());
            ShardDesc desc = new ShardDesc();
            desc.setShardId(tree.get(DatahubConstants.ShardId).asText());
            desc.setBeginHashKey(tree.get(DatahubConstants.BeginHashKey).asText());
            desc.setEndHashKey(tree.get(DatahubConstants.EndHashKey).asText());
            rs.setChildShard(desc);
        } catch(IOException e) {
            DatahubServiceException ex = new DatahubServiceException("JsonParseError",
                    "Parse body failed:" + response.getBody(), response);
            throw ex;
        }
        return rs;
    }

    private static MergeShardResultJsonDeser instance;

    private MergeShardResultJsonDeser() {

    }

    public static MergeShardResultJsonDeser getInstance() {
        if (instance == null)
            instance = new MergeShardResultJsonDeser();
        return instance;
    }
}
