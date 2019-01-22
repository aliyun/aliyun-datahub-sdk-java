package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.GetProjectRequest;
import com.aliyun.datahub.model.GetProjectResult;
import com.aliyun.datahub.rest.DatahubHttpHeaders;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

public class GetProjectResultJsonDeser implements Deserializer<GetProjectResult, GetProjectRequest, Response> {

    @Override
    public GetProjectResult deserialize(GetProjectRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }

        ObjectMapper mapper = JacksonParser.getObjectMapper();

        GetProjectResult result = new GetProjectResult();
        JsonNode tree = null;
        try {
            tree = mapper.readTree(response.getBody());
        } catch (IOException e) {
            throw new DatahubServiceException(
                    "JsonParseError", "Parse body failed:" + response.getBody(), response);
        }
        result.setCreateTime(tree.get("CreateTime").asLong() * 1000);
        result.setLastModifyTime(tree.get("LastModifyTime").asLong() * 1000);
        result.setComment(tree.get("Comment").asText());
        result.setProjectName(request.getProjectName());

        return result;
    }

    private static GetProjectResultJsonDeser instance;

    private GetProjectResultJsonDeser() {

    }

    public static GetProjectResultJsonDeser getInstance() {
        if (instance == null)
            instance = new GetProjectResultJsonDeser();
        return instance;
    }
}
