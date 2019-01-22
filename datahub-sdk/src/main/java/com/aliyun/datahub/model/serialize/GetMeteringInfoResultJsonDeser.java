package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.GetMeteringInfoRequest;
import com.aliyun.datahub.model.GetMeteringInfoResult;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

public class GetMeteringInfoResultJsonDeser implements Deserializer<GetMeteringInfoResult, GetMeteringInfoRequest, Response> {

    @Override
    public GetMeteringInfoResult deserialize(GetMeteringInfoRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }

        GetMeteringInfoResult res = new GetMeteringInfoResult();

        ObjectMapper mapper = JacksonParser.getObjectMapper();
        JsonNode tree = null;

        try {
            tree = mapper.readTree(response.getBody());
        } catch (IOException e) {
            throw new DatahubServiceException(
                    "JsonParseError", "Parse body failed:" + response.getBody(), response);
        }

        res.setActiveTime(tree.get("ActiveTime").asLong());
        res.setStorage(tree.get("Storage").asLong());
        return res;
    }

    private GetMeteringInfoResultJsonDeser() {

    }

    private static GetMeteringInfoResultJsonDeser instance;

    public static GetMeteringInfoResultJsonDeser getInstance() {
        if (instance == null)
            instance = new GetMeteringInfoResultJsonDeser();
        return instance;
    }
}
