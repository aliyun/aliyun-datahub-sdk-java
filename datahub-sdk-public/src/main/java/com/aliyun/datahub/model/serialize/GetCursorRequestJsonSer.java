package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.GetCursorRequest;

public class GetCursorRequestJsonSer implements Serializer<DefaultRequest, GetCursorRequest> {

    @Override
    public DefaultRequest serialize(GetCursorRequest request) throws DatahubClientException {
        DefaultRequest req = new DefaultRequest();

        req.setHttpMethod(HttpMethod.POST);
        req.setResource("/projects/" + request.getProjectName() + "/topics/"
                + request.getTopicName() + "/shards/" + request.getShardId());

        String typeName;
        if (request.getType() == null) {
            typeName = "SYSTEM_TIME";
        } else {
            typeName = request.getType().toString();
        }

        String timeFields = "";
        if (typeName.equals("SYSTEM_TIME"))
        {
            timeFields = ",\"SystemTime\":" + String.valueOf(request.getTimestamp());
        }
        else if (typeName.equals("SEQUENCE"))
        {
            timeFields = ",\"Sequence\":" + String.valueOf(request.getSequence());
        }

        String body = "{\"Action\":\"cursor\"" + timeFields + ",\"Type\":\"" + typeName + "\"}";
        req.setBody(body);
        return req;
    }

    private GetCursorRequestJsonSer() {

    }

    private static GetCursorRequestJsonSer instance;

    public static GetCursorRequestJsonSer getInstance() {
        if (instance == null)
            instance = new GetCursorRequestJsonSer();
        return instance;
    }
}
