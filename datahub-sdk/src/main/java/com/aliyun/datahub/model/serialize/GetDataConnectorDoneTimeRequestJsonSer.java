package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.GetDataConnectorDoneTimeRequest;
import com.aliyun.datahub.model.GetDataConnectorRequest;

public class GetDataConnectorDoneTimeRequestJsonSer implements Serializer<DefaultRequest, GetDataConnectorDoneTimeRequest> {
    @Override
    public DefaultRequest serialize(GetDataConnectorDoneTimeRequest request) throws DatahubClientException {
        DefaultRequest req = new DefaultRequest();
        req.setResource("/projects/" + request.getProjectName()
                + "/topics/" + request.getTopicName()
                + "/connectors/" + request.getConnectorType().toString().toLowerCase()
                + "?donetime"
        );
        req.setHttpMethod(HttpMethod.GET);
        return req;
    }

    private GetDataConnectorDoneTimeRequestJsonSer() {

    }

    private static GetDataConnectorDoneTimeRequestJsonSer instance;

    public static GetDataConnectorDoneTimeRequestJsonSer getInstance() {
        if (instance == null)
            instance = new GetDataConnectorDoneTimeRequestJsonSer();
        return instance;
    }
}
