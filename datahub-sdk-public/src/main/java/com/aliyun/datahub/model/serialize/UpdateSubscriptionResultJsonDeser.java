package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.UpdateSubscriptionRequest;
import com.aliyun.datahub.model.UpdateSubscriptionResult;
import com.aliyun.datahub.rest.DatahubHttpHeaders;

public class UpdateSubscriptionResultJsonDeser implements Deserializer<UpdateSubscriptionResult, UpdateSubscriptionRequest, Response> {
    @Override
    public UpdateSubscriptionResult deserialize(UpdateSubscriptionRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }

        UpdateSubscriptionResult rs = new UpdateSubscriptionResult();
        rs.setRequestId(response.getHeader(DatahubHttpHeaders.HEADER_DATAHUB_REQUEST_ID));
        return rs;
    }

    private UpdateSubscriptionResultJsonDeser() {}

    private static UpdateSubscriptionResultJsonDeser instance;

    public static UpdateSubscriptionResultJsonDeser getInstance() {
        if (instance == null) {
            instance = new UpdateSubscriptionResultJsonDeser();
        }
        return instance;
    }
}
