package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.UpdateSubscriptionRequest;
import com.aliyun.datahub.model.UpdateSubscriptionResult;

public class UpdateSubscriptionResultJsonDeser implements Deserializer<UpdateSubscriptionResult, UpdateSubscriptionRequest, Response> {
    @Override
    public UpdateSubscriptionResult deserialize(UpdateSubscriptionRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }

        return new UpdateSubscriptionResult();
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
