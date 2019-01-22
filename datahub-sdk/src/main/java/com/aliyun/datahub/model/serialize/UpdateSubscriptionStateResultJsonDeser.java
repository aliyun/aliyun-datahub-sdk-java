package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.UpdateSubscriptionResult;
import com.aliyun.datahub.model.UpdateSubscriptionStateRequest;

public class UpdateSubscriptionStateResultJsonDeser implements Deserializer<UpdateSubscriptionResult, UpdateSubscriptionStateRequest, Response> {
    @Override
    public UpdateSubscriptionResult deserialize(UpdateSubscriptionStateRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }

        return new UpdateSubscriptionResult();
    }

    private UpdateSubscriptionStateResultJsonDeser() {}

    private static UpdateSubscriptionStateResultJsonDeser instance;

    public static UpdateSubscriptionStateResultJsonDeser getInstance() {
        if (instance == null) {
            instance = new UpdateSubscriptionStateResultJsonDeser();
        }
        return instance;
    }
}
