package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.DeleteSubscriptionRequest;
import com.aliyun.datahub.model.DeleteSubscriptionResult;
import com.aliyun.datahub.rest.DatahubHttpHeaders;

public class DeleteSubscriptionResultJsonDeser implements Deserializer<DeleteSubscriptionResult, DeleteSubscriptionRequest, Response> {
    @Override
    public DeleteSubscriptionResult deserialize(DeleteSubscriptionRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }

        DeleteSubscriptionResult rs = new DeleteSubscriptionResult();
        rs.setRequestId(response.getHeader(DatahubHttpHeaders.HEADER_DATAHUB_REQUEST_ID));
        return rs;
    }

    private DeleteSubscriptionResultJsonDeser() {}

    private static DeleteSubscriptionResultJsonDeser instance;

    public static DeleteSubscriptionResultJsonDeser getInstance() {
        if (instance == null) {
            instance = new DeleteSubscriptionResultJsonDeser();
        }
        return instance;
    }
}
