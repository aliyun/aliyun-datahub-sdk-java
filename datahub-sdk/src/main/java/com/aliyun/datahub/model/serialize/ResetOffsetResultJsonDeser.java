package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.CommitOffsetResult;
import com.aliyun.datahub.model.ResetOffsetRequest;

public class ResetOffsetResultJsonDeser implements Deserializer<CommitOffsetResult,ResetOffsetRequest, Response> {
    @Override
    public CommitOffsetResult deserialize(ResetOffsetRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }
        return new CommitOffsetResult();
    }

    private ResetOffsetResultJsonDeser() {}

    private static ResetOffsetResultJsonDeser instance;

    public static ResetOffsetResultJsonDeser getInstance() {
        if (instance == null) {
            instance = new ResetOffsetResultJsonDeser();
        }
        return instance;
    }
}
