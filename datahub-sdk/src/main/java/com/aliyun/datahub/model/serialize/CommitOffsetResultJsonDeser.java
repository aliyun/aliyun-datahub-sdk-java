package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.CommitOffsetRequest;
import com.aliyun.datahub.model.CommitOffsetResult;

public class CommitOffsetResultJsonDeser implements Deserializer<CommitOffsetResult, CommitOffsetRequest, Response> {
    @Override
    public CommitOffsetResult deserialize(CommitOffsetRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }
        return new CommitOffsetResult();
    }

    private CommitOffsetResultJsonDeser() {}

    private static CommitOffsetResultJsonDeser instance;

    public static CommitOffsetResultJsonDeser getInstance() {
        if (instance == null) {
            instance = new CommitOffsetResultJsonDeser();
        }
        return instance;
    }
}
