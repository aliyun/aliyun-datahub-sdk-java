package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.CommitOffsetRequest;
import com.aliyun.datahub.model.CommitOffsetResult;
import com.aliyun.datahub.rest.DatahubHttpHeaders;

public class CommitOffsetResultJsonDeser implements Deserializer<CommitOffsetResult, CommitOffsetRequest, Response> {
    @Override
    public CommitOffsetResult deserialize(CommitOffsetRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }

        CommitOffsetResult rs = new CommitOffsetResult();
        rs.setRequestId(response.getHeader(DatahubHttpHeaders.HEADER_DATAHUB_REQUEST_ID));
        return rs;
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
