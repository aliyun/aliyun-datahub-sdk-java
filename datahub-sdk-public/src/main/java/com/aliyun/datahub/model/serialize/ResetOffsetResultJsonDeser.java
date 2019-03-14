package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.CommitOffsetResult;
import com.aliyun.datahub.model.ResetOffsetRequest;
import com.aliyun.datahub.rest.DatahubHttpHeaders;

public class ResetOffsetResultJsonDeser implements Deserializer<CommitOffsetResult,ResetOffsetRequest, Response> {
    @Override
    public CommitOffsetResult deserialize(ResetOffsetRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }

        CommitOffsetResult rs = new CommitOffsetResult();
        rs.setRequestId(response.getHeader(DatahubHttpHeaders.HEADER_DATAHUB_REQUEST_ID));
        return rs;
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
