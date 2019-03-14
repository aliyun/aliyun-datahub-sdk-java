package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.AppendFieldRequest;
import com.aliyun.datahub.model.AppendFieldResult;
import com.aliyun.datahub.rest.DatahubHttpHeaders;

public class AppendFieldResultJsonDeser implements Deserializer<AppendFieldResult,AppendFieldRequest,Response> {
    @Override
    public AppendFieldResult deserialize(AppendFieldRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }

        AppendFieldResult rs = new AppendFieldResult();
        rs.setRequestId(response.getHeader(DatahubHttpHeaders.HEADER_DATAHUB_REQUEST_ID));
        return rs;
    }
    private AppendFieldResultJsonDeser() {

    }

    private static AppendFieldResultJsonDeser instance;

    public static AppendFieldResultJsonDeser getInstance() {
        if (instance == null)
            instance = new AppendFieldResultJsonDeser();
        return instance;
    }
}
