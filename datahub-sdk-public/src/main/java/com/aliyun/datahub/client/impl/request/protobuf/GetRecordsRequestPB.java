package com.aliyun.datahub.client.impl.request.protobuf;

import com.aliyun.datahub.client.http.provider.protobuf.BaseProtobufModel;
import com.aliyun.datahub.client.model.protobuf.DatahubProtos;
import com.google.protobuf.Message;

public class GetRecordsRequestPB extends BaseProtobufModel {
    private DatahubProtos.GetRecordsRequest.Builder builder = DatahubProtos.GetRecordsRequest.newBuilder();

    public GetRecordsRequestPB setCursor(String cursor) {
        builder.setCursor(cursor);
        return this;
    }

    public GetRecordsRequestPB setLimit(int limit) {
        builder.setLimit(limit);
        return this;
    }

    @Override
    public Message getMessage() {
        return builder.build();
    }
}
