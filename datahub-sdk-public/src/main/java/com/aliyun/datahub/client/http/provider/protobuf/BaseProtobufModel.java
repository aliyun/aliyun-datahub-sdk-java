package com.aliyun.datahub.client.http.provider.protobuf;

import com.aliyun.datahub.client.model.BaseResult;
import com.google.protobuf.Message;

public abstract class BaseProtobufModel extends BaseResult {
    // get message
    public Message getMessage() {
        return null;
    }

    // set message
    public void setMessage(Message message) {
    }
}
