package com.aliyun.datahub.client.http.converter;

import com.google.protobuf.Message;

public interface BaseProtobufModel {
    // get message
    Message getMessage();
    // set message
    void setMessage(Message message);
}
