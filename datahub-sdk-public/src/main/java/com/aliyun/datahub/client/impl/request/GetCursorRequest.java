package com.aliyun.datahub.client.impl.request;

import com.aliyun.datahub.client.impl.serializer.GetCursorRequestSerializer;
import com.aliyun.datahub.client.model.CursorType;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize(using = GetCursorRequestSerializer.class)
public class GetCursorRequest extends BaseRequest {
    private CursorType type;
    private long parameter = -1;

    public GetCursorRequest() {
        setAction("cursor");
    }

    public CursorType getType() {
        return type;
    }

    public GetCursorRequest setType(CursorType type) {
        this.type = type;
        return this;
    }

    public long getParameter() {
        return parameter;
    }

    public GetCursorRequest setParameter(long parameter) {
        this.parameter = parameter;
        return this;
    }
}
