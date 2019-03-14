package com.aliyun.datahub.client.model;

import com.aliyun.datahub.client.impl.serializer.SinkConfigSerializer;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize(using = SinkConfigSerializer.class)
public abstract class SinkConfig {
    public enum AuthMode {
        /**
         * AK mode: you should input accessId and accessKey
         */
        AK,

        /**
         * Service will get a temporary key to visit other services.
         */
        STS
    }
}
