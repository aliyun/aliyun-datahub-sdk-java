package com.aliyun.datahub.client.model;

import com.aliyun.datahub.client.impl.serializer.RecordDataDeserializer;
import com.aliyun.datahub.client.impl.serializer.RecordDataSerializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize(using = RecordDataSerializer.class)
@JsonDeserialize(using = RecordDataDeserializer.class)
public abstract class RecordData {
}
