package com.aliyun.datahub.client.impl.serializer;

import com.aliyun.datahub.client.impl.request.GetCursorRequest;
import com.aliyun.datahub.client.model.CursorType;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

public class GetCursorRequestSerializer extends JsonSerializer<GetCursorRequest> {
    @Override
    public void serialize(GetCursorRequest getCursorRequest, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException, JsonProcessingException {
        jsonGenerator.writeStartObject();

        jsonGenerator.writeStringField("Action", getCursorRequest.getAction());
        jsonGenerator.writeStringField("Type", getCursorRequest.getType().name());

        if (getCursorRequest.getParameter() != -1) {
            if (CursorType.SYSTEM_TIME == getCursorRequest.getType()) {
                jsonGenerator.writeNumberField("SystemTime", getCursorRequest.getParameter());
            } else {
                jsonGenerator.writeNumberField("Sequence", getCursorRequest.getParameter());
            }
        }

        jsonGenerator.writeEndObject();
    }
}
