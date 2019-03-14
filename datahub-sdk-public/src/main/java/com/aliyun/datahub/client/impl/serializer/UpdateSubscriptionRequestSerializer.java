package com.aliyun.datahub.client.impl.serializer;

import com.aliyun.datahub.client.impl.request.UpdateSubscriptionRequest;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

public class UpdateSubscriptionRequestSerializer extends JsonSerializer<UpdateSubscriptionRequest> {
    @Override
    public void serialize(UpdateSubscriptionRequest updateSubscriptionRequest, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException, JsonProcessingException {
        jsonGenerator.writeStartObject();

        if (updateSubscriptionRequest.getComment() != null) {
            jsonGenerator.writeStringField("Comment", updateSubscriptionRequest.getComment());
        }

        if (updateSubscriptionRequest.getState() != null) {
            jsonGenerator.writeNumberField("State", updateSubscriptionRequest.getState().getValue());
        }

        jsonGenerator.writeEndObject();
    }
}
