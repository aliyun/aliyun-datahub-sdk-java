package com.aliyun.datahub.client.impl.serializer;

import com.aliyun.datahub.client.impl.request.ResetSubscriptionOffsetRequest;
import com.aliyun.datahub.client.model.SubscriptionOffset;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.util.Map;

public class ResetSubscriptionOffsetRequestSerializer extends JsonSerializer<ResetSubscriptionOffsetRequest> {
    @Override
    public void serialize(ResetSubscriptionOffsetRequest resetSubscriptionOffsetRequest, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException, JsonProcessingException {
        jsonGenerator.writeStartObject();

        jsonGenerator.writeStringField("Action", resetSubscriptionOffsetRequest.getAction());

        if (resetSubscriptionOffsetRequest.getOffsets() != null && resetSubscriptionOffsetRequest.getOffsets().size() != 0) {
            jsonGenerator.writeObjectFieldStart("Offsets");
            for (Map.Entry<String, SubscriptionOffset> entry : resetSubscriptionOffsetRequest.getOffsets().entrySet()) {
                jsonGenerator.writeObjectFieldStart(entry.getKey());
                jsonGenerator.writeNumberField("Timestamp", entry.getValue().getTimestamp());
                jsonGenerator.writeNumberField("Sequence", entry.getValue().getSequence());
                jsonGenerator.writeEndObject();
            }
            jsonGenerator.writeEndObject();
        }

        jsonGenerator.writeEndObject();
    }
}
