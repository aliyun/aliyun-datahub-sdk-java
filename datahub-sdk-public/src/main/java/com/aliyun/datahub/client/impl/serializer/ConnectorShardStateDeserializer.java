package com.aliyun.datahub.client.impl.serializer;

import com.aliyun.datahub.client.model.ConnectorShardState;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;

public class ConnectorShardStateDeserializer extends JsonDeserializer<ConnectorShardState> {
    @Override
    public ConnectorShardState deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
        JsonNode tree = jsonParser.getCodec().readTree(jsonParser);
        if (tree != null && !tree.isNull()) {
            return ConnectorShardState.fromString(tree.asText().toUpperCase());
        }
        return null;
    }
}
