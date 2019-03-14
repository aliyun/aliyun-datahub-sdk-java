package com.aliyun.datahub.client.impl.serializer;

import com.aliyun.datahub.client.model.Field;
import com.aliyun.datahub.client.model.FieldType;
import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.client.util.JsonUtils;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;

public class RecordSchemaDeserializer extends JsonDeserializer<RecordSchema> {
    @Override
    public RecordSchema deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
        JsonNode tree = jsonParser.getCodec().readTree(jsonParser);
        JsonNode node = null;
        if (tree == null || !tree.isTextual()) {
            throw new IOException("RecordSchema format is invalid");
        }

        node = JsonUtils.toJsonNode(tree.asText());
        if (node == null) {
            return null;
        } else if (!node.isObject()) {
            throw new IOException("RecordSchema format is invalid");
        }

        node = node.get("fields");
        if (node == null || !node.isArray()) {
            throw new IOException("Schema fields format is invalid");
        }

        RecordSchema schema = new RecordSchema();
        for (JsonNode fieldNode : node) {
            JsonNode fnode = fieldNode.get("name");
            String name = fnode.asText();
            fnode = fieldNode.get("type");
            FieldType type = FieldType.valueOf(fnode.asText().toUpperCase());
            boolean isAllowNull = true;
            fnode = fieldNode.get("notnull");
            if (fnode != null) {
                isAllowNull = !fnode.asBoolean();
            }

            schema.addField(new Field(name, type, isAllowNull));
        }
        return schema;
    }
}