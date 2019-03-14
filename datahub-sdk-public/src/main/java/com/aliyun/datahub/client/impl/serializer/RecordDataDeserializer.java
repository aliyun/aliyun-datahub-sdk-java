package com.aliyun.datahub.client.impl.serializer;

import com.aliyun.datahub.client.model.BlobRecordData;
import com.aliyun.datahub.client.model.RecordData;
import com.aliyun.datahub.client.model.TupleRecordData;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.Iterator;

public class RecordDataDeserializer extends JsonDeserializer<RecordData> {
    @Override
    public RecordData deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
        JsonNode tree = jsonParser.getCodec().readTree(jsonParser);
        if (tree.isArray()) {
            TupleRecordData data = new TupleRecordData(tree.size());
            Iterator<JsonNode> it = tree.elements();
            while (it.hasNext()) {
                JsonNode node = it.next();
                if (node.isNull()) {
                    data.internalAddValue(null);
                } else {
                    data.internalAddValue(node.asText());
                }
            }
            return data;


        } else {
            return new BlobRecordData(tree.binaryValue());
        }
    }
}
