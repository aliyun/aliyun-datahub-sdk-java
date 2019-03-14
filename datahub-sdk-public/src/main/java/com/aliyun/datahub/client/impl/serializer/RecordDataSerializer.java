package com.aliyun.datahub.client.impl.serializer;

import com.aliyun.datahub.client.model.*;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.math.BigDecimal;

public class RecordDataSerializer extends JsonSerializer<RecordData> {
    @Override
    public void serialize(RecordData recordData, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException, JsonProcessingException {
        if (recordData instanceof TupleRecordData) {
            TupleRecordData tupleRecordData = (TupleRecordData)recordData;
            jsonGenerator.writeStartArray();
            for (int i = 0; i < tupleRecordData.getRecordSchema().getFields().size(); ++i) {
                Field field = tupleRecordData.getRecordSchema().getField(i);
                Object object = tupleRecordData.getField(i);

                if (object != null) {
                    if (field.getType() == FieldType.DECIMAL) {
                        jsonGenerator.writeString(((BigDecimal)object).toPlainString());
                    } else {
                        jsonGenerator.writeString(String.valueOf(object));
                    }
                } else {
                    jsonGenerator.writeNull();
                }
            }
            jsonGenerator.writeEndArray();
        } else {
            jsonGenerator.writeBinary(((BlobRecordData)recordData).getData());
        }
    }
}
