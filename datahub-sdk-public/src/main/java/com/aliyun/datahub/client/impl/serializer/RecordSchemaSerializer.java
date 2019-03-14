package com.aliyun.datahub.client.impl.serializer;

import com.aliyun.datahub.client.model.Field;
import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.client.util.JsonUtils;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;

public class RecordSchemaSerializer extends JsonSerializer<RecordSchema> {
    @Override
    public void serialize(RecordSchema recordSchema, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException, JsonProcessingException {

        // convert object to string first. Mustn't call JsonUtils directly
        List<Field> fields = recordSchema.getFields();
        StringWriter writer = new StringWriter();
        JsonGenerator tempGenerator = JsonUtils.getMAPPER().getFactory().createGenerator(writer);
        tempGenerator.writeStartObject();

        tempGenerator.writeArrayFieldStart("fields");
        for (Field field : fields) {
            tempGenerator.writeStartObject();

            tempGenerator.writeStringField("name", field.getName());
            tempGenerator.writeStringField("type", field.getType().name());
            if (!field.isAllowNull()) {
                tempGenerator.writeBooleanField("notnull", !field.isAllowNull());
            }

            tempGenerator.writeEndObject();
        }
        tempGenerator.writeEndArray();
        tempGenerator.writeEndObject();
        tempGenerator.close();

        // write schema as string
        String shemaStr = writer.toString();
        jsonGenerator.writeString(shemaStr);
    }
}
