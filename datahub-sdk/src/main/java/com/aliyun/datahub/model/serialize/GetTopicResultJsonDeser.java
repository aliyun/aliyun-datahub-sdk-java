package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.common.data.FieldType;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.common.data.RecordType;
import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.exception.InvalidParameterException;
import com.aliyun.datahub.model.GetTopicRequest;
import com.aliyun.datahub.model.GetTopicResult;
import com.aliyun.datahub.rest.DatahubHttpHeaders;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import java.io.IOException;
import java.util.Iterator;

public class GetTopicResultJsonDeser implements Deserializer<GetTopicResult, GetTopicRequest, Response> {
    @Override
    public GetTopicResult deserialize(GetTopicRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }

        GetTopicResult rs = new GetTopicResult();
        ObjectMapper mapper = JacksonParser.getObjectMapper();

        JsonNode tree = null;
        try {
            tree = mapper.readTree(response.getBody());
        } catch (IOException e) {
            throw new DatahubServiceException(
                    "JsonParseError", "Parse body failed:" + response.getBody(), response);
        }

        rs.setShardCount(tree.get("ShardCount").asInt());
        rs.setLifeCycle(tree.get("Lifecycle").asInt());
        rs.setRecordType(RecordType.valueOf(tree.get("RecordType").asText()));
        rs.setCreateTime(tree.get("CreateTime").asLong() * 1000);
        rs.setLastModifyTime(tree.get("LastModifyTime").asLong() * 1000);
        rs.setComment(tree.get("Comment").asText());
        rs.setProjectName(request.getProjectName());
        rs.setTopicName(request.getTopicName());

        if (rs.getRecordType() == RecordType.TUPLE) {
            // parse schema
            RecordSchema schema = new RecordSchema();
            String strSchema = tree.get("RecordSchema").asText();
            JsonNode root = null;
            try {
                root = mapper.readTree(strSchema);
            } catch (IOException e) {
                throw new DatahubServiceException(
                        "JsonParseError", "Parse body failed:" + response.getBody().toString(), response);
            }

            JsonNode Fields = root.get("fields");
            if (Fields != null && !Fields.isNull()) {
                if (!Fields.isArray()) {
                    throw new DatahubServiceException(
                            "InvalidTopicSchema", "Invalid topic schema:" + strSchema, response);
                }

                Iterator<JsonNode> itField = Fields.getElements();
                while (itField.hasNext()) {
                    JsonNode fieldNode = itField.next();
                    JsonNode typeNode = fieldNode.get("type");
                    JsonNode nameNode = fieldNode.get("name");
                    JsonNode notnullNode = fieldNode.get("notnull");
                    String strType = typeNode.asText().toUpperCase();
                    FieldType type = null;
                    if (strType.equals(FieldType.STRING.toString())) {
                        type = FieldType.STRING;
                    } else if (strType.equals(FieldType.TIMESTAMP.toString())) {
                        type = FieldType.TIMESTAMP;
                    } else if (strType.equals(FieldType.DOUBLE.toString())) {
                        type = FieldType.DOUBLE;
                    } else if (strType.equals(FieldType.BOOLEAN.toString())) {
                        type = FieldType.BOOLEAN;
                    } else if (strType.equals(FieldType.BIGINT.toString())) {
                        type = FieldType.BIGINT;
                    } else if (strType.equals(FieldType.DECIMAL.toString())) {
                        type = FieldType.DECIMAL;
                    } else {
                        throw new DatahubServiceException(
                                "UnsupportedFieldType", "Unsupported field type: " + strType, response);
                    }

                    boolean notnull  = false;
                    if (notnullNode != null) {
                        notnull = notnullNode.asBoolean();
                    }
                    Field field = new Field(nameNode.asText(), type, notnull);

                    schema.addField(field);
                }
            }
            rs.setRecordSchema(schema);
        }

        return rs;
    }

    private GetTopicResultJsonDeser() {
    }

    private static GetTopicResultJsonDeser instance;

    public static GetTopicResultJsonDeser getInstance() {
        if (instance == null)
            instance = new GetTopicResultJsonDeser();
        return instance;
    }
}
