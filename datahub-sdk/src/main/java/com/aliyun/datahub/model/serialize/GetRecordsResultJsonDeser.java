package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.common.data.FieldType;
import com.aliyun.datahub.common.data.RecordType;
import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.exception.MalformedRecordException;
import com.aliyun.datahub.model.GetRecordsRequest;
import com.aliyun.datahub.model.GetRecordsResult;
import com.aliyun.datahub.model.RecordEntry;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

public class GetRecordsResultJsonDeser implements Deserializer<GetRecordsResult, GetRecordsRequest, Response> {
    @Override
    public GetRecordsResult deserialize(GetRecordsRequest request, Response response) throws DatahubClientException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }

        GetRecordsResult rs = new GetRecordsResult();
        ObjectMapper mapper = JacksonParser.getObjectMapper();

        JsonNode tree = null;
        try {
            tree = mapper.readTree(response.getBody());
        } catch (IOException e) {
            throw new DatahubServiceException(
                    "JsonParseError", "Parse body failed:" + response.getBody(), response);
        }

        rs.setNextCursor(tree.get("NextCursor").asText());
        long sequence = tree.get("StartSeq").asLong();
        rs.setStartSeq(sequence);

        JsonNode recordsNode = tree.get("Records");

        List<RecordEntry> records = new ArrayList<RecordEntry>();
        Iterator<JsonNode> itRecord = recordsNode.getElements();
        while (itRecord.hasNext()) {
            RecordEntry entry = new RecordEntry(request.getSchema());
            JsonNode record = itRecord.next();
            JsonNode time = record.get("SystemTime");
            JsonNode data = record.get("Data");
            JsonNode attrs = record.get("Attributes");

            // sequence
            entry.setSequence(sequence++);

            // time
            entry.setSystemTime(time.asLong());

            // shard id
            entry.setShardId(request.getShardId());

            // record data
            Iterator<JsonNode> itField = data.getElements();
            int i = 0;
            for (Field field : request.getSchema().getFields()) {
                if (!itField.hasNext()) {
                    // null value when data field size less than schema (maybe old data after append field)
                    // do not throw MalformedRecordException after 2.4.1
                    //throw new MalformedRecordException("field count error, please check schema.", response);

                    continue;
                }
                JsonNode node = itField.next();
                try {
                    if (!node.isNull()) {
                        if (field.getType() == FieldType.BIGINT) {
                            entry.setBigint(i, Long.parseLong(node.asText()));
                        } else if (field.getType() == FieldType.BOOLEAN) {
                            String v = node.asText();
                            if (v.equalsIgnoreCase("true") || v.equalsIgnoreCase("false")) {
                                entry.setBoolean(i, Boolean.parseBoolean(v));
                            } else {
                                throw new MalformedRecordException("invalid boolean value: " + v, response);
                            }
                        } else if (field.getType() == FieldType.DOUBLE) {
                            entry.setDouble(i, Double.parseDouble(node.asText()));
                        } else if (field.getType() == FieldType.STRING) {
                            entry.setString(i, node.asText());
                        } else if (field.getType() == FieldType.TIMESTAMP) {
                            entry.setTimeStamp(i, Long.parseLong(node.asText()));
                        } else if (field.getType() == FieldType.DECIMAL) {
                            entry.setDecimal(i, new BigDecimal(node.asText()));
                        } else {
                            throw new MalformedRecordException("unsupported data type:" + field.getType().name(), response);
                        }
                    }
                } catch (NumberFormatException e) {
                    throw new MalformedRecordException(
                            "invalid type cast:" + field.getType().name() + "(" + node.asText() + ")", response);
                }
                ++i;
            }
            if (itField.hasNext()) {
                // ignore this exception after 2.4.1
                //throw new MalformedRecordException("field count error, please check schema.", response);
            }
            // attributes
            if (attrs != null && !attrs.isNull()) {
                Iterator<Map.Entry<String, JsonNode>> itAttr = attrs.getFields();
                Map<String, String> attrMap = new HashMap<String, String>();
                while (itAttr.hasNext()) {
                    Map.Entry<String, JsonNode> attr = itAttr.next();
                    entry.putAttribute(attr.getKey(), attr.getValue().asText());
                }
            }
            records.add(entry);
        }

        rs.setRecords(records);
        return rs;
    }

    private GetRecordsResultJsonDeser() {

    }

    private static GetRecordsResultJsonDeser instance;

    public static GetRecordsResultJsonDeser getInstance() {
        if (instance == null)
            instance = new GetRecordsResultJsonDeser();
        return instance;
    }
}
