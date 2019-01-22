package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.ListProjectRequest;
import com.aliyun.datahub.model.ListProjectResult;
import com.aliyun.datahub.rest.DatahubHttpHeaders;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Iterator;

public class ListProjectResultJsonDeser implements Deserializer<ListProjectResult, ListProjectRequest, Response> {

    @Override
    public ListProjectResult deserialize(ListProjectRequest request, Response response) throws DatahubServiceException {
        if (!response.isOK()) {
            throw JsonErrorParser.getInstance().parse(response);
        }

        ListProjectResult result = new ListProjectResult();
        ByteArrayInputStream is = new ByteArrayInputStream(response.getBody());
        ObjectMapper mapper = JacksonParser.getObjectMapper();
        JsonNode tree = null;
        try {
            tree = mapper.readTree(is);
        } catch (IOException e) {
            throw new DatahubServiceException(
                    "JsonParseError", "Parse body failed:" + response.getBody(), response);
        }
        JsonNode node = tree.get("ProjectNames");
        if (node != null && !node.isNull()) {
            if (node.isArray()) {
                Iterator<JsonNode> it = node.getElements();
                while (it.hasNext()) {
                    JsonNode project = it.next();
                    result.addProject(project.asText());
                }
            }
        }
        return result;
    }

    private static ListProjectResultJsonDeser instance;

    private ListProjectResultJsonDeser() {

    }

    public static ListProjectResultJsonDeser getInstance() {
        if (instance == null)
            instance = new ListProjectResultJsonDeser();
        return instance;
    }
}
