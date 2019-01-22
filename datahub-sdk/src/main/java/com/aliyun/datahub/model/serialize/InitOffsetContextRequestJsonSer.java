package com.aliyun.datahub.model.serialize;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.HttpMethod;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.model.InitOffsetContextRequest;;

public class InitOffsetContextRequestJsonSer implements Serializer<DefaultRequest, InitOffsetContextRequest> {

	@Override
	public DefaultRequest serialize(InitOffsetContextRequest request) throws DatahubClientException {
		DefaultRequest req = new DefaultRequest();
		req.setHttpMethod(HttpMethod.POST);
		req.setResource("/projects/" + request.getProjectName() + "/topics/" + request.getTopicName()
				+ "/subscriptions/" + request.getSubId() + "/offsets");
        ObjectMapper mapper = JacksonParser.getObjectMapper();
        try {
            ObjectNode node = mapper.createObjectNode();
            node.put("Action", "open");
            ArrayNode shardIds = mapper.createArrayNode();
            for (String i : request.getShardIds()) {
                shardIds.add(i);
            }
            node.put("ShardIds", shardIds);
			req.setBody(mapper.writeValueAsString(node));
		} catch (JsonGenerationException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return req;
	}

	private InitOffsetContextRequestJsonSer() {
	}

	private static InitOffsetContextRequestJsonSer instance;

	public static InitOffsetContextRequestJsonSer getInstance() {
		if (instance == null) {
			instance = new InitOffsetContextRequestJsonSer();
		}
		return instance;
	}
}
