package com.aliyun.datahub.model;


import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubClientException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

/**
 * Created by wz on 17/6/5.
 */
public class FcDesc extends ConnectorConfig {

    private String endpoint;
    private String service;
    private String function;
    private String invocationRole;
    private int batchSize;
    private String startPosition;
    private Long startTimestamp;

    private String authMode = "sts";
    private String accessId;
    private String accessKey;

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

    public String getFunction() {
        return function;
    }

    public void setFunction(String function) {
        this.function = function;
    }

    public String getInvocationRole() {
        return invocationRole;
    }

    public void setInvocationRole(String invocationRole) {
        this.invocationRole = invocationRole;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public String getStartPosition() {
        return startPosition;
    }

    public void setStartPosition(String startPosition) {
        this.startPosition = startPosition;
    }

    public Long getStartTimestamp() {
        return startTimestamp;
    }

    public void setStartTimestamp(Long startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    public String getAuthMode() {
        return authMode;
    }

    public void setAuthMode(String authMode) {
        this.authMode = authMode;
    }

    public String getAccessId() {
        return accessId;
    }

    public void setAccessId(String accessId) {
        this.accessId = accessId;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    @Override
    public ObjectNode toJsonNode() {
        ObjectMapper mapper = JacksonParser.getObjectMapper();
        ObjectNode fcDesc = mapper.createObjectNode();
        fcDesc.put("Endpoint", endpoint);
        fcDesc.put("Service", service);
        fcDesc.put("Function", function);
        fcDesc.put("BatchSize", batchSize);
        fcDesc.put("StartPosition", startPosition);
        if (startPosition != null && startPosition.equals("SYSTEM_TIME")) {
            fcDesc.put("StartTimestamp", startTimestamp);
        }

        fcDesc.put("AuthMode", authMode);
        if (authMode.equals("ak")) {
            fcDesc.put("AccessId", accessId);
            fcDesc.put("AccessKey", accessKey);
        } else if (authMode.equals("sts")) {
            if (invocationRole != null && !invocationRole.isEmpty()) {
                fcDesc.put("InvocationRole", invocationRole);
            }
        }
        return fcDesc;
    }

    @Override
    public void ParseFromJsonNode(JsonNode node) {
        if (node != null && !node.isNull()) {
            JsonNode sub = node.get("Endpoint");
            if (sub != null && !sub.isNull()) {
                endpoint = sub.asText();
            }
            sub = node.get("Service");
            if (sub != null && !sub.isNull()) {
                service = sub.asText();
            }
            sub = node.get("Function");
            if (sub != null && !sub.isNull()) {
                function = sub.asText();
            }
            sub = node.get("InvocationRole");
            if (sub != null && !sub.isNull()) {
                invocationRole = sub.asText();
            }
            sub = node.get("BatchSize");
            if (sub != null && !sub.isNull()) {
                batchSize = sub.asInt();
            }
            sub = node.get("StartPosition");
            if (sub != null && !sub.isNull()) {
                startPosition = sub.asText();
            }
            sub = node.get("StartTimestamp");
            if (sub != null && !sub.isNull()) {
                startTimestamp = sub.asLong();
            }
        } else {
            throw new DatahubClientException("Invalid response, missing config.");
        }
    }
}
