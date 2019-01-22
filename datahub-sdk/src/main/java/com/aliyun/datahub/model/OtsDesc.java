package com.aliyun.datahub.model;

import org.apache.http.auth.AUTH;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubClientException;

public class OtsDesc extends ConnectorConfig {
    private String endpoint;
    private String instance;
    private String table;
    private AuthMode authMode;
    private String accessId;
    private String accessKey;
    private OffsetStoreType offsetStoreType;

    public OtsDesc() {}

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }
    
    public String getInstance() {
    	return instance;
    }
    
    public void setInstance(String instance) {
    	this.instance = instance;
    }
    
    public String getTable() {
    	return table;
    }
    
    public void setTable(String table) {
    	this.table = table;
    }

    public AuthMode getAuthMode() {
        return authMode;
    }

    public void setAuthMode(AuthMode authMode) {
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
    
    public OffsetStoreType getOffsetStoreType() {
    	return offsetStoreType;
    }
    
    public void setOffsetStoreType(OffsetStoreType offsetStoreType) {
    	this.offsetStoreType = offsetStoreType;
    }

    @Override
    public ObjectNode toJsonNode() {
        ObjectMapper mapper = JacksonParser.getObjectMapper();
        ObjectNode otsDesc = mapper.createObjectNode();
        otsDesc.put("Endpoint", endpoint);
        otsDesc.put("InstanceName", instance);
        otsDesc.put("TableName", table);
        if (AuthMode.AK == authMode) {
            otsDesc.put("AuthMode", "ak");
            otsDesc.put("AccessId", accessId);
            otsDesc.put("AccessKey", accessKey);
        }
        else
            otsDesc.put("AuthMode", "sts");
        if (OffsetStoreType.COORDINATOR == offsetStoreType)
            otsDesc.put("OffsetStoreType", "coordinator");
        else
            otsDesc.put("OffsetStoreType", "mysql");
        return otsDesc;
    }

    @Override
    public void ParseFromJsonNode(JsonNode node) {
        if (node != null && !node.isNull()) {
            JsonNode sub = node.get("Endpoint");
            if (sub != null && !sub.isNull()) {
                endpoint = sub.asText();
            }
            sub = node.get("InstanceName");
            if (sub != null && !sub.isNull()) {
                instance = sub.asText();
            }
            sub = node.get("TableName");
            if (sub != null && !sub.isNull()) {
                table = sub.asText();
            }
            sub = node.get("AuthMode");
            if (sub != null && !sub.isNull()) {
                if ("ak" == sub.asText())
                    authMode = AuthMode.AK;
                else
                    authMode = AuthMode.STS;
            }
            sub = node.get("AccessId");
            if (sub != null && !sub.isNull()) {
                accessId = sub.asText();
            }
            sub = node.get("AccessKey");
            if (sub != null && !sub.isNull()) {
                accessKey = sub.asText();
            }
            sub = node.get("OffsetStoreType");
            if (sub != null && !sub.isNull()) {
                if (sub.asText().equals("coordinator"))
                    offsetStoreType = OffsetStoreType.COORDINATOR;
                else
                    offsetStoreType = OffsetStoreType.MYSQL;
            }
        } else {
            throw new DatahubClientException("Invalid response, missing config.");
        }
    }
}
