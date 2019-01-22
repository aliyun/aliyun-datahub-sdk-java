package com.aliyun.datahub.model;

import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubClientException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

/**
 * Created by wz on 17/4/1.
 */
public class OssDesc extends ConnectorConfig {
    private String endpoint;
    private String bucket;
    private String prefix;
    private String timeFormat;
    private int timeRange;
    private String authMode;
    private String accessId;
    private String accessKey;

    public OssDesc() {}

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public String getTimeFormat() {
        return timeFormat;
    }

    public void setTimeFormat(String timeFormat) {
        this.timeFormat = timeFormat;
    }

    public int getTimeRange() {
        return timeRange;
    }

    public void setTimeRange(int timeRange) {
        this.timeRange = timeRange;
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
        ObjectNode ossDesc = mapper.createObjectNode();
        ossDesc.put("Endpoint", endpoint);
        ossDesc.put("Bucket", bucket);
        ossDesc.put("Prefix", prefix);
        ossDesc.put("TimeFormat", timeFormat);
        ossDesc.put("TimeRange", timeRange);
        ossDesc.put("AuthMode", authMode);
        if (authMode.equals("ak")) {
            ossDesc.put("AccessId", accessId);
            ossDesc.put("AccessKey", accessKey);
        }
        return ossDesc;
    }

    @Override
    public void ParseFromJsonNode(JsonNode node) {
        if (node != null && !node.isNull()) {
            JsonNode sub = node.get("Endpoint");
            if (sub != null && !sub.isNull()) {
                endpoint = sub.asText();
            }
            sub = node.get("Bucket");
            if (sub != null && !sub.isNull()) {
                bucket = sub.asText();
            }
            sub = node.get("Prefix");
            if (sub != null && !sub.isNull()) {
                prefix = sub.asText();
            }
            sub = node.get("TimeFormat");
            if (sub != null && !sub.isNull()) {
                timeFormat = sub.asText();
            }
            sub = node.get("TimeRange");
            if (sub != null && !sub.isNull()) {
                timeRange = sub.asInt();
            }
            sub = node.get("AuthMode");
            if (sub != null && !sub.isNull()) {
                authMode = sub.asText();
            }
            sub = node.get("AccessId");
            if (sub != null && !sub.isNull()) {
                accessId = sub.asText();
            }
            sub = node.get("AccessKey");
            if (sub != null && !sub.isNull()) {
                accessKey = sub.asText();
            }
        } else {
            throw new DatahubClientException("Invalid response, missing config.");
        }
    }
}
