package com.aliyun.datahub.model;

import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.exception.DatahubServiceException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class OdpsDesc extends ConnectorConfig {
    public enum PartitionMode {
        USER_DEFINE,
        SYSTEM_TIME,
        EVENT_TIME
    }

    private String projectName;
    private String tableName;
    private String odpsEndpoint;
    private String tunnelEndpoint;
    private String accessId;
    private String accessKey;
    private PartitionMode partitionMode;
    private int timeRange;
    private Map<String, String> partitionConfig;

    public OdpsDesc() {
        partitionMode = PartitionMode.USER_DEFINE;
        partitionConfig = new LinkedHashMap<String, String>();
    }

    public String getProject() {
        return projectName;
    }

    public void setProject(String projectName) {
        this.projectName = projectName;
    }

    public String getTable() {
        return tableName;
    }

    public void setTable(String tableName) {
        this.tableName = tableName;
    }

    public String getOdpsEndpoint() {
        return odpsEndpoint;
    }

    public void setOdpsEndpoint(String odpsEndpoint) {
        this.odpsEndpoint = odpsEndpoint;
    }

    public String getTunnelEndpoint() {
        return tunnelEndpoint;
    }

    public void setTunnelEndpoint(String tunnelEndpoint) {
        this.tunnelEndpoint = tunnelEndpoint;
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

    public PartitionMode getPartitionMode() {
        return partitionMode;
    }

    public void setPartitionMode(PartitionMode partitionMode) {
        this.partitionMode = partitionMode;
    }

    public int getTimeRange() {
        return timeRange;
    }

    public void setTimeRange(int timeRange) {
        this.timeRange = timeRange;
    }

    public Map<String, String> getPartitionConfig() {
        return partitionConfig;
    }

    public void setPartitionConfig(Map<String, String> partitionConfig) {
        this.partitionConfig = partitionConfig;
    }

    @Override
    public ObjectNode toJsonNode() {
        ObjectMapper mapper = JacksonParser.getObjectMapper();
        ObjectNode odpsDesc = mapper.createObjectNode();
        odpsDesc.put("Project", projectName);
        odpsDesc.put("Table", tableName);
        odpsDesc.put("OdpsEndpoint", odpsEndpoint);
        if (tunnelEndpoint != null && !tunnelEndpoint.isEmpty()) {
            odpsDesc.put("TunnelEndpoint", tunnelEndpoint);
        }
        odpsDesc.put("AccessId", accessId);
        odpsDesc.put("AccessKey", accessKey);
        odpsDesc.put("PartitionMode", partitionMode.toString());
        odpsDesc.put("TimeRange", timeRange);
        ObjectNode config = mapper.createObjectNode();
        for (Map.Entry<String,String> entry : partitionConfig.entrySet()) {
            config.put(entry.getKey(), entry.getValue());
        }
        odpsDesc.put("PartitionConfig", config);
        return odpsDesc;
    }

    @Override
    public void ParseFromJsonNode(JsonNode node) {
        if (node != null && !node.isNull()) {
            JsonNode odps = node.get("Project");
            if (odps != null && !odps.isNull()) {
                setProject(odps.asText());
            }
            odps = node.get("Table");
            if (odps != null && !odps.isNull()) {
                setTable(odps.asText());
            }
            odps = node.get("OdpsEndpoint");
            if (odps != null && !odps.isNull()) {
                setOdpsEndpoint(odps.asText());
            }
            odps = node.get("TunnelEndpoint");
            if (odps != null && !odps.isNull()) {
                setTunnelEndpoint(odps.asText());
            }
            odps = node.get("TunnelEndpoint");
            if (odps != null && !odps.isNull()) {
                setTunnelEndpoint(odps.asText());
            }
            odps = node.get("PartitionMode");
            if (odps != null && !odps.isNull()) {
                setPartitionMode(PartitionMode.valueOf(odps.asText()));
            }
            odps = node.get("TimeRange");
            if (odps != null && !odps.isNull()) {
                setTimeRange(Integer.valueOf(odps.asText()));
            }
            odps = node.get("PartitionConfig");
            if (odps != null && !odps.isNull()) {
                String text = odps.asText();
                ObjectMapper mapper = JacksonParser.getObjectMapper();
                JsonNode tree = null;
                try {
                    tree = mapper.readTree(text);
                } catch (IOException e) {
                    throw new DatahubServiceException("Parse partition config failed:" + text);
                }
                Map<String, String> config = new LinkedHashMap<String, String>();
                if (tree != null && !tree.isNull() && tree.isArray()) {
                    Iterator<JsonNode> it = tree.getElements();
                    while (it.hasNext()) {
                        JsonNode configItem = it.next();
                        config.put(configItem.get("key").asText(), configItem.get("value").asText());
                    }
                }
                setPartitionConfig(config);
            }
        } else {
            throw new DatahubClientException("Invalid response, missing config.");
        }
    }
}
