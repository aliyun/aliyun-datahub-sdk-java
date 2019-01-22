package com.aliyun.datahub.model;

import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.exception.DatahubServiceException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ElasticSearchDesc extends ConnectorConfig {
    private String index;
    private String endpoint;
    private String user;
    private String password;
    private List<String> idFields;
    private List<String> typeFields;
    private Long maxCommitSize;
    private boolean proxyMode = false;

    public ElasticSearchDesc() {
        idFields = new ArrayList<String>();
        typeFields = new ArrayList<String>();
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public List<String> getIdFields() {
        return idFields;
    }

    public void setIdFields(List<String> idFields) {
        this.idFields = idFields;
    }

    public List<String> getTypeFields() {
        return typeFields;
    }

    public void setTypeFields(List<String> typeFields) {
        this.typeFields = typeFields;
    }

    public Long getMaxCommitSize() {
        return maxCommitSize;
    }

    public void setMaxCommitSize(Long maxCommitSize) {
        this.maxCommitSize = maxCommitSize;
    }

    public boolean isProxyMode() {
        return proxyMode;
    }

    public void setProxyMode(boolean proxyMode) {
        this.proxyMode = proxyMode;
    }

    @Override
    public ObjectNode toJsonNode() {
        ObjectMapper mapper = JacksonParser.getObjectMapper();
        ObjectNode esNode = mapper.createObjectNode();
        esNode.put("Index", index);
        esNode.put("Endpoint", endpoint);
        esNode.put("User", user);
        esNode.put("Password", password);
        ArrayNode ids = mapper.createArrayNode();
        for (String i : idFields) {
            ids.add(i);
        }
        esNode.put("IDFields", ids);
        ArrayNode types = mapper.createArrayNode();
        for (String i : typeFields) {
            types.add(i);
        }
        esNode.put("TypeFields", types);
        if (maxCommitSize != null) {
            esNode.put("MaxCommitSize", String.valueOf(maxCommitSize));
        }
        esNode.put("ProxyMode", String.valueOf(proxyMode));
        return esNode;
    }

    @Override
    public void ParseFromJsonNode(JsonNode node) {
        if (node != null && !node.isNull()) {
            JsonNode config = node.get("Index");
            if (config != null && !config.isNull()) {
                setIndex(config.asText());
            }
            config = node.get("Endpoint");
            if (config != null && !config.isNull()) {
                setEndpoint(config.asText());
            }
            config = node.get("MaxCommitSize");
            if (config != null && !config.isNull()) {
                setMaxCommitSize(Long.valueOf(config.asText()));
            }
            config = node.get("ProxyMode");
            if (config != null && !config.isNull()) {
                setProxyMode(Boolean.valueOf(config.asText()));
            }
            config = node.get("IDFields");
            if (config != null && !config.isNull()) {
                String fields = config.asText();
                ObjectMapper mapper = JacksonParser.getObjectMapper();
                JsonNode tree = null;
                try {
                    tree = mapper.readTree(fields);
                } catch (IOException e) {
                    throw new DatahubServiceException("Parse IDFields failed:" + fields);
                }
                List<String> idFields = new ArrayList<String>();
                if (tree != null && !tree.isNull()) {
                    if (tree.isArray()) {
                        Iterator<JsonNode> it = tree.getElements();
                        while (it.hasNext()) {
                            JsonNode field = it.next();
                            idFields.add(field.asText());
                        }
                    }
                }
                setIdFields(idFields);
            }
            config = node.get("TypeFields");
            if (config != null && !config.isNull()) {
                String fields = config.asText();
                ObjectMapper mapper = JacksonParser.getObjectMapper();
                JsonNode tree = null;
                try {
                    tree = mapper.readTree(fields);
                } catch (IOException e) {
                    throw new DatahubServiceException("Parse typeFields failed:" + fields);
                }
                List<String> typeFields = new ArrayList<String>();
                if (tree != null && !tree.isNull()) {
                    if (tree.isArray()) {
                        Iterator<JsonNode> it = tree.getElements();
                        while (it.hasNext()) {
                            JsonNode field = it.next();
                            typeFields.add(field.asText());
                        }
                    }
                }
                setTypeFields(typeFields);
            }
        }  else {
            throw new DatahubClientException("Invalid response, missing config.");
        }
    }
}
