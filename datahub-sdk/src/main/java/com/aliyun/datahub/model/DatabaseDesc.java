package com.aliyun.datahub.model;

import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.DatahubClientException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

public class DatabaseDesc extends ConnectorConfig {
    private String host;
    private Integer port;
    private String database;
    private String user;
    private String password;
    private String table;
    private Long maxCommitSize;
    private boolean ignore = true;

    public DatabaseDesc() {}

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
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

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public boolean isIgnore() {
        return ignore;
    }

    public void setIgnore(boolean ignore) {
        this.ignore = ignore;
    }

    public Long getMaxCommitSize() {
        return maxCommitSize;
    }

    public void setMaxCommitSize(Long maxCommitSize) {
        this.maxCommitSize = maxCommitSize;
    }

    @Override
    public ObjectNode toJsonNode() {
        ObjectMapper mapper = JacksonParser.getObjectMapper();
        ObjectNode dbNode = mapper.createObjectNode();
        dbNode.put("Host", host);
        dbNode.put("Port", String.valueOf(port));
        dbNode.put("Ignore", String.valueOf(ignore));
        dbNode.put("Database", database);
        dbNode.put("User", user);
        dbNode.put("Password", password);
        dbNode.put("Table", table);
        if (maxCommitSize != null) {
            dbNode.put("MaxCommitSize", String.valueOf(maxCommitSize));
        }
        return dbNode;
    }

    @Override
    public void ParseFromJsonNode(JsonNode node) {
        if (node != null && !node.isNull()) {
            JsonNode config = node.get("Host");
            if (config != null && !config.isNull()) {
                setHost(config.asText());
            }
            config = node.get("Port");
            if (config != null && !config.isNull()) {
                setPort(Integer.valueOf(config.asText()));
            }
            config = node.get("Ignore");
            if (config != null && !config.isNull()) {
                setIgnore(Boolean.valueOf(config.asText()));
            }
            config = node.get("Database");
            if (config != null && !config.isNull()) {
                setDatabase(config.asText());
            }
            config = node.get("Table");
            if (config != null && !config.isNull()) {
                setTable(config.asText());
            }
            config = node.get("MaxCommitSize");
            if (config != null && !config.isNull()) {
                setMaxCommitSize(Long.valueOf(config.asText()));
            }
        }  else {
            throw new DatahubClientException("Invalid response, missing config.");
        }
    }
}
