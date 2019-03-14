package com.aliyun.datahub.client.model;

import java.util.List;

public class SinkEsConfig extends SinkConfig {
    /**
     * Elastic search index
     */
    private String index;

    /**
     * Elastic search service endpoint
     */
    private String endpoint;

    /**
     * User used to synchronize data
     */
    private String user;

    /**
     * User password to synchronize data
     */
    private String password;

    /**
     * Id fields to synchronize data
     */
    private List<String> idFields;

    /**
     * Type fields to synchronize data
     */
    private List<String> typeFields;

    private boolean proxyMode = true;

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

    public boolean isProxyMode() {
        return proxyMode;
    }

    public void setProxyMode(boolean proxyMode) {
        this.proxyMode = proxyMode;
    }
}
