package com.aliyun.datahub.model;

import java.util.ArrayList;
import java.util.List;
@Deprecated
public class ListConnectorResult {
    private List<String> connectors;

    public ListConnectorResult() {
        this.connectors = new ArrayList<String>();
    }

    public void setConnectors(List<String> connectors) {
        this.connectors = connectors;
    }

    public void addConnector(String connector) {
        this.connectors.add(connector);
    }

    public List<String> getConnectors() {
        return this.connectors;
    }
}
