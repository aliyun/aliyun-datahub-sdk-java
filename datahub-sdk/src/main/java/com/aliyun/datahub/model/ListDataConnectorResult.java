package com.aliyun.datahub.model;

import java.util.ArrayList;
import java.util.List;

public class ListDataConnectorResult {
    private List<String> dataConnectors;

    public ListDataConnectorResult() {
        this.dataConnectors = new ArrayList<String>();
    }

    public void setDataConnectors(List<String> connectors) {
        this.dataConnectors = connectors;
    }

    public void addDataConnector(String connector) {
        this.dataConnectors.add(connector);
    }

    public List<String> getDataConnectors() {
        return this.dataConnectors;
    }
}
