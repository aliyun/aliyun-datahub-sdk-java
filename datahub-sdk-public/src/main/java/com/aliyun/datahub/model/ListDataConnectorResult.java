package com.aliyun.datahub.model;

import java.util.ArrayList;
import java.util.List;

public class ListDataConnectorResult extends Result {
    private com.aliyun.datahub.client.model.ListConnectorResult proxyResult;

    public ListDataConnectorResult(com.aliyun.datahub.client.model.ListConnectorResult proxyResult) {
        this.proxyResult = proxyResult;
        setRequestId(proxyResult.getRequestId());
    }


    public ListDataConnectorResult() {
        proxyResult = new com.aliyun.datahub.client.model.ListConnectorResult();
    }

    public void setDataConnectors(List<String> connectors) {
        proxyResult.setConnectorNames(connectors);
    }

    public void addDataConnector(String connector) {
        List<String> connectorNames = proxyResult.getConnectorNames();
        if (connectorNames == null) {
            connectorNames = new ArrayList<>();
            proxyResult.setConnectorNames(connectorNames);
        }
        connectorNames.add(connector);
    }

    public List<String> getDataConnectors() {
        return proxyResult.getConnectorNames();
    }
}
