package com.aliyun.datahub.model;

import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.common.data.RecordType;

import java.util.ArrayList;
import java.util.List;
@Deprecated
public class GetConnectorResult {
    private String state;
    private ConnectorType type;
    private String creator;
    private String owner;
    private List<String> columnFields;
    private OdpsDesc odpsDesc;
    private List<ShardContext> shardContexts;

    public GetConnectorResult(){
        this.columnFields = new ArrayList<String>();
        this.shardContexts = new ArrayList<ShardContext>();
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getCreator() {
        return creator;
    }

    public void setCreator(String creator) {
        this.creator = creator;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public List<ShardContext> getShardContexts() {
        return shardContexts;
    }

    public void setShardContext(List<ShardContext> shardContexts) {
        this.shardContexts = shardContexts;
    }

    public List<String> getColumnFields() {
        return columnFields;
    }

    public void setColumnFields(List<String> columnFields) {
        this.columnFields = columnFields;
    }

    public OdpsDesc getOdpsDesc() {
        return odpsDesc;
    }

    public void setOdpsDesc(OdpsDesc odpsDesc) {
        this.odpsDesc = odpsDesc;
    }

    public ConnectorType getType() {
        return type;
    }

    public void setType(String type) {
        this.type = ConnectorType.valueOf(type.toUpperCase());
    }
}
