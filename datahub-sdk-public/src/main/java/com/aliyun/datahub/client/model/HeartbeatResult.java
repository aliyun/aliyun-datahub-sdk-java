package com.aliyun.datahub.client.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class HeartbeatResult extends BaseResult {
    @JsonProperty("PlanVersion")
    private long planVersion;

    @JsonProperty("ShardList")
    private List<String> shardList;

    @JsonProperty("TotalPlan")
    private String totalPlan;

    public long getPlanVersion() {
        return planVersion;
    }

    public void setPlanVersion(long planVersion) {
        this.planVersion = planVersion;
    }

    public List<String> getShardList() {
        return shardList;
    }

    public void setShardList(List<String> shardList) {
        this.shardList = shardList;
    }

    public String getTotalPlan() {
        return totalPlan;
    }

    public void setTotalPlan(String totalPlan) {
        this.totalPlan = totalPlan;
    }
}
