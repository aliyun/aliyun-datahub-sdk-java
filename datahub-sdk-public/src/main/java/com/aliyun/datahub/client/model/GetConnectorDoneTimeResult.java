package com.aliyun.datahub.client.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class GetConnectorDoneTimeResult extends BaseResult {
    @JsonProperty("DoneTime")
    private Long doneTime;

    public Long getDoneTime() {
        return doneTime;
    }

    public void setDoneTime(Long doneTime) {
        this.doneTime = doneTime;
    }
}
