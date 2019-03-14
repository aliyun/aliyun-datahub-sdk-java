package com.aliyun.datahub.client.impl.request;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CreateProjectRequest {
    @JsonProperty("Comment")
    private String comment;

    public String getComment() {
        return comment;
    }

    public CreateProjectRequest setComment(String comment) {
        this.comment = comment;
        return this;
    }
}
