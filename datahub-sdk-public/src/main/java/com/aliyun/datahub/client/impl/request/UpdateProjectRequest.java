package com.aliyun.datahub.client.impl.request;

import com.fasterxml.jackson.annotation.JsonProperty;

public class UpdateProjectRequest {
    @JsonProperty("Comment")
    private String comment;

    public String getComment() {
        return comment;
    }

    public UpdateProjectRequest setComment(String comment) {
        this.comment = comment;
        return this;
    }
}
