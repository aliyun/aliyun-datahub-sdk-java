package com.aliyun.datahub.model;

public class GetProjectResult extends Result {
    private com.aliyun.datahub.client.model.GetProjectResult proxyResult;

    public GetProjectResult(com.aliyun.datahub.client.model.GetProjectResult proxyResult) {
        this.proxyResult = proxyResult;
        setRequestId(proxyResult.getRequestId());
    }

    public GetProjectResult() {
        proxyResult = new com.aliyun.datahub.client.model.GetProjectResult();
    }

    private String projectName;
    private long createTime;
    private long lastModifyTime;
    private String comment;

    public String getProjectName() {
        return proxyResult.getProjectName();
    }

    public void setProjectName(String projectName) {
        proxyResult.setProjectName(projectName);
    }

    public long getCreateTime() {
        return proxyResult.getCreateTime();
    }

    public void setCreateTime(long createTime) {
        proxyResult.setCreateTime(createTime);
    }

    public long getLastModifyTime() {
        return proxyResult.getLastModifyTime();
    }

    public void setLastModifyTime(long lastModifyTime) {
        proxyResult.setLastModifyTime(lastModifyTime);
    }

    public String getComment() {
        return proxyResult.getComment();
    }

    public void setComment(String comment) {
        proxyResult.setComment(comment);
    }
}
