package com.aliyun.datahub.model;

import java.util.ArrayList;
import java.util.List;

public class ListProjectResult extends Result {
    private com.aliyun.datahub.client.model.ListProjectResult proxyResult;

    public ListProjectResult(com.aliyun.datahub.client.model.ListProjectResult proxyResult) {
        this.proxyResult = proxyResult;
        setRequestId(proxyResult.getRequestId());
    }

    public ListProjectResult() {
        proxyResult = new com.aliyun.datahub.client.model.ListProjectResult();
    }

    public void addProject(String projectName) {
        List<String> projectNames = proxyResult.getProjectNames();
        if (projectName == null) {
            projectNames = new ArrayList<>();
            proxyResult.setProjectNames(projectNames);
        }

        projectNames.add(projectName);
    }

    public List<String> getProjectNames() {
        return proxyResult.getProjectNames();
    }

    public void setProjectNames(List<String> projectNames) {
        proxyResult.setProjectNames(projectNames);
    }
}
