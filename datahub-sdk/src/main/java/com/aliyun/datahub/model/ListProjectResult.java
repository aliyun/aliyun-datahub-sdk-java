package com.aliyun.datahub.model;

import java.util.ArrayList;
import java.util.List;

public class ListProjectResult {
    private List<String> projectNames;

    public ListProjectResult() {
        this.projectNames = new ArrayList<String>();
    }

    public void addProject(String projectName) {
        this.projectNames.add(projectName);
    }

    public List<String> getProjectNames() {
        return projectNames;
    }

    public void setProjectNames(List<String> projectNames) {
        this.projectNames = projectNames;
    }
}
