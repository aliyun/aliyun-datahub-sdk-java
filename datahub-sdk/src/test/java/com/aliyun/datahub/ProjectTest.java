package com.aliyun.datahub;

import com.aliyun.datahub.common.data.RecordType;
import com.aliyun.datahub.exception.*;
import com.aliyun.datahub.model.GetProjectResult;
import com.aliyun.datahub.model.ListProjectResult;
import com.aliyun.datahub.util.DatahubTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test
public class ProjectTest {

    private DatahubClient client;
    private String projectName;

    @BeforeMethod
    public void init() {
        DatahubConfiguration conf = DatahubTestUtils.getConf();
        this.projectName = DatahubTestUtils.getRamdomProjectName();
        this.client = new DatahubClient(conf);
    }

    @Test
    public void testCreateProjectNormal() {
        String desc = "This is a test project";
        client.createProject(this.projectName, desc);
        client.deleteProject(this.projectName);
    }

    @Test(expectedExceptions = InvalidParameterException.class)
    public void testCreateProjectWithNullProject() {
        String desc = "This is a test project";
        client.createProject(null, desc);
    }

    @Test(expectedExceptions = InvalidParameterException.class)
    public void testCreateProjectWithNullComment() {
        client.createProject(this.projectName, null);
    }

    @Test(expectedExceptions = ResourceExistException.class)
    public void testCreateProjectWithProjectExist() {
        String desc = "This is a test project";
        try {
            client.createProject(this.projectName, desc);
            client.createProject(this.projectName, desc);
        } finally {
            client.deleteProject(this.projectName);
        }
    }

    @Test(expectedExceptions = ResourceNotFoundException.class)
    void testDeleteProjectWithProjectNotExist() {
        client.deleteProject(this.projectName);
    }

    @Test(expectedExceptions = InvalidParameterException.class)
    void testDeleteProjectWithNullProject() {
        String projectName = null;
        client.deleteProject(projectName);
    }

    @Test
    void testDeleteProjectNormal() {
        String desc = "This is a test project";
        client.createProject(this.projectName, desc);
        client.deleteProject(this.projectName);
        client.createProject(this.projectName, desc);
        client.deleteProject(this.projectName);
    }

    @Test(expectedExceptions = ResourceNotFoundException.class)
    void testGetProjectWithProjectNotExist() {
        client.getProject(this.projectName);
    }

    @Test(expectedExceptions = InvalidParameterException.class)
    void testGetProjectWithNullProject() {
        String projectName = null;
        client.getProject(projectName);
    }

    @Test
    public void testGetProjectNormal() {
        String desc = "get project test";
        client.createProject(this.projectName, desc);
        GetProjectResult result = client.getProject(this.projectName);
        Assert.assertEquals(result.getProjectName(), this.projectName);
        Assert.assertEquals(result.getComment(), desc);

        client.deleteProject(this.projectName);
    }

    @Test
    public void testGetProjectNotOwner() {
        String desc = "get project test";
        client.createProject(this.projectName, desc);

        try {
            DatahubClient secondClient = new DatahubClient(DatahubTestUtils.getSecondSubConf());
            secondClient.getProject(this.projectName);
            Assert.assertTrue(false);
        } catch (NoPermissionException e) {
            Assert.assertTrue(true);
        }
        client.deleteProject(this.projectName);
    }

    @Test
    public void testListProjectNormal() {
        String desc = "list project test";
        client.createProject(projectName, desc);

        ListProjectResult rs = client.listProject();
        Assert.assertTrue(rs.getProjectNames().contains(projectName.toLowerCase()));

        client.deleteProject(projectName);
    }

    @Test
    void testDeleteProjectWithTopic() {
        String desc = "This is a test project";
        client.createProject(this.projectName, desc);
        client.createTopic(projectName, "testTopic", 1, 1, RecordType.BLOB, "test");
        try {
            client.deleteProject(this.projectName);
            Assert.assertTrue(false);
        } catch (OperationDeniedException e) {
            e.printStackTrace();
            Assert.assertTrue(true);
        }
        client.deleteTopic(projectName, "testTopic");
        client.deleteProject(this.projectName);
    }
}
