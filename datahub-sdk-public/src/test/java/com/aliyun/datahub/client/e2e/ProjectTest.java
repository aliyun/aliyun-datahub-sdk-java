package com.aliyun.datahub.client.e2e;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.common.ErrorCode;
import com.aliyun.datahub.client.e2e.common.Configure;
import com.aliyun.datahub.client.exception.NoPermissionException;
import com.aliyun.datahub.client.exception.ResourceNotFoundException;
import com.aliyun.datahub.client.model.*;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.aliyun.datahub.client.e2e.common.Constant.*;
import static org.junit.Assert.fail;

public class ProjectTest extends BaseTest {
    @BeforeClass
    public static void setUpBeforeClass() {
        bCreateTopic = false;
        BaseTest.setUpBeforeClass();
    }

    @Test
    public void normalTest() {
        String projectName = "tmp_" + System.currentTimeMillis();
        // create project
        CreateProjectResult createProjectResult = client.createProject(projectName, "comment");

        // get project
        GetProjectResult getProjectResult = client.getProject(projectName);
        Assert.assertEquals("comment", getProjectResult.getComment());
        Assert.assertEquals(projectName, getProjectResult.getProjectName());

        // update project
        UpdateProjectResult updateProjectResult = client.updateProject(projectName, "update sdk test project");
        getProjectResult = client.getProject(projectName);
        Assert.assertEquals("update sdk test project", getProjectResult.getComment());

        // list project
        ListProjectResult listProjectResult = client.listProject();
        Assert.assertTrue(listProjectResult.getProjectNames().contains(projectName));

        // delete project
        DeleteProjectResult deleteProjectResult = client.deleteProject(projectName);
        System.out.println(deleteProjectResult.getRequestId());
    }

    @Test
    public void testGetUnknownProject() {
        try {
            client.getProject("UnknownProject");
            fail("Get project with unknown not throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ResourceNotFoundException);
        }
    }

    @Test
    public void testNoPermissionOperator() {
        String projectName = "tmp_" + System.currentTimeMillis();
        // delete project with subuser
        DatahubClient subUserClient = DatahubClientBuilder.newBuilder().setDatahubConfig(
                new DatahubConfig(Configure.getString(DATAHUB_ENDPOINT),
                        new AliyunAccount(Configure.getString(DATAHUB_SUBUSER_ACCESS_ID), Configure.getString(DATAHUB_SUBUSER_ACCESS_KEY))
                )
        ).build();

        client.createProject(projectName, "comment");
        try {
            subUserClient.deleteProject(projectName);
            fail("Delete project with subuser not throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof NoPermissionException);
        }

        client.deleteProject(projectName);
    }

    @Test
    public void testCreateIgnoreCase() {
        String projectName = "tmp_" + System.currentTimeMillis();
        client.createProject(projectName.toUpperCase(), "comment");
        GetProjectResult getProjectResult = client.getProject(projectName.toUpperCase());
        Assert.assertEquals(projectName.toLowerCase(), getProjectResult.getProjectName());
        client.deleteProject(projectName);
    }

    @Test
    public void testDeleteProjectWithTopicExist() {
        String projectName = "tmp_" + System.currentTimeMillis();
        client.createProject(projectName, "comment");
        String topicName = getTestTopicName(RecordType.TUPLE);
        client.createTopic(projectName, topicName, 1, 1, RecordType.BLOB, "comment");
        try {
            client.deleteProject(projectName);
            fail("Delete project with topic exists not throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof NoPermissionException);
            Assert.assertEquals(ErrorCode.OPERATOR_DENIED, ((NoPermissionException)e).getErrorCode());
        }
        client.deleteTopic(projectName, topicName);
        client.deleteProject(projectName);
    }
}
