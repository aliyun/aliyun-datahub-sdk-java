package com.aliyun.datahub.client.ut;

import com.aliyun.datahub.client.exception.InvalidParameterException;
import com.aliyun.datahub.client.model.GetProjectResult;
import com.aliyun.datahub.client.model.ListProjectResult;
import org.mockserver.model.Header;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.junit.Assert.fail;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

public class ProjectTest extends MockServer {
    @Test
    public void testListProject() {
        mockServerClient.when(request())
                .respond(
                        response().withStatusCode(200)
                                .withHeaders(
                                        new Header("x-datahub-request-id", "testId"),
                                        new Header("content-type", "application/json")
                                )
                                .withBody("{\"ProjectNames\": [\"project1\", \"project2\"]}")
                );

        ListProjectResult listProjectResult = client.listProject();
        Assert.assertTrue(listProjectResult.getProjectNames().contains("project1"));
        Assert.assertTrue(listProjectResult.getProjectNames().contains("project2"));

        mockServerClient.verify(
                request()
                        .withMethod("GET")
                        .withPath("/projects")
                        .withHeader("x-datahub-client-version", "1.1"));
    }

    @Test
    public void testGetProject() {
        mockServerClient.when(request()).respond(
                response()
                        .withStatusCode(200)
                        .withHeaders(
                                new Header("x-datahub-request-id", "testId"),
                                new Header("Content-Type", "application/json")
                        ).withBody("{\"CreateTime\": 1, \"LastModifyTime\":2, \"Comment\":\"test\"}")
        );

        GetProjectResult getProjectResult = client.getProject("test_project");
        Assert.assertEquals("testId", getProjectResult.getRequestId());
        Assert.assertEquals(1, getProjectResult.getCreateTime());
        Assert.assertEquals(2, getProjectResult.getLastModifyTime());
        Assert.assertEquals("test", getProjectResult.getComment());

        mockServerClient.verify(
                request().withMethod("GET").withPath("/projects/test_project")
        );
    }

    @Test
    public void testGetProjectWithNull() {
        try {
            client.getProject(null);
            fail("Get project with null not throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
            InvalidParameterException ex = (InvalidParameterException)e;
            Assert.assertNull(ex.getErrorCode());
        }
    }

    @Test
    public void testCreateProject() {
        mockServerClient.when(request()).respond(
                response()
                        .withStatusCode(200)
                        .withHeaders(
                                new Header("x-datahub-request-id", "testId"),
                                new Header("Content-Type", "application/json")
                        )
        );
        client.createProject("test_project", "test_comment");
        mockServerClient.verify(
                request().withMethod("POST").withPath("/projects/test_project")
                        .withBody(exact("{\"Comment\":\"test_comment\"}"))
        );
    }

    @Test
    public void testCreateProjectWithNull() {
        try {
            client.createProject(null, "comment");
            fail("Create project with null not throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
            InvalidParameterException ex = (InvalidParameterException)e;
            Assert.assertNull(ex.getErrorCode());
        }

        try {
            client.createProject("test_project", null);
            fail("Create project with null not throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
            InvalidParameterException ex = (InvalidParameterException)e;
            Assert.assertNull(ex.getErrorCode());
        }
    }

    @Test
    public void testCreateProjectWithInvalid() {

        try {
            client.createProject("11aabb", "");
            fail("Create project with null not throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
            InvalidParameterException ex = (InvalidParameterException)e;
            Assert.assertNull(ex.getErrorCode());
        }

        try {
            client.createProject("aaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbMaxLength", "");
            fail("Create project with null not throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
            InvalidParameterException ex = (InvalidParameterException)e;
            Assert.assertNull(ex.getErrorCode());
        }
    }


    @Test
    public void testUpdateProject() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                )
        );
        client.updateProject("test_project", "update_comment");
        mockServerClient.verify(
                request().withMethod("PUT").withPath("/projects/test_project")
                        .withBody(exact("{\"Comment\":\"update_comment\"}"))
        );
    }

    @Test
    public void testDeleteProject() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                )
        );
        client.deleteProject("test_project");
        mockServerClient.verify(
                request().withMethod("DELETE").withPath("/projects/test_project")
        );
    }
}
