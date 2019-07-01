package com.aliyun.datahub.client.ut;

import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.model.GetProjectResult;
import org.apache.commons.lang3.StringUtils;
import org.mockserver.model.Header;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.junit.Assert.fail;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

public class ErrorResponseTest extends MockServer {

    @Test
    public void testResponseErrorNormal() {
        // normal json error response
        mockServerClient.when(
                request()
                        .withMethod("GET")
                        .withPath("/projects/test_project")
                        .withHeader("x-datahub-client-version", "1.1")
        ).respond(
                response()
                        .withStatusCode(400)
                        .withHeaders(
                                new Header("x-datahub-request-id", "testId"),
                                new Header("content-type", "application/json")
                        ).withBody("{\"ErrorCode\":\"testCode\", \"ErrorMessage\":\"testMessage\"}")
        );

        try {
            GetProjectResult getProjectResult = client.getProject("test_project");
            fail("TestRepsonseErrorNormal not throw exception");
        } catch (DatahubClientException e) {
            Assert.assertEquals(400, e.getHttpStatus());
            Assert.assertEquals("testCode", e.getErrorCode());
            Assert.assertEquals("[httpStatus:400, requestId:testId, errorCode:testCode, errorMessage:testMessage]", e.getMessage());
            Assert.assertEquals("testId", e.getRequestId());
        }
    }

    @Test
    public void testResponseErrorWithNotJsonType() {
        mockServerClient.when(
                request()
                        .withMethod("GET")
                        .withPath("/projects/test_project")
                        .withHeader("x-datahub-client-version", "1.1")
        ).respond(
                response()
                        .withStatusCode(500)
                        .withHeaders(
                                new Header("content-type", "text/plain")
                        ).withBody("plain text xxx")
        );

        try {
            GetProjectResult getProjectResult = client.getProject("test_project");
            fail("TestResponseErrorWithNotJsonType not throw exception");
        } catch (DatahubClientException e) {
            Assert.assertNull(e.getRequestId());
            Assert.assertEquals(500, e.getHttpStatus());
            Assert.assertNull(e.getErrorCode());
            Assert.assertEquals("[httpStatus:500, requestId:null, errorCode:null, errorMessage:plain text xxx]", e.getMessage());
        }
    }

    @Test
    public void testResponseErrorWithNoBody() {
        mockServerClient.when(
                request()
                        .withMethod("GET")
                        .withPath("/projects/test_project")
                        .withHeader("x-datahub-client-version", "1.1")
        ).respond(
                response()
                        .withStatusCode(500)
                        .withHeaders(
                                new Header("content-type", "text/plain")
                        )
        );

        try {
            GetProjectResult getProjectResult = client.getProject("test_project");
            fail("testResponseErrorWithNoBody not throw exception");
        } catch (DatahubClientException e) {
            Assert.assertNull(e.getRequestId());
            Assert.assertEquals(500, e.getHttpStatus());
            Assert.assertNull(e.getErrorCode());
            Assert.assertNotNull(e.getMessage());
            Assert.assertEquals("[httpStatus:500, requestId:null, errorCode:null, errorMessage:]", e.getMessage());
        }
    }

    @Test
    public void testResponseErrorWithEmptyBody() {
        mockServerClient.when(
                request()
                        .withMethod("GET")
                        .withPath("/projects/test_project")
                        .withHeader("x-datahub-client-version", "1.1")
        ).respond(
                response()
                        .withStatusCode(500)
                        .withHeaders(
                                new Header("content-type", "text/plain")
                        ).withBody("")
        );

        try {
            GetProjectResult getProjectResult = client.getProject("test_project");
            fail("testResponseErrorWithNoBody not throw exception");
        } catch (DatahubClientException e) {
            Assert.assertNull(e.getRequestId());
            Assert.assertEquals(500, e.getHttpStatus());
            Assert.assertNull(e.getErrorCode());
            Assert.assertEquals("[httpStatus:500, requestId:null, errorCode:null, errorMessage:]", e.getMessage());
        }

    }

    @Test
    public void testResponseErrorWithErrorJson() {
        mockServerClient.when(
                request()
                        .withMethod("GET")
                        .withPath("/projects/test_project")
                        .withHeader("x-datahub-client-version", "1.1")
        ).respond(
                response()
                        .withStatusCode(500)
                        .withHeaders(
                                new Header("content-type", "application/json")
                        ).withBody("{\"ErrorCode\":[\"1\", \"2\"]}")
        );

        try {
            GetProjectResult getProjectResult = client.getProject("test_project");
            fail("testResponseErrorWithErrorJson not throw exception");
        } catch (DatahubClientException e) {
            Assert.assertNull(e.getRequestId());
            Assert.assertEquals(500, e.getHttpStatus());
            Assert.assertTrue(StringUtils.isEmpty(e.getErrorCode()));
            Assert.assertTrue(StringUtils.isEmpty(e.getErrorMessage()));
        }

    }
}
