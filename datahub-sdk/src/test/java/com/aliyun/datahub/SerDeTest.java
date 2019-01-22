package com.aliyun.datahub;

import com.aliyun.datahub.common.transport.DefaultResponse;
import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.model.*;
import com.aliyun.datahub.model.serialize.*;
import org.testng.annotations.*;

import java.util.ArrayList;
import java.util.List;
@Test
public class SerDeTest {

    @Test(expectedExceptions = DatahubServiceException.class)
    public void testGetProjectResultJsonDeserWithInvalidRequestBody() {
        final String project = "test_project";
        DefaultResponse response = new DefaultResponse();
        GetProjectRequest request = new GetProjectRequest(project);
        String malformedBody = "{";
        response.setBody(malformedBody.getBytes());
        GetProjectResultJsonDeser.getInstance().deserialize(request, response);
    }

    @Test(expectedExceptions = DatahubServiceException.class)
    public void testListProjectResultJsonDeserWithInvalidRequestBody() {
        DefaultResponse response = new DefaultResponse();
        ListProjectRequest request = new ListProjectRequest();
        String malformedBody = "{";
        response.setBody(malformedBody.getBytes());
        ListProjectResultJsonDeser.getInstance().deserialize(request, response);
    }

    @Test(expectedExceptions = DatahubServiceException.class)
    public void testGetTopicResultJsonDeserWithInvalidRequestBody() {
        final String project = "test_project";
        final String topic = "test_topic";
        DefaultResponse response = new DefaultResponse();
        GetTopicRequest request = new GetTopicRequest(project, topic);
        String malformedBody = "{";
        response.setBody(malformedBody.getBytes());
        GetTopicResultJsonDeser.getInstance().deserialize(request, response);
    }

    @Test(expectedExceptions = DatahubServiceException.class)
    public void testListTopicResultJsonDeserWithInvalidRequestBody() {
        final String project = "test_project";
        DefaultResponse response = new DefaultResponse();
        ListTopicRequest request = new ListTopicRequest(project);
        String malformedBody = "{";
        response.setBody(malformedBody.getBytes());
        ListTopicResultJsonDeser.getInstance().deserialize(request, response);
    }

    @Test(expectedExceptions = DatahubServiceException.class)
    public void testListShardResultJsonDeserWithInvalidRequestBody() {
        final String project = "test_project";
        final String topic = "test_topic";
        DefaultResponse response = new DefaultResponse();
        ListShardRequest request = new ListShardRequest(project, topic);
        String malformedBody = "{";
        response.setBody(malformedBody.getBytes());
        ListShardResultJsonDeser.getInstance().deserialize(request, response);
    }

    @Test(expectedExceptions = DatahubServiceException.class)
    public void testPutRecordsResultJsonDeserWithInvalidRequestBody() {
        final String project = "test_project";
        final String topic = "test_topic";
        DefaultResponse response = new DefaultResponse();
        PutRecordsRequest request = new PutRecordsRequest(new ArrayList<RecordEntry>());
        String malformedBody = "{";
        response.setBody(malformedBody.getBytes());
        PutRecordsResultJsonDeser.getInstance().deserialize(request, response);
    }
}
