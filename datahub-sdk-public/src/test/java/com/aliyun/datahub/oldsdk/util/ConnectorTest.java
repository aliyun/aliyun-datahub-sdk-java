package com.aliyun.datahub.oldsdk.util;

import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.DatahubConfiguration;
import com.aliyun.datahub.common.transport.DefaultRequest;
import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.exception.ResourceNotFoundException;
import com.aliyun.datahub.model.ConnectorType;
import com.aliyun.datahub.model.GetDataConnectorRequest;
import com.aliyun.datahub.model.GetSubscriptionRequest;
import com.aliyun.datahub.model.OffsetContext;
import com.aliyun.datahub.model.serialize.JsonSerializerFactory;
import com.aliyun.datahub.rest.RestClient;

public class ConnectorTest {
    protected ConnectorType connectorType = null;
    protected DatahubClient client = null;

    private RestClient restClient = null;

    public ConnectorTest() {}

    protected void SetUp() {
        DatahubConfiguration conf = DatahubTestUtils.getConf();
        restClient = conf.newRestClient();
        client = new DatahubClient(conf);
    }

    protected boolean IsSubscriptionExist(String project, String topic, String subId) {
        try {
            GetSubscriptionRequest request = new GetSubscriptionRequest(project, topic, subId);
            DefaultRequest req = JsonSerializerFactory.getInstance().getGetSubscriptionRequestSer().serialize(request);
            Response response = restClient.requestWithNoRetry(req);
            JsonSerializerFactory.getInstance().getGetSubscriptionResultDeser().deserialize(request, response);
        } catch (ResourceNotFoundException e) {
            return false;
        }
        return true;
    }

    protected ConnectorExtInfoJsonDeser.ConnectorExtInfoResult GetConnectorExtInfo(String project, String topic) {
        GetDataConnectorRequest request = new GetDataConnectorRequest(project, topic, connectorType);
        DefaultRequest req = JsonSerializerFactory.getInstance().getGetDataConnectorRequestSer().serialize(request);
        Response response = restClient.requestWithNoRetry(req);
        return ConnectorExtInfoJsonDeser.getInstance().deserialize(request, response);
    }

    protected long GetCurSequenceBySubscriptionId(String project, String topic, String subId, String shardId) {
        OffsetContext.Offset offset = new OffsetContext.Offset(0, 0);
        OffsetContext offsetCtx = new OffsetContext(project, topic, subId, shardId,
                offset, 0, "0");
        client.updateOffsetContext(offsetCtx);
        return offsetCtx.getOffset().getSequence();
    }

    protected OffsetContext.Offset GetOffset(String project, String topic, String subId, String shardId) {
        OffsetContext.Offset offset = new OffsetContext.Offset(0, 0);
        OffsetContext offsetCtx = new OffsetContext(project, topic, subId, shardId,
                offset, 0, "0");
        client.updateOffsetContext(offsetCtx);
        return offsetCtx.getOffset();
    }
}
