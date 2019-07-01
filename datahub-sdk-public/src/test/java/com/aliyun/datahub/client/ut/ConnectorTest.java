package com.aliyun.datahub.client.ut;

import com.aliyun.datahub.client.model.*;
import org.mockserver.model.Header;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

public class ConnectorTest extends MockServer {
    @Test
    public void testCreateOdpsConnector() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                )
        );

        SinkOdpsConfig config = new SinkOdpsConfig() {{
            setEndpoint("OdpsEndpoint");
            setTunnelEndpoint("TunnelEndpoint");
            setProject("test_project");
            setTable("test_table");
            setAccessKey("test_sk");
            setAccessId("test_ak");

            setPartitionMode(PartitionMode.SYSTEM_TIME);
            // partition config
            PartitionConfig partitionConfig = new PartitionConfig() {{
                addConfig("ds", "%Y%m%d");
                addConfig("hh", "%H");
                addConfig("mm", "%M");
            }};
            setPartitionConfig(partitionConfig);
            setTimeRange(15);
        }};

        List<String> columnFields = new ArrayList<String>() {{
            add("field1");
            add("field2");
        }};
        // create connector
        client.createConnector("test_project", "test_topic", ConnectorType.SINK_ODPS,
                columnFields, config);

        mockServerClient.verify(
                request().withMethod("POST").withPath("/projects/test_project/topics/test_topic/connectors/sink_odps")
                        .withBody(exact("{\"Action\":\"Create\",\"Type\":\"SINK_ODPS\",\"SinkStartTime\":-1,\"ColumnFields\":[\"field1\",\"field2\"],\"Config\":{\"Project\":\"test_project\",\"Table\":\"test_table\",\"OdpsEndpoint\":\"OdpsEndpoint\",\"TunnelEndpoint\":\"TunnelEndpoint\",\"AccessId\":\"test_ak\",\"AccessKey\":\"test_sk\",\"PartitionMode\":\"SYSTEM_TIME\",\"TimeRange\":15,\"PartitionConfig\":{\"ds\":\"%Y%m%d\",\"hh\":\"%H\",\"mm\":\"%M\"}}}"))
        );
    }

    @Test
    public void testCreateDatabaseConnector() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                )
        );

        SinkMysqlConfig config = new SinkMysqlConfig() {{
            setHost("test_host");
            setPort(0);
            setDatabase("test_database");
            setTable("test_table");
            setUser("test_user");
            setPassword("test_password");
            setInsertMode(InsertMode.IGNORE);
        }};

        List<String> columnFields = new ArrayList<String>() {{
            add("field1");
            add("field2");
        }};
        // create connector
        client.createConnector("test_project", "test_topic", ConnectorType.SINK_ADS,
                columnFields, config);

        mockServerClient.verify(
                request().withMethod("POST").withPath("/projects/test_project/topics/test_topic/connectors/sink_ads")
                        .withBody(exact("{\"Action\":\"Create\",\"Type\":\"SINK_ADS\",\"SinkStartTime\":-1,\"ColumnFields\":[\"field1\",\"field2\"],\"Config\":{\"Host\":\"test_host\",\"Port\":\"0\",\"Database\":\"test_database\",\"Table\":\"test_table\",\"User\":\"test_user\",\"Password\":\"test_password\",\"Ignore\":\"true\"}}"))
        );
    }

    @Test
    public void testCreateFcConnector() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                )
        );

        SinkFcConfig config = new SinkFcConfig() {{
            setService("test_service");
            setEndpoint("test_endpoint");
            setFunction("test_function");
            setAuthMode(AuthMode.STS);
        }};

        List<String> columnFields = new ArrayList<String>() {{
            add("field1");
            add("field2");
        }};
        // create connector
        client.createConnector("test_project", "test_topic", ConnectorType.SINK_FC,
                columnFields, config);

        mockServerClient.verify(
                request().withMethod("POST").withPath("/projects/test_project/topics/test_topic/connectors/sink_fc")
                        .withBody(exact("{\"Action\":\"Create\",\"Type\":\"SINK_FC\",\"SinkStartTime\":-1,\"ColumnFields\":[\"field1\",\"field2\"],\"Config\":{\"Endpoint\":\"test_endpoint\",\"Service\":\"test_service\",\"Function\":\"test_function\",\"AuthMode\":\"sts\"}}"))
        );
    }

    @Test
    public void testCreateOssConnector() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                )
        );

        SinkOssConfig config = new SinkOssConfig() {{
            setEndpoint("test_endpoint");
            setBucket("test_bucket");
            setPrefix("test_prefix");
            setTimeFormat("%d%M");
            setTimeRange(1);
            setAuthMode(AuthMode.STS);
        }};

        List<String> columnFields = new ArrayList<String>() {{
            add("field1");
            add("field2");
        }};
        // create connector
        client.createConnector("test_project", "test_topic", ConnectorType.SINK_OSS,
                columnFields, config);

        mockServerClient.verify(
                request().withMethod("POST").withPath("/projects/test_project/topics/test_topic/connectors/sink_oss")
                        .withBody(exact("{\"Action\":\"Create\",\"Type\":\"SINK_OSS\",\"SinkStartTime\":-1,\"ColumnFields\":[\"field1\",\"field2\"],\"Config\":{\"Endpoint\":\"test_endpoint\",\"Bucket\":\"test_bucket\",\"Prefix\":\"test_prefix\",\"TimeFormat\":\"%d%M\",\"TimeRange\":1,\"AuthMode\":\"sts\"}}"))
        );
    }

    @Test
    public void testCreateOtsConnector() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                )
        );

        SinkOtsConfig config = new SinkOtsConfig() {{
            setEndpoint("test_endpoint");
            setInstance("test_instance");
            setTable("test_table");
            setAuthMode(AuthMode.AK);
            setAccessId("test_ak");
            setAccessKey("test_sk");
        }};

        List<String> columnFields = new ArrayList<String>() {{
            add("field1");
            add("field2");
        }};
        // create connector
        client.createConnector("test_project", "test_topic", ConnectorType.SINK_OTS,
                columnFields, config);

        mockServerClient.verify(
                request().withMethod("POST").withPath("/projects/test_project/topics/test_topic/connectors/sink_ots")
                        .withBody("{\"Action\":\"Create\",\"Type\":\"SINK_OTS\",\"SinkStartTime\":-1,\"ColumnFields\":[\"field1\",\"field2\"],\"Config\":{\"Endpoint\":\"test_endpoint\",\"InstanceName\":\"test_instance\",\"TableName\":\"test_table\",\"AuthMode\":\"ak\",\"AccessId\":\"test_ak\",\"AccessKey\":\"test_sk\"}}")
        );
    }

    @Test
    public void testCreateEsConnector() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                )
        );

        SinkEsConfig config = new SinkEsConfig() {{
            setEndpoint("test_endpoint");
            setIndex("test_index");
            setUser("test_user");
            setPassword("test_password");
            setIdFields(new ArrayList<String>() {{ add("id1"); add("id2");}});
            setTypeFields(new ArrayList<String>() {{ add("type1"); add("type2");}});
            setProxyMode(true);
        }};

        List<String> columnFields = new ArrayList<String>() {{
            add("field1");
            add("field2");
        }};
        // create connector
        client.createConnector("test_project", "test_topic", ConnectorType.SINK_ES,
                columnFields, config);

        mockServerClient.verify(
                request().withMethod("POST").withPath("/projects/test_project/topics/test_topic/connectors/sink_es")
                        .withBody(exact("{\"Action\":\"Create\",\"Type\":\"SINK_ES\",\"SinkStartTime\":-1,\"ColumnFields\":[\"field1\",\"field2\"],\"Config\":{\"Index\":\"test_index\",\"Endpoint\":\"test_endpoint\",\"User\":\"test_user\",\"Password\":\"test_password\",\"IDFields\":[\"id1\",\"id2\"],\"TypeFields\":[\"type1\",\"type2\"],\"ProxyMode\":\"true\"}}"))
        );
    }

    @Test
    public void testGetDataConnector() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                ).withBody("{\"ColumnFields\":[\"field1\",\"field2\"],\"Config\":{\"OdpsEndpoint\":\"OdpsEndpoint\",\"OffsetStoreType\":\"coordinator\",\"PartitionConfig\":\"[{\\\"key\\\":\\\"ds\\\",\\\"value\\\":\\\"%Y%m%d\\\"},{\\\"key\\\":\\\"hh\\\",\\\"value\\\":\\\"%H\\\"},{\\\"key\\\":\\\"mm\\\",\\\"value\\\":\\\"%M\\\"}]\",\"PartitionMode\":\"SYSTEM_TIME\",\"Project\":\"test_project\",\"Table\":\"test_project\",\"TableCreateTime\":\"1525685912\",\"TimeRange\":\"15\",\"TunnelEndpoint\":\"TunnelEndpoint\"},\"Creator\":\"\",\"ExtraInfo\":{\"SubscriptionId\":\"1525831659109IhQ7a\"},\"Owner\":\"\",\"ShardContexts\":[{\"CurrentSequence\":0,\"EndSequence\":9223372036854775807,\"ShardId\":\"0\",\"StartSequence\":0},{\"CurrentSequence\":0,\"EndSequence\":9223372036854775807,\"ShardId\":\"1\",\"StartSequence\":0},{\"CurrentSequence\":0,\"EndSequence\":9223372036854775807,\"ShardId\":\"2\",\"StartSequence\":0},{\"CurrentSequence\":0,\"EndSequence\":9223372036854775807,\"ShardId\":\"3\",\"StartSequence\":0}],\"State\":\"CONNECTOR_RUNNING\",\"Type\":\"sink_odps\"}")
        );

        GetConnectorResult getDataConnectorResult = client.getConnector("test_project", "test_topic", ConnectorType.SINK_ODPS);
        Assert.assertEquals(ConnectorType.SINK_ODPS, getDataConnectorResult.getType());
        Assert.assertEquals(ConnectorState.RUNNING, getDataConnectorResult.getState());
        Assert.assertEquals(2, getDataConnectorResult.getColumnFields().size());
        SinkOdpsConfig config = (SinkOdpsConfig)getDataConnectorResult.getConfig();
        Assert.assertEquals("OdpsEndpoint", config.getEndpoint());
        Assert.assertEquals("TunnelEndpoint", config.getTunnelEndpoint());
        Assert.assertEquals(15, config.getTimeRange());
        Assert.assertEquals(SinkOdpsConfig.PartitionMode.SYSTEM_TIME, config.getPartitionMode());
        SinkOdpsConfig.PartitionConfig partitionConfig = config.getPartitionConfig();
        Assert.assertEquals("%Y%m%d", partitionConfig.getConfigMap().get("ds"));
        Assert.assertEquals("%H", partitionConfig.getConfigMap().get("hh"));
        Assert.assertEquals("%M", partitionConfig.getConfigMap().get("mm"));

        mockServerClient.verify(
                request().withMethod("GET").withPath("/projects/test_project/topics/test_topic/connectors/sink_odps")
        );
    }

    @Test
    public void testUpdateConnector() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                )
        );

        SinkOdpsConfig config = new SinkOdpsConfig() {{
            setEndpoint("OdpsEndpoint");
            setTunnelEndpoint("TunnelEndpoint");
            setProject("test_project");
            setTable("test_table");
            setAccessKey("test_sk");
            setAccessId("test_ak");

            setPartitionMode(PartitionMode.SYSTEM_TIME);
            // partition config
            PartitionConfig partitionConfig = new PartitionConfig() {{
                addConfig("ds", "%Y%m%d");
                addConfig("hh", "%H");
                addConfig("mm", "%M");
            }};
            setPartitionConfig(partitionConfig);
            setTimeRange(15);
        }};

        UpdateConnectorResult updateConnectorResult = client.updateConnector("test_project", "test_topic", ConnectorType.SINK_ODPS, config);
        mockServerClient.verify(
                request().withMethod("POST").withPath("/projects/test_project/topics/test_topic/connectors/sink_odps")
                        .withBody(exact("{\"Action\":\"updateconfig\",\"Config\":{\"Project\":\"test_project\",\"Table\":\"test_table\",\"OdpsEndpoint\":\"OdpsEndpoint\",\"TunnelEndpoint\":\"TunnelEndpoint\",\"AccessId\":\"test_ak\",\"AccessKey\":\"test_sk\",\"PartitionMode\":\"SYSTEM_TIME\",\"TimeRange\":15,\"PartitionConfig\":{\"ds\":\"%Y%m%d\",\"hh\":\"%H\",\"mm\":\"%M\"}}}"))
        );
    }

    @Test
    public void testListDataConnector() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                ).withBody("{\"Connectors\": [\"sink_odps\", \"sink_oss\"]}")
        );
        ListConnectorResult listDataConnectorResult = client.listConnector("test_project", "test_topic");
        Assert.assertEquals(2, listDataConnectorResult.getConnectorNames().size());
        Assert.assertTrue(listDataConnectorResult.getConnectorNames().contains("sink_oss"));

        mockServerClient.verify(
                request().withMethod("GET").withPath("/projects/test_project/topics/test_topic/connectors")
        );
    }

    @Test
    public void testGetDataConnectorDoneTime() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                ).withBody("{\"DoneTime\": 10}")
        );

        GetConnectorDoneTimeResult getDataConnectorDoneTimeResult = client.getConnectorDoneTime("test_project", "test_topic", ConnectorType.SINK_ODPS);
        Assert.assertEquals(new Long(10), getDataConnectorDoneTimeResult.getDoneTime());

        mockServerClient.verify(
                request().withMethod("GET").withPath("/projects/test_project/topics/test_topic/connectors/sink_odps")
                        .withQueryStringParameter("doneTime", "")
        );
    }

    @Test
    public void testReloadDataConnector() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                )
        );

        client.reloadConnector("test_project", "test_topic", ConnectorType.SINK_ODPS);
        mockServerClient.verify(
                request().withMethod("POST").withPath("/projects/test_project/topics/test_topic/connectors/sink_odps")
                        .withBody("{\"Action\":\"Reload\"}")
        );
    }

    @Test
    public void testGetDataConnectorShardStatus() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                ).withBody("{\"ShardStatusInfos\":{\"0\":{\"CurrentSequence\":10,\"CurrentTimestamp\":-1,\"DiscardCount\":0,\"LastErrorMessage\":\"error_message\",\"ShardId\":\"0\",\"State\":\"CONTEXT_PLANNED\",\"UpdateTime\":1542940638},\"1\":{\"CurrentSequence\":11,\"CurrentTimestamp\":-1,\"DiscardCount\":0,\"LastErrorMessage\":\"\",\"ShardId\":\"1\",\"State\":\"CONTEXT_PLANNED\",\"UpdateTime\":1542940638}}}")
        );

        GetConnectorShardStatusResult getDataConnectorShardStatusResult = client.getConnectorShardStatus("test_project", "test_topic", ConnectorType.SINK_ODPS);
        Assert.assertEquals(2, getDataConnectorShardStatusResult.getStatusEntryMap().size());
        Assert.assertEquals(10, getDataConnectorShardStatusResult.getStatusEntryMap().get("0").getCurrSequence());
        Assert.assertEquals("error_message", getDataConnectorShardStatusResult.getStatusEntryMap().get("0").getLastErrorMessage());
        Assert.assertEquals(11, getDataConnectorShardStatusResult.getStatusEntryMap().get("1").getCurrSequence());

        mockServerClient.verify(
                request().withMethod("POST").withPath("/projects/test_project/topics/test_topic/connectors/sink_odps")
                        .withBody("{\"Action\":\"Status\"}")
        );
    }

    @Test
    public void testAppendDataConnectorField() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                )
        );

        client.appendConnectorField("test_project", "test_topic", ConnectorType.SINK_ODPS, "field3");
        mockServerClient.verify(
                request().withMethod("POST").withPath("/projects/test_project/topics/test_topic/connectors/sink_odps")
                        .withBody("{\"Action\":\"appendfield\",\"FieldName\":\"field3\"}")
        );
    }

    @Test
    public void testDeleteDataConnector() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                )
        );

        client.deleteConnector("test_project", "test_topic", ConnectorType.SINK_ODPS);
        mockServerClient.verify(
                request().withMethod("DELETE").withPath("/projects/test_project/topics/test_topic/connectors/sink_odps")
        );
    }

    @Test
    public void testUpdateConnectorState() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                )
        );

        client.updateConnectorState("test_project", "test_topic", ConnectorType.SINK_ODPS, ConnectorState.STOPPED);
        mockServerClient.verify(
                request().withMethod("POST")
                        .withPath("/projects/test_project/topics/test_topic/connectors/sink_odps")
                        .withBody( "{\"Action\":\"updatestate\",\"State\":\"CONNECTOR_PAUSED\"}")
        );
    }

    @Test
    public void testUpdateConnectorOffset() {
        mockServerClient.when(request()).respond(
                response().withStatusCode(200).withHeaders(
                        new Header("x-datahub-request-id", "testId"),
                        new Header("Content-Type", "application/json")
                )
        );

        ConnectorOffset offset = new ConnectorOffset();
        offset.setTimestamp(100);
        offset.setSequence(10);

        client.updateConnectorOffset("test_project", "test_topic", ConnectorType.SINK_ODPS, "0", offset);
        mockServerClient.verify(
                request().withMethod("POST")
                        .withPath("/projects/test_project/topics/test_topic/connectors/sink_odps")
                        .withBody("{\"Action\":\"updateshardcontext\",\"ShardId\":\"0\",\"CurrentTime\":100,\"CurrentSequence\":10}")
        );
    }
}
