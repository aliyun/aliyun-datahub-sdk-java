package com.aliyun.datahub.client.example;

import com.aliyun.datahub.client.model.*;

import java.util.ArrayList;
import java.util.List;

public class ConnectorExample extends BaseExample {
    @Override
    public void runExample() {
        runWithOdpsType();

        runWithAdsType();

        // Other types are similar
    }

    private void runWithOdpsType() {
        // create sink maxcompute
        SinkOdpsConfig config = new SinkOdpsConfig() {{
            setEndpoint(ODPS_ENDPOINT);
            setTunnelEndpoint(ODPS_TUNNEL_ENDPOINT);
            setProject(ODPS_PROJECT);
            setTable(ODPS_TABLE);
            setAccessId(TEST_AK);
            setAccessKey(TEST_SK);

            // partition mode
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

        final List<String> columnFields = new ArrayList<String>() {{
            add("field1");
            add("field2");
        }};

        // create connector
        CreateConnectorResult createConnectorResult = client.createConnector(TEST_PROJECT, TEST_TOPIC_TUPLE, ConnectorType.SINK_ODPS, columnFields, config);


        // get connector
        GetConnectorResult getDataConnectorResult = client.getConnector(TEST_PROJECT, TEST_TOPIC_TUPLE, ConnectorType.SINK_ODPS);

        // get connector shard task
        GetConnectorShardStatusResult getDataConnectorShardStatusResult =
                client.getConnectorShardStatus(TEST_PROJECT, TEST_TOPIC_TUPLE, ConnectorType.SINK_ODPS);

        // get connector done time, only for odps
        GetConnectorDoneTimeResult getDataConnectorDoneTimeResult =
                client.getConnectorDoneTime(TEST_PROJECT, TEST_TOPIC_TUPLE, ConnectorType.SINK_ODPS);


        // append column field to connector
        client.appendConnectorField(TEST_PROJECT, TEST_TOPIC_TUPLE, ConnectorType.SINK_ODPS, "field3");

        // reload connector
        client.reloadConnector(TEST_PROJECT, TEST_TOPIC_TUPLE, ConnectorType.SINK_ODPS);

        // delete connector
        client.deleteConnector(TEST_PROJECT, TEST_TOPIC_TUPLE, ConnectorType.SINK_ODPS);
    }

    private void runWithAdsType() {
        SinkConfig config = new SinkMysqlConfig() {{
            setHost(ADS_HOST);
            setPort(ADS_PORT);
            setUser(ADS_USER);
            setPassword(ADS_PASSWORD);
            setDatabase(ADS_DATABASE);
            setTable(ADS_TABLE);
        }};

        final List<String> columnFields = new ArrayList<String>() {{
            add("field1");
            add("field2");
        }};

        // create connector
        client.createConnector(TEST_PROJECT, TEST_TOPIC_TUPLE, ConnectorType.SINK_ADS, 1000, columnFields, config);

        // get connector
        GetConnectorResult getDataConnectorResult = client.getConnector(TEST_PROJECT, TEST_TOPIC_TUPLE, ConnectorType.SINK_ADS);

        // get connector shard status
        GetConnectorShardStatusResult getConnectorShardStatusResult = client.getConnectorShardStatus(TEST_PROJECT, TEST_TOPIC_TUPLE, ConnectorType.SINK_ADS);

        // stop connecator
        UpdateConnectorStateResult updateConnectorStateResult = client.updateConnectorState(TEST_PROJECT, TEST_TOPIC_TUPLE, ConnectorType.SINK_ADS, ConnectorState.STOPPED);

        // update connector offset
        UpdateConnectorOffsetResult updateConnectorOffsetResult = client.updateConnectorOffset(TEST_PROJECT, TEST_TOPIC_TUPLE, ConnectorType.SINK_ADS, null,
                new ConnectorOffset(){{ setTimestamp(10); setSequence(100);}});

        // get connector shard status
        getConnectorShardStatusResult = client.getConnectorShardStatus(TEST_PROJECT, TEST_TOPIC_TUPLE, ConnectorType.SINK_ADS);


        // start connector
        updateConnectorStateResult = client.updateConnectorState(TEST_PROJECT, TEST_TOPIC_TUPLE, ConnectorType.SINK_ADS, ConnectorState.RUNNING);

        // delete connector
        client.deleteConnector(TEST_PROJECT, TEST_TOPIC_TUPLE, ConnectorType.SINK_ADS);
    }
}
