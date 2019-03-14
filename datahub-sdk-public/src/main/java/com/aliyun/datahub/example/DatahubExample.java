package com.aliyun.datahub.example;

import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.DatahubConfiguration;
import com.aliyun.datahub.auth.AliyunAccount;
import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.common.data.FieldType;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.common.data.RecordType;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.exception.InvalidCursorException;
import com.aliyun.datahub.model.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DatahubExample {
    private String accessId = "";
    private String accessKey = "=";
    private String endpoint = "http://";
    private String projectName = "test_project";
    private String topicName = "topic_test_example";
    private RecordSchema schema = null;
    private DatahubConfiguration conf;
    private DatahubClient client;

    public DatahubExample() {
        this.conf = new DatahubConfiguration(new AliyunAccount(accessId, accessKey), endpoint);
        this.client = new DatahubClient(conf);
    }

    public void init() {
        schema = new RecordSchema();
        schema.addField(new Field("f1", FieldType.STRING));
        client.createTopic(projectName, topicName, 3, 3, RecordType.TUPLE, schema, "topic");
        GetTopicResult topic = client.getTopic(projectName, topicName);
        schema = topic.getRecordSchema();
    }

    public void putRecords() {
        ListShardResult shards = client.listShard(projectName, topicName);

        List<RecordEntry> recordEntries = new ArrayList<RecordEntry>();

        int recordNum = 10;

        for (int n = 0; n < recordNum; n++) {
            //RecordData
            RecordEntry entry = new RecordEntry(schema);

            for (int i = 0; i < entry.getFieldCount(); i++) {
                entry.setString(i, "test");
            }

            //RecordShardId
            String shardId = shards.getShards().get(0).getShardId();

            entry.setShardId(shardId);

            recordEntries.add(entry);
        }
        PutRecordsResult result = client.putRecords(projectName, topicName, recordEntries);

        //fail handle


        //handle failed records

    }

    public void getRecords() {
        ListShardResult shards = client.listShard(projectName, topicName);
        String shardId = shards.getShards().get(0).getShardId();

        GetCursorResult cursorRs = client.getCursor(projectName, topicName, shardId, System.currentTimeMillis() - 24 * 3600 * 1000 /* ms */);
        String cursor = cursorRs.getCursor();

        int limit = 10;
        while (true) {
            try {
                GetRecordsResult recordRs = client.getRecords(projectName, topicName, shardId, cursor, limit, schema);

                List<RecordEntry> recordEntries = recordRs.getRecords();

                if (cursor.equals(recordRs.getNextCursor())) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                cursor = recordRs.getNextCursor();
            } catch (InvalidCursorException ex) {
                cursorRs = client.getCursor(projectName, topicName, shardId, GetCursorRequest.CursorType.OLDEST);
                cursor = cursorRs.getCursor();
            }
        }
    }
    public void createOdpsDataConnector () {
        // Create SinkOdps DataConnector
        // ODPS相关配置设置
        String odpsProject = "datahub_test";
        String odpsTable = "test_table";
        String odpsEndpoint = "http://service-all.ext.odps.aliyun-inc.com/api";
        String tunnelEndpoint = "http://dt-all.ext.odps.aliyun-inc.com";
        OdpsDesc odpsDesc = new OdpsDesc();
        odpsDesc.setProject(odpsProject);
        odpsDesc.setTable(odpsTable);
        odpsDesc.setOdpsEndpoint(odpsEndpoint);
        odpsDesc.setTunnelEndpoint(tunnelEndpoint);
        odpsDesc.setAccessId(accessId);
        odpsDesc.setAccessKey(accessKey);
        odpsDesc.setPartitionMode(OdpsDesc.PartitionMode.USER_DEFINE);

        // 顺序选中topic中部分列或全部列 同步到odps，未选中的列将不会同步
        List<String> columnFields = new ArrayList<String>();
        columnFields.add("f1");
        // 默认是使用UserDefine 的分区模式，具体参见文档[https://help.aliyun.com/document_detail/47453.html?spm=5176.product53345.6.555.MpixiB]
        // 如果需要使用SYSTEM_TIME或EVENT_TIME模式，需要如下设置
        // begin
        int timeRange = 15;  // 分钟，分区时间间隔，最小15分钟
        odpsDesc.setPartitionMode(OdpsDesc.PartitionMode.SYSTEM_TIME);
        odpsDesc.setTimeRange(timeRange);
        Map<String, String> partitionConfig = new HashMap<String, String>();
        //目前仅支持 %Y%m%d%H%M 的组合，任意多级分区
        partitionConfig.put("pt", "%Y%m%d");
        partitionConfig.put("ct", "%H%M");
        odpsDesc.setPartitionConfig(partitionConfig);
        // end

        client.createDataConnector(projectName, topicName, ConnectorType.SINK_ODPS, columnFields, odpsDesc);

        // 特殊需求下可以间歇性 如每15分钟获取Connector状态查看是否有异常,遍历所有shard
        String shard = "0";
        GetDataConnectorShardStatusResult getDataConnectorShardStatusResult =
            client.getDataConnectorShardStatus(projectName, topicName, ConnectorType.SINK_ODPS, shard);
        System.out.println(getDataConnectorShardStatusResult.getCurSequence());
        System.out.println(getDataConnectorShardStatusResult.getLastErrorMessage());
    }

    public void createADSDataConnector () {
        // Create SinkADS/SinkMysql DataConnector
        // ODPS相关配置设置
        String dbHost = "127.0.0.1";
        int dbPort = 3306;
        String dbName = "db";
        String user = "123";
        String password = "123";
        String tableName = "table";
        DatabaseDesc desc = new DatabaseDesc();
        desc.setHost(dbHost);
        desc.setPort(dbPort);
        desc.setDatabase(dbName);
        desc.setUser(user);
        desc.setPassword(password);
        desc.setTable(tableName);
        // batchCommit大小，单位KB
        desc.setMaxCommitSize(512L);
        // 是否忽略错误 采用insert ignore
        desc.setIgnore(true);
        // 顺序选中topic中部分列或全部列 同步到ads，未选中的列将不会同步,选中列必须存在于ADS、Mysql中
        List<String> columnFields = new ArrayList<String>();
        columnFields.add("f1");

        client.createDataConnector(projectName, topicName, ConnectorType.SINK_ADS, columnFields, desc);
        // 或 client.createDataConnector(projectName, topicName, ConnectorType.SINK_MYSQL, columnFields, odpsDesc);
        // 特殊需求下可以间歇性 如每15分钟获取Connector状态查看是否有异常,遍历所有shard
        String shard = "0";
        GetDataConnectorShardStatusResult getDataConnectorShardStatusResult =
            client.getDataConnectorShardStatus(projectName, topicName, ConnectorType.SINK_ADS, shard);
        System.out.println(getDataConnectorShardStatusResult.getCurSequence());
        System.out.println(getDataConnectorShardStatusResult.getLastErrorMessage());
    }

    public void createESDataConnector () {
        // Create SinkES DataConnector
        // ODPS相关配置设置
        String dbHost = "127.0.0.1";
        String index = "index";
        String user = "123";
        String password = "123";
        String tableName = "table";
        List<String> ids = new ArrayList<String>();
        ids.add("f1");
        List<String> types = new ArrayList<String>();
        types.add("f1");
        ElasticSearchDesc desc = new ElasticSearchDesc();
        desc.setEndpoint(dbHost);
        desc.setIndex(index);
        desc.setUser(user);
        desc.setPassword(password);
        // es的id和type将会根据datahub中个别字段的值决定，不同值的数据会写入es的不同type中
        desc.setIdFields(ids);
        desc.setTypeFields(types);
        // batchCommit大小，单位KB
        desc.setMaxCommitSize(512L);
        // 是否使用代理模式，若未true将不会扫描es所有node，直接通过代理写入，vpc es必须使用该选项
        desc.setProxyMode(true);
        // 顺序选中topic中部分列或全部列 同步到ads，未选中的列将不会同步,选中列必须存在于ADS、Mysql中
        List<String> columnFields = new ArrayList<String>();
        columnFields.add("f1");

        client.createDataConnector(projectName, topicName, ConnectorType.SINK_ES, columnFields, desc);
        // 特殊需求下可以间歇性 如每15分钟获取Connector状态查看是否有异常,遍历所有shard
        String shard = "0";
        GetDataConnectorShardStatusResult getDataConnectorShardStatusResult =
            client.getDataConnectorShardStatus(projectName, topicName, ConnectorType.SINK_ADS, shard);
        System.out.println(getDataConnectorShardStatusResult.getCurSequence());
        System.out.println(getDataConnectorShardStatusResult.getLastErrorMessage());
    }

    public void appendField() {
        client.appendField(new AppendFieldRequest(projectName, topicName, new Field("fieldName", FieldType.STRING)));
        client.appendDataConnectorField(new AppendDataConnectorFieldRequest(projectName, topicName, ConnectorType.SINK_ODPS, "fieldName"));
    }

    public static void main(String[] args) {
        DatahubExample example = new DatahubExample();
        try {
            example.init();
            example.putRecords();
            example.getRecords();
            example.createADSDataConnector();
        } catch (DatahubClientException e) {
            e.printStackTrace();
        }
    }
}
