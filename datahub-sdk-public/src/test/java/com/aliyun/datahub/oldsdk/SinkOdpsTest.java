package com.aliyun.datahub.oldsdk;

import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.common.data.FieldType;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.common.data.RecordType;
import com.aliyun.datahub.exception.*;
import com.aliyun.datahub.model.*;
import com.aliyun.datahub.oldsdk.util.ConnectorExtInfoJsonDeser;
import com.aliyun.datahub.oldsdk.util.ConnectorTest;
import com.aliyun.datahub.oldsdk.util.DatahubTestUtils;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.*;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;

public class SinkOdpsTest extends ConnectorTest {
  private String projectName = null;
  private String topicName = null;
  private OdpsDesc odpsDesc = null;
  private int shardCount = 3;
  private int lifeCycle = 7;
  private long recordCount = 20;
  private long timestamp = System.currentTimeMillis() * 1000;
  private long shipperWaitTime = 1000*60;

  //Odps
  private Odps odps = null;
  private TableTunnel tunnel = null;
  private String odpsProject = DatahubTestUtils.getOdpsProjectName();
  private String odpsTable = null;
  private String odpsEndpoint = DatahubTestUtils.getOdpsEndpoint();
  private String tunnelEndpoint = DatahubTestUtils.getOdpsTunnelEndpoint();
  private String accessId = DatahubTestUtils.getSecondAccessId();
  private String accessKey = DatahubTestUtils.getSecondAccesskey();

  @BeforeMethod
  public void SetUp() {
    try {
      super.SetUp();

      Account account = new AliyunAccount(DatahubTestUtils.getSecondAccessId(), DatahubTestUtils.getSecondAccesskey());
      odps = new Odps(account);
      odps.setDefaultProject(odpsProject);
      odps.setEndpoint(odpsEndpoint);
      tunnel = new TableTunnel(odps);
      if (tunnelEndpoint != null && !tunnelEndpoint.isEmpty()) {
        tunnel.setEndpoint(tunnelEndpoint);
      }

      Random random = new Random(System.currentTimeMillis());
      String hint = Long.toHexString(random.nextLong());

      System.out.println("Hint is " + hint);
      System.out.println("Timestamp is " + timestamp);
      // setUp for odps
      odpsTable = String.format("ut_test_table_%s", hint);
      System.out.println("OdpsTable Name is " + odpsTable);

      projectName = DatahubTestUtils.getProjectName();
      topicName = String.format("ut_test_topic_%s", hint);
      connectorType = ConnectorType.SINK_ODPS;
      System.out.println("Topic Name is " + topicName);

      odpsDesc = new OdpsDesc();
      odpsDesc.setProject(odpsProject);
      odpsDesc.setTable(odpsTable);
      odpsDesc.setOdpsEndpoint(odpsEndpoint);
      odpsDesc.setTunnelEndpoint(tunnelEndpoint);
      odpsDesc.setAccessId(accessId);
      odpsDesc.setAccessKey(accessKey);
    } catch (Exception e) {

    }
  }

  @AfterMethod
  public void TearDown() {
      for (String listTopicName : client.listTopic(projectName).getTopics()) {
        ListDataConnectorResult listConnectorResult = client.listDataConnector(projectName, listTopicName);
        for (String connector : listConnectorResult.getDataConnectors()) {
          client.deleteDataConnector(projectName, listTopicName, ConnectorType.valueOf(connector.toUpperCase()));
        }
        client.deleteTopic(projectName, listTopicName);
      }

      try {
        odps.tables().delete(odpsProject, odpsTable);
      } catch (OdpsException e) {
        //
      }
  }

  private void CreateTopic() {
    RecordType type = RecordType.BLOB;
    client.createTopic(projectName, topicName, shardCount, lifeCycle, type, "");
    client.waitForShardReady(projectName, topicName);
  }

  private void CreateTopic(RecordSchema schema) {
    RecordType type = RecordType.TUPLE;
    client.createTopic(projectName, topicName, shardCount, lifeCycle, type, schema, "");
    client.waitForShardReady(projectName, topicName);
  }

  private void CreateTable(TableSchema odpsSchema) {
    try {
      odps.tables().create(odpsProject, odpsTable, odpsSchema);
    } catch (OdpsException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }
  }

  private void DropTable() {
    try {
      odps.tables().delete(odpsProject, odpsTable);
    } catch (OdpsException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }
  }

  private void downloadCheckData(long recordCount, TableSchema odpsSchema, PartitionSpec partition) throws TunnelException, IOException {
    TableTunnel.DownloadSession down = null;
    if (odpsSchema.getPartitionColumns().isEmpty()) {
      down = tunnel.createDownloadSession(odpsProject, odpsTable);
    } else {
      down = tunnel.createDownloadSession(odpsProject, odpsTable, partition);
    }
    Assert.assertNotNull(down);

    TableSchema schema = down.getSchema();
    Assert.assertNotNull(schema);

    long count = down.getRecordCount();
    Assert.assertEquals(recordCount, count);

    RecordReader reader = down.openRecordReader(0, recordCount);
    Assert.assertNotNull(reader);

    long i = 0;
    Record r;
    for (; (r = reader.read()) != null; ++i) {
      checkData(r, schema);
    }
    reader.close();

    org.junit.Assert.assertEquals(recordCount, i);
  }

  private void downloadCheckData(long recordCount, TableSchema odpsSchema) throws TunnelException, IOException {
    if (odpsSchema.getPartitionColumns().isEmpty()) {
      downloadCheckData(recordCount, odpsSchema, null);
    } else {
      PartitionSpec partitionSpec = new PartitionSpec();
      for (Column i : odpsSchema.getPartitionColumns()) {
        partitionSpec.set(i.getName(), String.valueOf(timestamp));
      }
      downloadCheckData(recordCount, odpsSchema, partitionSpec);
    }
  }

  private void checkData(Record r, TableSchema schema) {
    for (Column i : schema.getColumns()) {
      switch (i.getType()) {
        case BIGINT:
          Assert.assertEquals(r.getBigint(i.getName()), Long.valueOf(timestamp));
          break;
        case BOOLEAN:
          Assert.assertEquals(r.getBoolean(i.getName()), Boolean.valueOf(false));
          break;
        case STRING:
          Assert.assertEquals(r.getString(i.getName()), String.valueOf(timestamp));
          break;
        case DATETIME:
          Assert.assertEquals(r.getDatetime(i.getName()), new Date(timestamp/1000));
          break;
        case DOUBLE:
          Assert.assertEquals(r.getDouble(i.getName()), Double.valueOf(timestamp));
          break;
        case DECIMAL:
          BigDecimal bd = BigDecimal.valueOf(Math.sqrt(timestamp));
          if (bd.scale() > 18) {
            bd = bd.setScale(18, BigDecimal.ROUND_HALF_UP);
          }
          Assert.assertEquals(r.getDecimal(i.getName()), bd);
          break;
        default:
          Assert.assertTrue(false);
      }
    }
  }

  private RecordEntry genDataWithEventTime(RecordSchema schema, long eventTime) {
    RecordEntry entry = new RecordEntry(schema);
    for (Field i : schema.getFields()) {
      if (i.getName().equals("event_time")) {
        entry.setTimeStamp("event_time", eventTime);
        continue;
      }
      switch (i.getType()) {
        case STRING:
          entry.setString(i.getName(), String.valueOf(timestamp));
          break;
        case BOOLEAN:
          entry.setBoolean(i.getName(), Boolean.valueOf(false));
          break;
        case DOUBLE:
          entry.setDouble(i.getName(), Double.valueOf(timestamp));
          break;
        case TIMESTAMP:
          entry.setTimeStamp(i.getName(), Long.valueOf(timestamp));
          break;
        case BIGINT:
          entry.setBigint(i.getName(), Long.valueOf(timestamp));
          break;
        case DECIMAL:
          entry.setDecimal(i.getName(), BigDecimal.valueOf(Math.sqrt(timestamp)));
          break;
        default:
          Assert.assertTrue(false);
      }
    }
    return entry;
  }

  private RecordEntry genData(RecordSchema schema) {
    RecordEntry entry = new RecordEntry(schema);
    for (Field i : schema.getFields()) {
      switch (i.getType()) {
        case STRING:
          entry.setString(i.getName(), String.valueOf(timestamp));
          break;
        case BOOLEAN:
          entry.setBoolean(i.getName(), Boolean.valueOf(false));
          break;
        case DOUBLE:
          entry.setDouble(i.getName(), Double.valueOf(timestamp));
          break;
        case TIMESTAMP:
          entry.setTimeStamp(i.getName(), Long.valueOf(timestamp));
          break;
        case BIGINT:
          entry.setBigint(i.getName(), Long.valueOf(timestamp));
          break;
        case DECIMAL:
          entry.setDecimal(i.getName(), BigDecimal.valueOf(Math.sqrt(timestamp)));
          break;
        default:
          Assert.assertTrue(false);
      }
    }
    return entry;
  }

  private void putRecordsWithEventTime(RecordSchema schema, long eventTime) {
    ListShardResult shards = client.listShard(projectName, topicName);
    for (ShardEntry shard : shards.getShards()) {
      if (shard.getState() == ShardState.CLOSED) {
        continue;
      }
      List<RecordEntry> recordEntries = new ArrayList<RecordEntry>();
      for (long n = 0; n < recordCount; n++) {
        //RecordData
        RecordEntry entry = genDataWithEventTime(schema, eventTime);

        entry.setShardId(shard.getShardId());

        recordEntries.add(entry);
      }
      PutRecordsResult result = client.putRecords(projectName, topicName, recordEntries);
      if (result.getFailedRecordCount() != 0) {
        System.out.println(result.getFailedRecordError().get(0).getMessage());
      }
      Assert.assertEquals(result.getFailedRecordCount(), 0);
    }
  }


  private void putRecords(RecordSchema schema) {
    ListShardResult shards = client.listShard(projectName, topicName);
    for (ShardEntry shard : shards.getShards()) {
      if (shard.getState() == ShardState.CLOSED) {
        continue;
      }
      List<RecordEntry> recordEntries = new ArrayList<RecordEntry>();
      for (long n = 0; n < recordCount; n++) {
        //RecordData
        RecordEntry entry = genData(schema);

        entry.setShardId(shard.getShardId());

        recordEntries.add(entry);
      }
      PutRecordsResult result = client.putRecords(projectName, topicName, recordEntries);
      if (result.getFailedRecordCount() != 0) {
        System.out.println(result.getFailedRecordError().get(0).getMessage());
      }
      Assert.assertEquals(result.getFailedRecordCount(), 0);
    }
  }

  @Test
  public void testSinkOdpsConnectorNormal() {
    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("f2");
    columnFields.add("f3");
    columnFields.add("f4");
    columnFields.add("f5");
    columnFields.add("f6");
    columnFields.add("pt");
    columnFields.add("ct");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("f2", FieldType.BIGINT));
    schema.addField(new Field("f3", FieldType.DOUBLE));
    schema.addField(new Field("f4", FieldType.TIMESTAMP));
    schema.addField(new Field("f5", FieldType.BOOLEAN));
    schema.addField(new Field("f6", FieldType.DECIMAL));
    schema.addField(new Field("pt", FieldType.STRING));
    schema.addField(new Field("ct", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addColumn(new Column("f2", OdpsType.BIGINT));
    odpsSchema.addColumn(new Column("f3", OdpsType.DOUBLE));
    odpsSchema.addColumn(new Column("f4", OdpsType.DATETIME));
    odpsSchema.addColumn(new Column("f5", OdpsType.BOOLEAN));
    odpsSchema.addColumn(new Column("f6", OdpsType.DECIMAL));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("ct", OdpsType.STRING));
    CreateTable(odpsSchema);

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);

    ListDataConnectorResult listDataConnectorResult = client.listDataConnector(projectName, topicName);
    List<String> connectors = listDataConnectorResult.getDataConnectors();
    Assert.assertTrue(connectors.contains(connectorType.toString().toLowerCase()));

    GetDataConnectorResult getDataConnectorResult = client.getDataConnector(projectName, topicName, connectorType);
    Assert.assertEquals(getDataConnectorResult.getColumnFields(), columnFields);

    ConnectorExtInfoJsonDeser.ConnectorExtInfoResult detailRs = GetConnectorExtInfo(projectName, topicName);
    Assert.assertEquals(detailRs.getOffsetStoreType(), "coordinator");
    Assert.assertFalse(detailRs.getSubscriptionId().isEmpty());

    putRecords(schema);
    try {
      Thread.sleep(shipperWaitTime);
    } catch (InterruptedException e) {

    }
    // check odps data
    try {
      downloadCheckData(recordCount * shardCount, odpsSchema);
    } catch (IOException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    } catch (TunnelException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }

    getDataConnectorResult = client.getDataConnector(projectName, topicName, connectorType);
    Assert.assertEquals(getDataConnectorResult.getColumnFields(), columnFields);
    for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
      Assert.assertEquals(sc.getCurSequence(), recordCount);
      Assert.assertEquals(GetCurSequenceBySubscriptionId(projectName, topicName, detailRs.getSubscriptionId(),
              sc.getShardId()), sc.getCurSequence());
    }

    Assert.assertTrue(IsSubscriptionExist(projectName, topicName, detailRs.getSubscriptionId()));
    client.deleteDataConnector(projectName, topicName, connectorType);
    Assert.assertFalse(IsSubscriptionExist(projectName, topicName, detailRs.getSubscriptionId()));

    listDataConnectorResult = client.listDataConnector(projectName, topicName);
    connectors = listDataConnectorResult.getDataConnectors();
    Assert.assertFalse(connectors.contains(connectorType.toString().toLowerCase()));
  }

  @Test
  public void testNotTopicOwnerOrCreator() {
    String projectName = "test_sub_project";
    DatahubClient client = new DatahubClient(DatahubTestUtils.getConf());
    try {
      try {
        client.createProject(projectName, "test_sub_project");
      } catch (DatahubServiceException e) {

      }

      List<String> columnFields = new ArrayList<String>();
      columnFields.add("f1");

      RecordSchema schema = new RecordSchema();
      schema.addField(new Field("f1", FieldType.STRING));

      RecordType type = RecordType.TUPLE;
      client.createTopic(projectName, topicName, shardCount, lifeCycle, type, schema, "");
      client.waitForShardReady(projectName, topicName);

      TableSchema odpsSchema = new TableSchema();
      odpsSchema.addColumn(new Column("f1", OdpsType.STRING));

      CreateTable(odpsSchema);
      DatahubClient subClient = new DatahubClient(DatahubTestUtils.getSecondSubConf());
      subClient.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
    } catch (DatahubServiceException e) {
      e.printStackTrace();
    }
    finally {
      ListTopicResult listTopicResult = client.listTopic(projectName);
      for (String topic : listTopicResult.getTopics()) {
        ListDataConnectorResult listDataConnectorResult = client.listDataConnector(projectName, topic);
        for (String connector : listDataConnectorResult.getDataConnectors()) {
          client.deleteDataConnector(projectName, topic, ConnectorType.valueOf(connector.toUpperCase()));
        }
        client.deleteTopic(projectName,topic);
      }
      client.deleteProject(projectName);
    }
  }

  @Test(expectedExceptions = InvalidParameterException.class)
  public void testPartitionNotSet() {
    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("f2");
    columnFields.add("f3");
    columnFields.add("f4");
    columnFields.add("f5");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("f2", FieldType.BIGINT));
    schema.addField(new Field("f3", FieldType.DOUBLE));
    schema.addField(new Field("f4", FieldType.TIMESTAMP));
    schema.addField(new Field("f5", FieldType.BOOLEAN));
    schema.addField(new Field("pt", FieldType.STRING));
    schema.addField(new Field("ct", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addColumn(new Column("f2", OdpsType.BIGINT));
    odpsSchema.addColumn(new Column("f3", OdpsType.DOUBLE));
    odpsSchema.addColumn(new Column("f4", OdpsType.DATETIME));
    odpsSchema.addColumn(new Column("f5", OdpsType.BOOLEAN));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("ct", OdpsType.STRING));
    CreateTable(odpsSchema);

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
  }

  @Test(expectedExceptions = InvalidParameterException.class)
  public void testPartitionNotMatch() {
    List<String> columnFields = new ArrayList<String>();

    columnFields.add("f1");
    columnFields.add("f2");
    columnFields.add("f3");
    columnFields.add("f4");
    columnFields.add("f5");
    columnFields.add("pt");
    columnFields.add("ct");
    columnFields.add("f5");
    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("f2", FieldType.BIGINT));
    schema.addField(new Field("f3", FieldType.DOUBLE));
    schema.addField(new Field("f4", FieldType.TIMESTAMP));
    schema.addField(new Field("f5", FieldType.BOOLEAN));
    schema.addField(new Field("pt", FieldType.STRING));
    schema.addField(new Field("ct", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addColumn(new Column("f2", OdpsType.BIGINT));
    odpsSchema.addColumn(new Column("f3", OdpsType.DOUBLE));
    odpsSchema.addColumn(new Column("f4", OdpsType.DATETIME));
    odpsSchema.addColumn(new Column("f5", OdpsType.BOOLEAN));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("ct", OdpsType.STRING));
    CreateTable(odpsSchema);

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
  }

  @Test(expectedExceptions = InvalidParameterException.class)
  public void testPartitionNotExist() {
    List<String> columnFields = new ArrayList<String>();

    columnFields.add("f1");
    columnFields.add("f2");
    columnFields.add("f3");
    columnFields.add("f4");
    columnFields.add("f5");
    columnFields.add("f6");
    columnFields.add("ct");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("f2", FieldType.BIGINT));
    schema.addField(new Field("f3", FieldType.DOUBLE));
    schema.addField(new Field("f4", FieldType.TIMESTAMP));
    schema.addField(new Field("f5", FieldType.BOOLEAN));
    schema.addField(new Field("pt", FieldType.STRING));
    schema.addField(new Field("ct", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addColumn(new Column("f2", OdpsType.BIGINT));
    odpsSchema.addColumn(new Column("f3", OdpsType.DOUBLE));
    odpsSchema.addColumn(new Column("f4", OdpsType.DATETIME));
    odpsSchema.addColumn(new Column("f5", OdpsType.BOOLEAN));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("ct", OdpsType.STRING));
    CreateTable(odpsSchema);

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
  }

  @Test
  public void testColumnOrderNotMatchWithOdps() {
    List<String> columnFields = new ArrayList<String>();

    columnFields.add("f1");
    columnFields.add("f2");
    columnFields.add("f3");
    columnFields.add("f5");
    columnFields.add("f4");
    columnFields.add("pt");
    columnFields.add("ct");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("f2", FieldType.BIGINT));
    schema.addField(new Field("f3", FieldType.DOUBLE));
    schema.addField(new Field("f4", FieldType.TIMESTAMP));
    schema.addField(new Field("f5", FieldType.BOOLEAN));
    schema.addField(new Field("pt", FieldType.STRING));
    schema.addField(new Field("ct", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addColumn(new Column("f2", OdpsType.BIGINT));
    odpsSchema.addColumn(new Column("f3", OdpsType.DOUBLE));
    odpsSchema.addColumn(new Column("f4", OdpsType.DATETIME));
    odpsSchema.addColumn(new Column("f5", OdpsType.BOOLEAN));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("ct", OdpsType.STRING));
    CreateTable(odpsSchema);

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
  }

  @Test(expectedExceptions = com.aliyun.datahub.exception.OdpsException.class)
  public void testSubAccount() {
    List<String> columnFields = new ArrayList<String>();

    columnFields.add("f1");
    columnFields.add("f2");
    columnFields.add("f3");
    columnFields.add("f5");
    columnFields.add("f4");
    columnFields.add("pt");
    columnFields.add("ct");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("f2", FieldType.BIGINT));
    schema.addField(new Field("f3", FieldType.DOUBLE));
    schema.addField(new Field("f4", FieldType.TIMESTAMP));
    schema.addField(new Field("f5", FieldType.BOOLEAN));
    schema.addField(new Field("pt", FieldType.STRING));
    schema.addField(new Field("ct", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addColumn(new Column("f2", OdpsType.BIGINT));
    odpsSchema.addColumn(new Column("f3", OdpsType.DOUBLE));
    odpsSchema.addColumn(new Column("f4", OdpsType.DATETIME));
    odpsSchema.addColumn(new Column("f5", OdpsType.BOOLEAN));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("ct", OdpsType.STRING));
    CreateTable(odpsSchema);
    odpsDesc.setAccessId(DatahubTestUtils.getSecondSubAccessId());
    odpsDesc.setAccessKey(DatahubTestUtils.getSecondSubAccesskey());
    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
  }

  @Test
  public void testPartitionOrderNotMatchWithOdps() {
    List<String> columnFields = new ArrayList<String>();

    columnFields.add("f1");
    columnFields.add("f2");
    columnFields.add("f3");
    columnFields.add("f4");
    columnFields.add("f5");
    columnFields.add("pt");
    columnFields.add("ct");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("f2", FieldType.BIGINT));
    schema.addField(new Field("f3", FieldType.DOUBLE));
    schema.addField(new Field("f4", FieldType.TIMESTAMP));
    schema.addField(new Field("f5", FieldType.BOOLEAN));
    schema.addField(new Field("pt", FieldType.STRING));
    schema.addField(new Field("ct", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addColumn(new Column("f2", OdpsType.BIGINT));
    odpsSchema.addColumn(new Column("f3", OdpsType.DOUBLE));
    odpsSchema.addColumn(new Column("f4", OdpsType.DATETIME));
    odpsSchema.addColumn(new Column("f5", OdpsType.BOOLEAN));
    odpsSchema.addPartitionColumn(new Column("ct", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    CreateTable(odpsSchema);

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
  }

  @Test(expectedExceptions = InvalidParameterException.class)
  public void testPartitionNotMatchWithOdps() {
    List<String> columnFields = new ArrayList<String>();

    columnFields.add("f1");
    columnFields.add("f2");
    columnFields.add("f3");
    columnFields.add("f4");
    columnFields.add("f5");
    columnFields.add("pt");
    columnFields.add("ct");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("f2", FieldType.BIGINT));
    schema.addField(new Field("f3", FieldType.DOUBLE));
    schema.addField(new Field("f4", FieldType.TIMESTAMP));
    schema.addField(new Field("f5", FieldType.BOOLEAN));
    schema.addField(new Field("pt", FieldType.STRING));
    schema.addField(new Field("ct", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addColumn(new Column("f2", OdpsType.BIGINT));
    odpsSchema.addColumn(new Column("f3", OdpsType.DOUBLE));
    odpsSchema.addColumn(new Column("f4", OdpsType.DATETIME));
    odpsSchema.addColumn(new Column("f5", OdpsType.BOOLEAN));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    CreateTable(odpsSchema);

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
  }

  @Test(expectedExceptions = InvalidParameterException.class)
  public void testPartitionNotMatchWithOdps2() {
    List<String> columnFields = new ArrayList<String>();

    columnFields.add("f1");
    columnFields.add("f2");
    columnFields.add("f3");
    columnFields.add("f4");
    columnFields.add("f5");
    columnFields.add("pt");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("f2", FieldType.BIGINT));
    schema.addField(new Field("f3", FieldType.DOUBLE));
    schema.addField(new Field("f4", FieldType.TIMESTAMP));
    schema.addField(new Field("f5", FieldType.BOOLEAN));
    schema.addField(new Field("pt", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addColumn(new Column("f2", OdpsType.BIGINT));
    odpsSchema.addColumn(new Column("f3", OdpsType.DOUBLE));
    odpsSchema.addColumn(new Column("f4", OdpsType.DATETIME));
    odpsSchema.addColumn(new Column("f5", OdpsType.BOOLEAN));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("ct", OdpsType.STRING));
    CreateTable(odpsSchema);

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
  }

  @Test
  public void testOdpsPartitionFiledNotString() {
    List<String> columnFields = new ArrayList<String>();

    columnFields.add("f1");
    columnFields.add("f2");
    columnFields.add("f3");
    columnFields.add("f4");
    columnFields.add("f5");
    columnFields.add("pt");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("f2", FieldType.BIGINT));
    schema.addField(new Field("f3", FieldType.DOUBLE));
    schema.addField(new Field("f4", FieldType.TIMESTAMP));
    schema.addField(new Field("f5", FieldType.BOOLEAN));
    schema.addField(new Field("pt", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addColumn(new Column("f2", OdpsType.BIGINT));
    odpsSchema.addColumn(new Column("f3", OdpsType.DOUBLE));
    odpsSchema.addColumn(new Column("f4", OdpsType.DATETIME));
    odpsSchema.addColumn(new Column("f5", OdpsType.BOOLEAN));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.BIGINT));
    CreateTable(odpsSchema);

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
  }

  @Test(expectedExceptions = InvalidParameterException.class)
  public void testPartitionFiledBigint() {
    List<String> columnFields = new ArrayList<String>();

    columnFields.add("f1");
    columnFields.add("f2");
    columnFields.add("f3");
    columnFields.add("f4");
    columnFields.add("f5");
    columnFields.add("pt");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("f2", FieldType.BIGINT));
    schema.addField(new Field("f3", FieldType.DOUBLE));
    schema.addField(new Field("f4", FieldType.TIMESTAMP));
    schema.addField(new Field("f5", FieldType.BOOLEAN));
    schema.addField(new Field("pt", FieldType.BIGINT));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addColumn(new Column("f2", OdpsType.BIGINT));
    odpsSchema.addColumn(new Column("f3", OdpsType.DOUBLE));
    odpsSchema.addColumn(new Column("f4", OdpsType.DATETIME));
    odpsSchema.addColumn(new Column("f5", OdpsType.BOOLEAN));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.BIGINT));
    CreateTable(odpsSchema);

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
  }

  @Test(expectedExceptions = InvalidParameterException.class)
  public void testPartitionFiledNotString() {
    List<String> columnFields = new ArrayList<String>();

    columnFields.add("f1");
    columnFields.add("f2");
    columnFields.add("f3");
    columnFields.add("f4");
    columnFields.add("f5");
    columnFields.add("pt");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("f2", FieldType.BIGINT));
    schema.addField(new Field("f3", FieldType.DOUBLE));
    schema.addField(new Field("f4", FieldType.TIMESTAMP));
    schema.addField(new Field("f5", FieldType.BOOLEAN));
    schema.addField(new Field("pt", FieldType.BIGINT));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addColumn(new Column("f2", OdpsType.BIGINT));
    odpsSchema.addColumn(new Column("f3", OdpsType.DOUBLE));
    odpsSchema.addColumn(new Column("f4", OdpsType.DATETIME));
    odpsSchema.addColumn(new Column("f5", OdpsType.BOOLEAN));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    CreateTable(odpsSchema);

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
  }

  @Test
  public void testWithoutPartition() {
    List<String> columnFields = new ArrayList<String>();

    columnFields.add("f1");
    columnFields.add("f2");
    columnFields.add("f3");
    columnFields.add("f4");
    columnFields.add("f5");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("f2", FieldType.BIGINT));
    schema.addField(new Field("f3", FieldType.DOUBLE));
    schema.addField(new Field("f4", FieldType.TIMESTAMP));
    schema.addField(new Field("f5", FieldType.BOOLEAN));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addColumn(new Column("f2", OdpsType.BIGINT));
    odpsSchema.addColumn(new Column("f3", OdpsType.DOUBLE));
    odpsSchema.addColumn(new Column("f4", OdpsType.DATETIME));
    odpsSchema.addColumn(new Column("f5", OdpsType.BOOLEAN));
    CreateTable(odpsSchema);

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
    ListDataConnectorResult listDataConnectorResult = client.listDataConnector(projectName, topicName);
    List<String> connectors = listDataConnectorResult.getDataConnectors();
    Assert.assertTrue(connectors.contains(connectorType.toString().toLowerCase()));

    GetDataConnectorResult getDataConnectorResult = client.getDataConnector(projectName, topicName, connectorType);
    Assert.assertEquals(getDataConnectorResult.getColumnFields(), columnFields);

    ConnectorExtInfoJsonDeser.ConnectorExtInfoResult detailRs = GetConnectorExtInfo(projectName, topicName);
    Assert.assertEquals(detailRs.getOffsetStoreType(), "coordinator");
    Assert.assertFalse(detailRs.getSubscriptionId().isEmpty());

    putRecords(schema);
    try {
      Thread.sleep(shipperWaitTime);
    } catch (InterruptedException e) {

    }
    // check odps data
    try {
      downloadCheckData(recordCount * shardCount, odpsSchema);
    } catch (IOException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    } catch (TunnelException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }

    getDataConnectorResult = client.getDataConnector(projectName, topicName, connectorType);
    Assert.assertEquals(getDataConnectorResult.getColumnFields(), columnFields);
    for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
      Assert.assertEquals(sc.getCurSequence(), recordCount);
      Assert.assertEquals(GetCurSequenceBySubscriptionId(projectName, topicName, detailRs.getSubscriptionId(),
              sc.getShardId()), sc.getCurSequence());
    }

    Assert.assertTrue(IsSubscriptionExist(projectName, topicName, detailRs.getSubscriptionId()));
    client.deleteDataConnector(projectName, topicName, connectorType);
    Assert.assertFalse(IsSubscriptionExist(projectName, topicName, detailRs.getSubscriptionId()));

    listDataConnectorResult = client.listDataConnector(projectName, topicName);
    connectors = listDataConnectorResult.getDataConnectors();
    Assert.assertFalse(connectors.contains(connectorType.toString().toLowerCase()));
  }

  @Test(expectedExceptions = InvalidParameterException.class)
  public void testFieldNotExist() {
    List<String> columnFields = new ArrayList<String>();

    columnFields.add("f1");
    columnFields.add("f2");
    columnFields.add("f3");
    columnFields.add("f4");
    columnFields.add("f6");
    columnFields.add("pt");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("f2", FieldType.BIGINT));
    schema.addField(new Field("f3", FieldType.DOUBLE));
    schema.addField(new Field("f4", FieldType.TIMESTAMP));
    schema.addField(new Field("f5", FieldType.BOOLEAN));
    schema.addField(new Field("pt", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addColumn(new Column("f2", OdpsType.BIGINT));
    odpsSchema.addColumn(new Column("f3", OdpsType.DOUBLE));
    odpsSchema.addColumn(new Column("f4", OdpsType.DATETIME));
    odpsSchema.addColumn(new Column("f5", OdpsType.BOOLEAN));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    CreateTable(odpsSchema);

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
  }

  @Test(expectedExceptions = InvalidParameterException.class)
  public void testTableColSizeNotMatch() {
    List<String> columnFields = new ArrayList<String>();
    List<String> partitionFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("f2");
    columnFields.add("f3");
    columnFields.add("f4");
    columnFields.add("f5");
    columnFields.add("pt");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("f2", FieldType.BIGINT));
    schema.addField(new Field("f3", FieldType.DOUBLE));
    schema.addField(new Field("f4", FieldType.TIMESTAMP));
    schema.addField(new Field("f5", FieldType.BOOLEAN));
    schema.addField(new Field("pt", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addColumn(new Column("f2", OdpsType.BIGINT));
    odpsSchema.addColumn(new Column("f3", OdpsType.DOUBLE));
    odpsSchema.addColumn(new Column("f4", OdpsType.DATETIME));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    CreateTable(odpsSchema);

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
  }

  @Test(expectedExceptions = InvalidParameterException.class)
  public void testTableColTypeNotMatch() {
    List<String> columnFields = new ArrayList<String>();

    columnFields.add("f1");
    columnFields.add("f2");
    columnFields.add("f3");
    columnFields.add("f4");
    columnFields.add("f5");
    columnFields.add("pt");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("f2", FieldType.BIGINT));
    schema.addField(new Field("f3", FieldType.DOUBLE));
    schema.addField(new Field("f4", FieldType.TIMESTAMP));
    schema.addField(new Field("f5", FieldType.BOOLEAN));
    schema.addField(new Field("pt", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addColumn(new Column("f2", OdpsType.BIGINT));
    odpsSchema.addColumn(new Column("f3", OdpsType.DOUBLE));
    odpsSchema.addColumn(new Column("f4", OdpsType.DATETIME));
    odpsSchema.addColumn(new Column("f5", OdpsType.DATETIME));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    CreateTable(odpsSchema);

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
  }

  @Test(expectedExceptions = InvalidParameterException.class)
  public void testTopicFieldSizeNotMatch() {
    List<String> columnFields = new ArrayList<String>();

    columnFields.add("f1");
    columnFields.add("f2");
    columnFields.add("f3");
    columnFields.add("f4");
    columnFields.add("f5");
    columnFields.add("pt");
    columnFields.add("pt");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("f2", FieldType.BIGINT));
    schema.addField(new Field("f3", FieldType.DOUBLE));
    schema.addField(new Field("f4", FieldType.TIMESTAMP));
    schema.addField(new Field("f5", FieldType.BOOLEAN));
    schema.addField(new Field("pt", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addColumn(new Column("f2", OdpsType.BIGINT));
    odpsSchema.addColumn(new Column("f3", OdpsType.DOUBLE));
    odpsSchema.addColumn(new Column("f4", OdpsType.DATETIME));
    odpsSchema.addColumn(new Column("f5", OdpsType.BOOLEAN));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    CreateTable(odpsSchema);

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
  }

  @Test(expectedExceptions = InvalidParameterException.class)
  public void testDuplicateField() {
    List<String> columnFields = new ArrayList<String>();

    columnFields.add("f1");
    columnFields.add("f1");
    columnFields.add("pt");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("f2", FieldType.STRING));
    schema.addField(new Field("pt", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addColumn(new Column("f2", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    CreateTable(odpsSchema);

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
  }

  @Test(expectedExceptions = InvalidParameterException.class)
  public void testMissingOdpsEndpoint() {
    List<String> columnFields = new ArrayList<String>();

    columnFields.add("f1");
    columnFields.add("pt");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("pt", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    CreateTable(odpsSchema);

    odpsDesc.setOdpsEndpoint("");

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
  }

  @Test(expectedExceptions = InvalidParameterException.class)
  public void testInvaliadOdpsEndpoint() {
    List<String> columnFields = new ArrayList<String>();

    columnFields.add("f1");
    columnFields.add("pt");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("pt", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    CreateTable(odpsSchema);

    odpsDesc.setOdpsEndpoint("invalid");

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
  }

  @Test(expectedExceptions = InvalidParameterException.class)
  public void testInvaliadTunnelEndpoint() {
    List<String> columnFields = new ArrayList<String>();

    columnFields.add("f1");
    columnFields.add("pt");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("pt", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    CreateTable(odpsSchema);

    odpsDesc.setTunnelEndpoint("invalid");

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
  }

  @Test(expectedExceptions = DatahubServiceException.class)
  public void testInvaliadOdpsAccessKey() {
    List<String> columnFields = new ArrayList<String>();

    columnFields.add("f1");
    columnFields.add("pt");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("pt", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    CreateTable(odpsSchema);

    odpsDesc.setAccessKey("invalid");

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
  }

  @Test(expectedExceptions = DatahubServiceException.class)
  public void testInvaliadOdpsAccessId() {
    List<String> columnFields = new ArrayList<String>();

    columnFields.add("f1");
    columnFields.add("pt");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("pt", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    CreateTable(odpsSchema);

    odpsDesc.setAccessId("invalid");

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
  }

  @Test(expectedExceptions = DatahubServiceException.class)
  public void testInvaliadOdpsTable() {
    List<String> columnFields = new ArrayList<String>();

    columnFields.add("f1");
    columnFields.add("pt");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("pt", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    CreateTable(odpsSchema);

    odpsDesc.setTable("invalid");

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
  }

  @Test(expectedExceptions = DatahubServiceException.class)
  public void testInvaliadOdpsProject() {
    List<String> columnFields = new ArrayList<String>();

    columnFields.add("f1");
    columnFields.add("pt");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("pt", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    CreateTable(odpsSchema);

    odpsDesc.setProject("invalid");

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
  }

  @Test
  public void testRouteTunnelEndpoint() {
    List<String> columnFields = new ArrayList<String>();

    columnFields.add("f1");
    columnFields.add("pt");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("pt", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    CreateTable(odpsSchema);

    odpsDesc.setTunnelEndpoint("");

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);

    ListDataConnectorResult listDataConnectorResult = client.listDataConnector(projectName, topicName);
    List<String> connectors = listDataConnectorResult.getDataConnectors();
    Assert.assertTrue(connectors.contains(connectorType.toString().toLowerCase()));

    GetDataConnectorResult getDataConnectorResult = client.getDataConnector(projectName, topicName, connectorType);
    Assert.assertEquals(getDataConnectorResult.getColumnFields(), columnFields);

    ConnectorExtInfoJsonDeser.ConnectorExtInfoResult detailRs = GetConnectorExtInfo(projectName, topicName);
    Assert.assertEquals(detailRs.getOffsetStoreType(), "coordinator");
    Assert.assertFalse(detailRs.getSubscriptionId().isEmpty());

    putRecords(schema);
    try {
      Thread.sleep(shipperWaitTime);
    } catch (InterruptedException e) {

    }
    // check odps data
    try {
      downloadCheckData(recordCount * shardCount, odpsSchema);
    } catch (IOException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    } catch (TunnelException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }

    getDataConnectorResult = client.getDataConnector(projectName, topicName, connectorType);
    Assert.assertEquals(getDataConnectorResult.getColumnFields(), columnFields);
    for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
      Assert.assertEquals(sc.getCurSequence(), recordCount);
      Assert.assertEquals(GetCurSequenceBySubscriptionId(projectName, topicName, detailRs.getSubscriptionId(),
              sc.getShardId()), sc.getCurSequence());
    }

    Assert.assertTrue(IsSubscriptionExist(projectName, topicName, detailRs.getSubscriptionId()));
    client.deleteDataConnector(projectName, topicName, connectorType);
    Assert.assertFalse(IsSubscriptionExist(projectName, topicName, detailRs.getSubscriptionId()));

    listDataConnectorResult = client.listDataConnector(projectName, topicName);
    connectors = listDataConnectorResult.getDataConnectors();
    Assert.assertFalse(connectors.contains(connectorType.toString().toLowerCase()));
  }

  @Test(expectedExceptions = ResourceNotFoundException.class)
  public void testInvaliadProject() {
    List<String> columnFields = new ArrayList<String>();

    columnFields.add("f1");
    columnFields.add("pt");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("pt", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    CreateTable(odpsSchema);

    client.createDataConnector("project_not_exist", topicName, connectorType, columnFields, odpsDesc);
  }

  @Test(expectedExceptions = ResourceNotFoundException.class)
  public void testInvaliadTopic() {
    List<String> columnFields = new ArrayList<String>();

    columnFields.add("f1");
    columnFields.add("pt");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("pt", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    CreateTable(odpsSchema);

    client.createDataConnector(projectName, "topic_not_exist", connectorType, columnFields, odpsDesc);
  }

  @Test(expectedExceptions = ResourceExistException.class)
  public void testConnectorAlreadyExist() {
    List<String> columnFields = new ArrayList<String>();

    columnFields.add("f1");
    columnFields.add("pt");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("pt", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    CreateTable(odpsSchema);

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
  }

  @Test(expectedExceptions = ResourceExistException.class)
  public void testOdpsConnectorAlreadyExist() {
    List<String> columnFields = new ArrayList<String>();

    columnFields.add("f1");
    columnFields.add("pt");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("pt", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    CreateTable(odpsSchema);

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
  }

  @Test
  public void testSameConnectorDiffTopic() {
    List<String> columnFields = new ArrayList<String>();

    columnFields.add("f1");
    columnFields.add("pt");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("pt", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    CreateTable(odpsSchema);

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
    Assert.assertTrue(client.listDataConnector(projectName, topicName).getDataConnectors().size() == 1);
    Assert.assertTrue(client.listDataConnector(projectName, topicName).getDataConnectors().contains(connectorType.toString().toLowerCase()));

    ConnectorExtInfoJsonDeser.ConnectorExtInfoResult detailRs = GetConnectorExtInfo(projectName, topicName);
    Assert.assertEquals(detailRs.getOffsetStoreType(), "coordinator");
    Assert.assertFalse(detailRs.getSubscriptionId().isEmpty());

    putRecords(schema);
    try {
      Thread.sleep(shipperWaitTime);
    } catch (InterruptedException e) {

    }
    // check odps data
    try {
      downloadCheckData(recordCount * shardCount, odpsSchema);
    } catch (IOException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    } catch (TunnelException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }

    String subId = detailRs.getSubscriptionId();
    GetDataConnectorResult getDataConnectorResult = client.getDataConnector(projectName, topicName, connectorType);
    Assert.assertEquals(getDataConnectorResult.getColumnFields(), columnFields);
    for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
      Assert.assertEquals(sc.getCurSequence(), recordCount);
      Assert.assertEquals(GetCurSequenceBySubscriptionId(projectName, topicName,
              subId, sc.getShardId()), sc.getCurSequence());
    }

    topicName += "_tmp";
    CreateTopic(schema);
    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);

    detailRs = GetConnectorExtInfo(projectName, topicName);
    Assert.assertEquals(detailRs.getOffsetStoreType(), "coordinator");
    Assert.assertFalse(detailRs.getSubscriptionId().isEmpty());
    Assert.assertNotEquals(detailRs.getSubscriptionId(), subId);

    putRecords(schema);
    try {
      Thread.sleep(shipperWaitTime);
    } catch (InterruptedException e) {

    }
    // check odps data
    try {
      downloadCheckData(recordCount * shardCount * 2, odpsSchema);
    } catch (IOException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    } catch (TunnelException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }

    getDataConnectorResult = client.getDataConnector(projectName, topicName, connectorType);
    Assert.assertEquals(getDataConnectorResult.getColumnFields(), columnFields);
    for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
      Assert.assertEquals(sc.getCurSequence(), recordCount);
      Assert.assertEquals(GetCurSequenceBySubscriptionId(projectName, topicName, detailRs.getSubscriptionId(),
              sc.getShardId()), sc.getCurSequence());
    }

    Assert.assertTrue(client.listDataConnector(projectName, topicName).getDataConnectors().size() == 1);
    Assert.assertTrue(client.listDataConnector(projectName, topicName).getDataConnectors().contains(connectorType.toString().toLowerCase()));
    client.deleteDataConnector(projectName, topicName.replace("_tmp", ""), connectorType);
    client.deleteTopic(projectName, topicName.replace("_tmp", ""));
  }

  @Test(expectedExceptions = ResourceNotFoundException.class)
  public void testGetconnectorNotExist() {
    client.getDataConnector(projectName, topicName, connectorType);
  }

  @Test(expectedExceptions = ResourceNotFoundException.class)
  public void testListconnectorProjectNotExist() {
    client.listDataConnector("Project_not_exist", topicName);
  }

  @Test
  public void testSinkOdpsShardSplit() {
    List<String> columnFields = new ArrayList<String>();

    columnFields.add("f1");
    columnFields.add("f2");
    columnFields.add("f3");
    columnFields.add("f4");
    columnFields.add("f5");
    columnFields.add("pt");
    columnFields.add("ct");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("f2", FieldType.BIGINT));
    schema.addField(new Field("f3", FieldType.DOUBLE));
    schema.addField(new Field("f4", FieldType.TIMESTAMP));
    schema.addField(new Field("f5", FieldType.BOOLEAN));
    schema.addField(new Field("pt", FieldType.STRING));
    schema.addField(new Field("ct", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addColumn(new Column("f2", OdpsType.BIGINT));
    odpsSchema.addColumn(new Column("f3", OdpsType.DOUBLE));
    odpsSchema.addColumn(new Column("f4", OdpsType.DATETIME));
    odpsSchema.addColumn(new Column("f5", OdpsType.BOOLEAN));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("ct", OdpsType.STRING));
    CreateTable(odpsSchema);

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);

    ListDataConnectorResult listDataConnectorResult = client.listDataConnector(projectName, topicName);
    List<String> connectors = listDataConnectorResult.getDataConnectors();
    Assert.assertTrue(connectors.contains(connectorType.toString().toLowerCase()));

    GetDataConnectorResult getDataConnectorResult = client.getDataConnector(projectName, topicName, connectorType);
    Assert.assertEquals(getDataConnectorResult.getColumnFields(), columnFields);

    ConnectorExtInfoJsonDeser.ConnectorExtInfoResult detailRs = GetConnectorExtInfo(projectName, topicName);
    Assert.assertEquals(detailRs.getOffsetStoreType(), "coordinator");
    Assert.assertFalse(detailRs.getSubscriptionId().isEmpty());

    for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
      Assert.assertEquals(sc.getCurSequence(), 0);
    }

    putRecords(schema);
    try {
      Thread.sleep(shipperWaitTime);
    } catch (InterruptedException e) {

    }
    client.splitShard(projectName, topicName, "0");
    client.waitForShardReady(projectName, topicName);

    putRecords(schema);

    try {
      Thread.sleep(shipperWaitTime + 60 * 1000);
    } catch (InterruptedException e) {

    }

    getDataConnectorResult = client.getDataConnector(projectName, topicName, connectorType);
    Assert.assertEquals(getDataConnectorResult.getColumnFields(), columnFields);
    ListShardResult listShardResult = client.listShard(projectName, topicName);
    for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
      for (ShardEntry se : listShardResult.getShards()) {
        if (sc.getShardId().equals(se.getShardId())) {
          if (se.getState() == ShardState.CLOSED) {
            System.out.println("ShardId:"+se.getShardId()+",subId:"+detailRs.getSubscriptionId()
                    +",Seq:"+sc.getCurSequence()
                    +",Seq2:"+GetCurSequenceBySubscriptionId(projectName, topicName, detailRs.getSubscriptionId(), sc.getShardId()));
            Assert.assertEquals(sc.getCurSequence(), recordCount);
            Assert.assertEquals(GetCurSequenceBySubscriptionId(projectName, topicName,
                    detailRs.getSubscriptionId(), sc.getShardId()), sc.getCurSequence());
          } else if (se.getParentShardIds().contains("0")) {
            Assert.assertEquals(sc.getCurSequence(), recordCount);
            Assert.assertEquals(GetCurSequenceBySubscriptionId(projectName, topicName,
                    detailRs.getSubscriptionId(), sc.getShardId()), sc.getCurSequence());
          } else {
            Assert.assertEquals(sc.getCurSequence(), 2 * recordCount);
            Assert.assertEquals(GetCurSequenceBySubscriptionId(projectName, topicName,
                    detailRs.getSubscriptionId(), sc.getShardId()), sc.getCurSequence());
          }
        } else {
          continue;
        }
      }
    }

    Assert.assertTrue(IsSubscriptionExist(projectName, topicName, detailRs.getSubscriptionId()));
    client.deleteDataConnector(projectName, topicName, connectorType);
    Assert.assertFalse(IsSubscriptionExist(projectName, topicName, detailRs.getSubscriptionId()));

    listDataConnectorResult = client.listDataConnector(projectName, topicName);
    connectors = listDataConnectorResult.getDataConnectors();
    Assert.assertFalse(connectors.contains(connectorType.toString().toLowerCase()));
  }

  @Test
  public void testSinkOdpsShardMerge() {
    List<String> columnFields = new ArrayList<String>();

    columnFields.add("f1");
    columnFields.add("f2");
    columnFields.add("f3");
    columnFields.add("f4");
    columnFields.add("f5");
    columnFields.add("pt");
    columnFields.add("ct");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("f2", FieldType.BIGINT));
    schema.addField(new Field("f3", FieldType.DOUBLE));
    schema.addField(new Field("f4", FieldType.TIMESTAMP));
    schema.addField(new Field("f5", FieldType.BOOLEAN));
    schema.addField(new Field("pt", FieldType.STRING));
    schema.addField(new Field("ct", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addColumn(new Column("f2", OdpsType.BIGINT));
    odpsSchema.addColumn(new Column("f3", OdpsType.DOUBLE));
    odpsSchema.addColumn(new Column("f4", OdpsType.DATETIME));
    odpsSchema.addColumn(new Column("f5", OdpsType.BOOLEAN));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("ct", OdpsType.STRING));
    CreateTable(odpsSchema);

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);

    ListDataConnectorResult listDataConnectorResult = client.listDataConnector(projectName, topicName);
    List<String> connectors = listDataConnectorResult.getDataConnectors();
    Assert.assertTrue(connectors.contains(connectorType.toString().toLowerCase()));

    GetDataConnectorResult getDataConnectorResult = client.getDataConnector(projectName, topicName, connectorType);
    Assert.assertEquals(getDataConnectorResult.getColumnFields(), columnFields);

    ConnectorExtInfoJsonDeser.ConnectorExtInfoResult detailRs = GetConnectorExtInfo(projectName, topicName);
    Assert.assertEquals(detailRs.getOffsetStoreType(), "coordinator");
    Assert.assertFalse(detailRs.getSubscriptionId().isEmpty());

    for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
      Assert.assertEquals(sc.getCurSequence(), 0);
      Assert.assertEquals(GetCurSequenceBySubscriptionId(projectName, topicName,
              detailRs.getSubscriptionId(), sc.getShardId()), sc.getCurSequence());
    }

    putRecords(schema);
    try {
      Thread.sleep(shipperWaitTime);
    } catch (InterruptedException e) {

    }
    client.mergeShard(projectName, topicName, "0", "1");
    client.waitForShardReady(projectName, topicName);

    putRecords(schema);

    try {
      Thread.sleep(shipperWaitTime + 60 * 1000);
    } catch (InterruptedException e) {

    }

    getDataConnectorResult = client.getDataConnector(projectName, topicName, connectorType);
    Assert.assertEquals(getDataConnectorResult.getColumnFields(), columnFields);
    ListShardResult listShardResult = client.listShard(projectName, topicName);
    for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
      for (ShardEntry se : listShardResult.getShards()) {
        if (sc.getShardId().equals(se.getShardId())) {
          if (se.getState() == ShardState.CLOSED) {
            Assert.assertEquals(sc.getCurSequence(), recordCount);
            Assert.assertEquals(GetCurSequenceBySubscriptionId(projectName, topicName,
                    detailRs.getSubscriptionId(), sc.getShardId()), sc.getCurSequence());
          } else if (se.getParentShardIds().contains("0")) {
            Assert.assertEquals(sc.getCurSequence(), recordCount);
            Assert.assertEquals(GetCurSequenceBySubscriptionId(projectName, topicName,
                    detailRs.getSubscriptionId(), sc.getShardId()), sc.getCurSequence());
          } else {
            Assert.assertEquals(sc.getCurSequence(), 2 * recordCount);
            Assert.assertEquals(GetCurSequenceBySubscriptionId(projectName, topicName,
                    detailRs.getSubscriptionId(), sc.getShardId()), sc.getCurSequence());
          }
        } else {
          continue;
        }
      }
    }

    Assert.assertTrue(IsSubscriptionExist(projectName, topicName, detailRs.getSubscriptionId()));
    client.deleteDataConnector(projectName, topicName, connectorType);
    Assert.assertFalse(IsSubscriptionExist(projectName, topicName, detailRs.getSubscriptionId()));

    listDataConnectorResult = client.listDataConnector(projectName, topicName);
    connectors = listDataConnectorResult.getDataConnectors();
    Assert.assertFalse(connectors.contains(connectorType.toString().toLowerCase()));
  }

  @Test(expectedExceptions = ResourceNotFoundException.class)
  public void testReloadInvalidConnectorShard() {
    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("pt");
    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("pt", FieldType.STRING));
    CreateTopic(schema);
    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    CreateTable(odpsSchema);
    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
    client.reloadDataConnector(projectName, topicName, connectorType, "11");
  }

  @Test(expectedExceptions = ResourceNotFoundException.class)
  public void testReloadInvalidConnector() {
    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("pt");
    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("pt", FieldType.STRING));
    CreateTopic(schema);
    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    CreateTable(odpsSchema);
    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
    client.reloadDataConnector(projectName, topicName + "notexist", connectorType);
  }

  @Test
  public void testReloadConnectorNormal() {
    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("pt");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("pt", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    CreateTable(odpsSchema);

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);

    ListDataConnectorResult listDataConnectorResult = client.listDataConnector(projectName, topicName);
    List<String> connectors = listDataConnectorResult.getDataConnectors();
    Assert.assertTrue(connectors.contains(connectorType.toString().toLowerCase()));

    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {

    }

    GetDataConnectorResult getDataConnectorResult = client.getDataConnector(projectName, topicName, connectorType);
    Assert.assertEquals(getDataConnectorResult.getColumnFields(), columnFields);

    ConnectorExtInfoJsonDeser.ConnectorExtInfoResult detailRs = GetConnectorExtInfo(projectName, topicName);
    Assert.assertEquals(detailRs.getOffsetStoreType(), "coordinator");
    Assert.assertFalse(detailRs.getSubscriptionId().isEmpty());

    GetDataConnectorShardStatusResult getDataConnectorShardStatusResult = client.getDataConnectorShardStatus(projectName, topicName, connectorType, "0");
    Assert.assertEquals(getDataConnectorShardStatusResult.getStartSequence(), 0);
    Assert.assertEquals(getDataConnectorShardStatusResult.getCurSequence(), -1);
//    Assert.assertEquals(getDataConnectorShardStatusResult.getLastErrorMessage(), "");

    String subId = detailRs.getSubscriptionId();
    DropTable();

    putRecords(schema);
    try {
      Thread.sleep(shipperWaitTime);
    } catch (InterruptedException e) {

    }

    getDataConnectorShardStatusResult = client.getDataConnectorShardStatus(projectName, topicName, connectorType, "0");
    Assert.assertEquals(getDataConnectorShardStatusResult.getStartSequence(), 0);
    Assert.assertEquals(getDataConnectorShardStatusResult.getCurSequence(), -1);
//    Assert.assertNotEquals(getDataConnectorShardStatusResult.getLastErrorMessage(), "");

    CreateTable(odpsSchema);
    // re create connector
    Assert.assertTrue(IsSubscriptionExist(projectName, topicName, subId));
    client.deleteDataConnector(projectName, topicName, connectorType);
    Assert.assertFalse(IsSubscriptionExist(projectName, topicName, subId));

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
    try {
      Thread.sleep(shipperWaitTime);
    } catch (InterruptedException e) {

    }

    // check odps data
    try {
      downloadCheckData(recordCount * shardCount, odpsSchema);
    } catch (IOException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    } catch (TunnelException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }

    getDataConnectorResult = client.getDataConnector(projectName, topicName, connectorType);
    Assert.assertEquals(getDataConnectorResult.getColumnFields(), columnFields);

    detailRs = GetConnectorExtInfo(projectName, topicName);
    Assert.assertNotEquals(detailRs.getSubscriptionId(), subId);
    for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
      Assert.assertEquals(sc.getCurSequence(), recordCount);

      getDataConnectorShardStatusResult = client.getDataConnectorShardStatus(projectName, topicName, connectorType, sc.getShardId());
      Assert.assertEquals(getDataConnectorShardStatusResult.getStartSequence(), 0);
      Assert.assertEquals(getDataConnectorShardStatusResult.getCurSequence(), recordCount);
//      Assert.assertEquals(getDataConnectorShardStatusResult.getLastErrorMessage(), "");
    }

    Assert.assertTrue(IsSubscriptionExist(projectName, topicName, detailRs.getSubscriptionId()));
    client.deleteDataConnector(projectName, topicName, connectorType);
    Assert.assertFalse(IsSubscriptionExist(projectName, topicName, detailRs.getSubscriptionId()));

    listDataConnectorResult = client.listDataConnector(projectName, topicName);
    connectors = listDataConnectorResult.getDataConnectors();
    Assert.assertFalse(connectors.contains(connectorType.toString().toLowerCase()));
  }

  @Test
  public void testGetDataConnectorShardStatusNormal() {
    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("pt");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("pt", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    CreateTable(odpsSchema);

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);

    ConnectorExtInfoJsonDeser.ConnectorExtInfoResult detailRs = GetConnectorExtInfo(projectName, topicName);
    Assert.assertEquals(detailRs.getOffsetStoreType(), "coordinator");
    Assert.assertFalse(detailRs.getSubscriptionId().isEmpty());

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {

    }

    DropTable();
    putRecords(schema);
    try {
      Thread.sleep(shipperWaitTime);
    } catch (InterruptedException e) {

    }

    GetDataConnectorShardStatusResult getDataConnectorShardStatusResult = client.getDataConnectorShardStatus(projectName, topicName, connectorType, "0");
    Assert.assertEquals(getDataConnectorShardStatusResult.getStartSequence(), 0);
    Assert.assertEquals(getDataConnectorShardStatusResult.getCurSequence(), -1);
    Assert.assertNotEquals(getDataConnectorShardStatusResult.getLastErrorMessage(), "");

    Assert.assertTrue(IsSubscriptionExist(projectName, topicName, detailRs.getSubscriptionId()));
    client.deleteDataConnector(projectName, topicName, connectorType);
    Assert.assertFalse(IsSubscriptionExist(projectName, topicName, detailRs.getSubscriptionId()));
  }

  @Test(expectedExceptions = ResourceNotFoundException.class)
  public void testGetDataConnectorShardStatusInvalidShard() {
    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("pt");
    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("pt", FieldType.STRING));
    CreateTopic(schema);
    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    CreateTable(odpsSchema);
    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
    GetDataConnectorShardStatusResult getDataConnectorShardStatusResult = client.getDataConnectorShardStatus(projectName, topicName, connectorType, "11");
  }

  @Test(expectedExceptions = ResourceNotFoundException.class)
  public void testGetDataConnectorShardStatusInvalidConnector() {
    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("pt");
    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("pt", FieldType.STRING));
    CreateTopic(schema);
    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    CreateTable(odpsSchema);
    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
    GetDataConnectorShardStatusResult getDataConnectorShardStatusResult = client.getDataConnectorShardStatus(projectName, topicName + "notexist", connectorType, "0");
  }

  @Test(expectedExceptions = InvalidParameterException.class)
  public void testCreateConnector4BlobTopic() {
    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("pt");

    CreateTopic();
    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    CreateTable(odpsSchema);
    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
  }

  @Test
  public void testSinkOdpsConnectorRecreateTable() {
    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("pt");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("pt", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    CreateTable(odpsSchema);

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);

    putRecords(schema);
    try {
      Thread.sleep(shipperWaitTime);
    } catch (InterruptedException e) {

    }
    // check odps data
    try {
      downloadCheckData(recordCount * shardCount, odpsSchema);
    } catch (IOException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    } catch (TunnelException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }

    GetDataConnectorResult getDataConnectorResult = client.getDataConnector(projectName, topicName, connectorType);
    Assert.assertEquals(getDataConnectorResult.getColumnFields(), columnFields);

    ConnectorExtInfoJsonDeser.ConnectorExtInfoResult detailRs = GetConnectorExtInfo(projectName, topicName);
    Assert.assertEquals(detailRs.getOffsetStoreType(), "coordinator");
    Assert.assertFalse(detailRs.getSubscriptionId().isEmpty());

    String subId = detailRs.getSubscriptionId();
    for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
      Assert.assertEquals(sc.getCurSequence(), recordCount);
      Assert.assertEquals(GetCurSequenceBySubscriptionId(projectName, topicName,
              subId, sc.getShardId()), sc.getCurSequence());
    }

    Assert.assertTrue(IsSubscriptionExist(projectName, topicName, subId));
    client.deleteDataConnector(projectName, topicName, connectorType);
    Assert.assertFalse(IsSubscriptionExist(projectName, topicName, subId));

    DropTable();
    CreateTable(odpsSchema);
    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);

    try {
      Thread.sleep(shipperWaitTime);
    } catch (InterruptedException e) {

    }
    // check odps data
    try {
      downloadCheckData(recordCount * shardCount, odpsSchema);
    } catch (IOException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    } catch (TunnelException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }

    getDataConnectorResult = client.getDataConnector(projectName, topicName, connectorType);
    Assert.assertEquals(getDataConnectorResult.getColumnFields(), columnFields);

    detailRs = GetConnectorExtInfo(projectName, topicName);
    Assert.assertEquals(detailRs.getOffsetStoreType(), "coordinator");
    Assert.assertFalse(detailRs.getSubscriptionId().isEmpty());
    Assert.assertNotEquals(detailRs.getSubscriptionId(), subId);

    for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
      Assert.assertEquals(sc.getCurSequence(), recordCount);
      Assert.assertEquals(GetCurSequenceBySubscriptionId(projectName, topicName,
              detailRs.getSubscriptionId(), sc.getShardId()), sc.getCurSequence());
    }
  }

  @Test
  public void testSinkOdpsConnectorRecreateTableAndTopic() {
    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("pt");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("pt", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    CreateTable(odpsSchema);

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);

    putRecords(schema);
    try {
      Thread.sleep(shipperWaitTime);
    } catch (InterruptedException e) {

    }
    // check odps data
    try {
      downloadCheckData(recordCount * shardCount, odpsSchema);
    } catch (IOException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    } catch (TunnelException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }

    GetDataConnectorResult getDataConnectorResult = client.getDataConnector(projectName, topicName, connectorType);
    Assert.assertEquals(getDataConnectorResult.getColumnFields(), columnFields);

    ConnectorExtInfoJsonDeser.ConnectorExtInfoResult detailRs = GetConnectorExtInfo(projectName, topicName);
    Assert.assertEquals(detailRs.getOffsetStoreType(), "coordinator");
    Assert.assertFalse(detailRs.getSubscriptionId().isEmpty());

    String subId = detailRs.getSubscriptionId();
    for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
      Assert.assertEquals(sc.getCurSequence(), recordCount);
      Assert.assertEquals(GetCurSequenceBySubscriptionId(projectName, topicName,
              subId, sc.getShardId()), sc.getCurSequence());
    }

    Assert.assertTrue(IsSubscriptionExist(projectName, topicName, subId));
    client.deleteDataConnector(projectName, topicName, connectorType);
    Assert.assertFalse(IsSubscriptionExist(projectName, topicName, subId));

    client.deleteTopic(projectName, topicName);
    DropTable();
    topicName = topicName + "_recreate";
    CreateTopic(schema);
    putRecords(schema);
    CreateTable(odpsSchema);
    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);

    try {
      Thread.sleep(shipperWaitTime);
    } catch (InterruptedException e) {

    }
    // check odps data
    try {
      downloadCheckData(recordCount * shardCount, odpsSchema);
    } catch (IOException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    } catch (TunnelException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }

    getDataConnectorResult = client.getDataConnector(projectName, topicName, connectorType);
    Assert.assertEquals(getDataConnectorResult.getColumnFields(), columnFields);

    detailRs = GetConnectorExtInfo(projectName, topicName);
    Assert.assertEquals(detailRs.getOffsetStoreType(), "coordinator");
    Assert.assertFalse(detailRs.getSubscriptionId().isEmpty());
    Assert.assertNotEquals(detailRs.getSubscriptionId(), subId);

    for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
      Assert.assertEquals(sc.getCurSequence(), recordCount);
      Assert.assertEquals(GetCurSequenceBySubscriptionId(projectName, topicName,
              detailRs.getSubscriptionId(), sc.getShardId()), sc.getCurSequence());
    }

    Assert.assertTrue(IsSubscriptionExist(projectName, topicName, detailRs.getSubscriptionId()));
    client.deleteDataConnector(projectName, topicName, connectorType);
    Assert.assertFalse(IsSubscriptionExist(projectName, topicName, detailRs.getSubscriptionId()));
  }

  @Test
  public void testDeleteConnectorTopicUpperString() {
    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    CreateTable(odpsSchema);

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);

    ConnectorExtInfoJsonDeser.ConnectorExtInfoResult detailRs = GetConnectorExtInfo(projectName, topicName);
    Assert.assertEquals(detailRs.getOffsetStoreType(), "coordinator");
    Assert.assertFalse(detailRs.getSubscriptionId().isEmpty());

    Assert.assertTrue(IsSubscriptionExist(projectName, topicName, detailRs.getSubscriptionId()));
    client.deleteDataConnector(projectName, topicName.toUpperCase(), connectorType);
    Assert.assertFalse(IsSubscriptionExist(projectName, topicName, detailRs.getSubscriptionId()));

    ListDataConnectorResult listDataConnectorResult = client.listDataConnector(projectName, topicName);
    List<String> connectors = listDataConnectorResult.getDataConnectors();
    Assert.assertTrue(!connectors.contains(connectorType.toString().toLowerCase()));
  }

  @Test
  public void testDeleteTopicUpperString() {
    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    CreateTable(odpsSchema);

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);

    ConnectorExtInfoJsonDeser.ConnectorExtInfoResult detailRs = GetConnectorExtInfo(projectName, topicName);
    Assert.assertEquals(detailRs.getOffsetStoreType(), "coordinator");
    Assert.assertFalse(detailRs.getSubscriptionId().isEmpty());

    Assert.assertTrue(IsSubscriptionExist(projectName, topicName, detailRs.getSubscriptionId()));
    client.deleteDataConnector(projectName, topicName, connectorType);
    Assert.assertFalse(IsSubscriptionExist(projectName, topicName, detailRs.getSubscriptionId()));

    client.deleteTopic(projectName, topicName.toUpperCase());
    ListDataConnectorResult listDataConnectorResult = client.listDataConnector(projectName, topicName);
    List<String> connectors = listDataConnectorResult.getDataConnectors();
    Assert.assertTrue(!connectors.contains(connectorType.toString().toLowerCase()));

  }

  @Test
  public void testCheckCreateIntance() {
    try {
      List<String> columnFields = new ArrayList<String>();
      columnFields.add("f1");

      RecordSchema schema = new RecordSchema();
      schema.addField(new Field("f1", FieldType.STRING));
      CreateTopic(schema);

      TableSchema odpsSchema = new TableSchema();
      odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
      CreateTable(odpsSchema);

      odpsDesc.setAccessId(DatahubTestUtils.getAccessId());
      odpsDesc.setAccessKey(DatahubTestUtils.getAccessKey());

      com.aliyun.odps.security.SecurityManager sm = odps.projects().get().getSecurityManager();
      sm.runQuery("REVOKE CreateInstance ON PROJECT " + odpsProject + " from user " + DatahubTestUtils.getDefaultUser(), false);
      try {
        client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
        Assert.assertTrue(false);
      } catch (com.aliyun.datahub.exception.OdpsException e) {
        Assert.assertEquals(e.getMessage().contains("You have no CreateInstance permission"), true);
      }

      sm.runQuery("grant CreateInstance on project " + odpsProject + " to user " + DatahubTestUtils.getDefaultUser(), false);
      try {
        client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
        Assert.assertTrue(false);
      } catch (com.aliyun.datahub.exception.OdpsException e) {
        Assert.assertEquals(e.getMessage().contains("You have no Update permission"), true);
      }

      sm.runQuery("grant Update on table " + odpsTable + " to user " + DatahubTestUtils.getDefaultUser(), false);
      try {
        client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
        Assert.assertTrue(false);
      } catch (com.aliyun.datahub.exception.OdpsException e) {
        Assert.assertEquals(e.getMessage().contains("You have no Alter permission"), true);
      }
      sm.runQuery("grant Alter on table " + odpsTable + " to user " + DatahubTestUtils.getDefaultUser(), false);

      try {
        client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
        Assert.assertTrue(false);
      } catch (com.aliyun.datahub.exception.OdpsException e) {
        Assert.assertEquals(e.getMessage().contains("You have no Describe permission"), true);
      }
      sm.runQuery("grant Describe on table " + odpsTable + " to user " + DatahubTestUtils.getDefaultUser(), false);
      client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
    } catch (OdpsException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }
  }

  @Test
  public void testSinkOdpsConnectorNormalAutoPartitionSystemTime() {
    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("ct", OdpsType.STRING));
    CreateTable(odpsSchema);

    int timeRange = 60;
    odpsDesc.setPartitionMode(OdpsDesc.PartitionMode.SYSTEM_TIME);
    odpsDesc.setTimeRange(timeRange);
    Map<String, String> partitionConfig = new LinkedHashMap<String, String>();
    partitionConfig.put("pt", "%Y%m%d");
    partitionConfig.put("ct", "%H%M");
    odpsDesc.setPartitionConfig(partitionConfig);
    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);

    GetDataConnectorResult getDataConnectorResult = client.getDataConnector(projectName, topicName, connectorType);
    Assert.assertEquals(getDataConnectorResult.getColumnFields(), columnFields);

    ConnectorExtInfoJsonDeser.ConnectorExtInfoResult detailRs = GetConnectorExtInfo(projectName, topicName);
    Assert.assertEquals(detailRs.getOffsetStoreType(), "coordinator");
    Assert.assertFalse(detailRs.getSubscriptionId().isEmpty());

    long currentTime = System.currentTimeMillis()/1000;
    currentTime = (currentTime - currentTime%(timeRange*60)) * 1000;
    putRecords(schema);
    try {
      Thread.sleep(shipperWaitTime);
    } catch (InterruptedException e) {

    }
    // check odps data
    try {
      PartitionSpec partitionSpec = new PartitionSpec();
      SimpleDateFormat pt = new SimpleDateFormat("yyyyMMdd");
      partitionSpec.set("pt", pt.format(new Date(currentTime)));
      SimpleDateFormat ct = new SimpleDateFormat("HHmm");
      partitionSpec.set("ct", ct.format(new Date(currentTime)));
      System.out.println(partitionSpec.toString());
      downloadCheckData(recordCount * shardCount, odpsSchema, partitionSpec);
    } catch (IOException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    } catch (TunnelException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }

    getDataConnectorResult = client.getDataConnector(projectName, topicName, connectorType);
    Assert.assertEquals(getDataConnectorResult.getColumnFields(), columnFields);
    for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
      Assert.assertEquals(sc.getCurSequence(), recordCount);
      Assert.assertEquals(GetCurSequenceBySubscriptionId(projectName, topicName,
              detailRs.getSubscriptionId(), sc.getShardId()), sc.getCurSequence());
    }

    Assert.assertTrue(IsSubscriptionExist(projectName, topicName, detailRs.getSubscriptionId()));
    client.deleteDataConnector(projectName, topicName, connectorType);
    Assert.assertFalse(IsSubscriptionExist(projectName, topicName, detailRs.getSubscriptionId()));
  }

  @Test
  public void testSinkOdpsConnectorNormalAutoPartitionEventTime() {
    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("event_time", FieldType.TIMESTAMP));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("ct", OdpsType.STRING));
    CreateTable(odpsSchema);

    int timeRange = 60;
    odpsDesc.setPartitionMode(OdpsDesc.PartitionMode.EVENT_TIME);
    odpsDesc.setTimeRange(timeRange);
    Map<String, String> partitionConfig = new LinkedHashMap<String, String>();
    partitionConfig.put("pt", "%Y%m%d");
    partitionConfig.put("ct", "%H%M");
    odpsDesc.setPartitionConfig(partitionConfig);
    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);

    GetDataConnectorResult getDataConnectorResult = client.getDataConnector(projectName, topicName, connectorType);
    Assert.assertEquals(getDataConnectorResult.getColumnFields(), columnFields);

    ConnectorExtInfoJsonDeser.ConnectorExtInfoResult detailRs = GetConnectorExtInfo(projectName, topicName);
    Assert.assertEquals(detailRs.getOffsetStoreType(), "coordinator");
    Assert.assertFalse(detailRs.getSubscriptionId().isEmpty());

    Random rand = new Random();
    // rand in one day
    long randSecond = rand.nextInt(3600 * 24);
    long currentTime = System.currentTimeMillis()/1000 - randSecond;
    currentTime = (currentTime - currentTime%(timeRange*60)) * 1000;
    putRecordsWithEventTime(schema, currentTime*1000);
    // rand in one day
    long randSecond2 = rand.nextInt(3600 * 24);
    long currentTime2 = System.currentTimeMillis()/1000 - randSecond2;
    currentTime2 = (currentTime2 - currentTime2%(timeRange*60)) * 1000;
    putRecordsWithEventTime(schema, currentTime2*1000);
    try {
      Thread.sleep(shipperWaitTime);
    } catch (InterruptedException e) {

    }
    // check odps data
    try {
      PartitionSpec partitionSpec = new PartitionSpec();
      SimpleDateFormat pt = new SimpleDateFormat("yyyyMMdd");
      partitionSpec.set("pt", pt.format(new Date(currentTime)));
      SimpleDateFormat ct = new SimpleDateFormat("HHmm");
      partitionSpec.set("ct", ct.format(new Date(currentTime)));
      System.out.println(partitionSpec.toString());
      downloadCheckData(recordCount * shardCount, odpsSchema, partitionSpec);
    } catch (IOException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    } catch (TunnelException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }

    try {
      PartitionSpec partitionSpec = new PartitionSpec();
      SimpleDateFormat pt = new SimpleDateFormat("yyyyMMdd");
      partitionSpec.set("pt", pt.format(new Date(currentTime2)));
      SimpleDateFormat ct = new SimpleDateFormat("HHmm");
      partitionSpec.set("ct", ct.format(new Date(currentTime2)));
      System.out.println(partitionSpec.toString());
      downloadCheckData(recordCount * shardCount, odpsSchema, partitionSpec);
    } catch (IOException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    } catch (TunnelException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }

    getDataConnectorResult = client.getDataConnector(projectName, topicName, connectorType);
    Assert.assertEquals(getDataConnectorResult.getColumnFields(), columnFields);
    for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
      Assert.assertEquals(sc.getCurSequence(), recordCount*2);
      Assert.assertEquals(GetCurSequenceBySubscriptionId(projectName, topicName,
              detailRs.getSubscriptionId(), sc.getShardId()), sc.getCurSequence());
    }

    Assert.assertTrue(IsSubscriptionExist(projectName, topicName, detailRs.getSubscriptionId()));
    client.deleteDataConnector(projectName, topicName, connectorType);
    Assert.assertFalse(IsSubscriptionExist(projectName, topicName, detailRs.getSubscriptionId()));
  }

  @Test(expectedExceptions = InvalidParameterException.class)
  public void testSinkOdpsConnectorAutoPartitionSystemTimePartitionColExist() {
    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("pt", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("ct", OdpsType.STRING));
    CreateTable(odpsSchema);

    int timeRange = 60;
    odpsDesc.setPartitionMode(OdpsDesc.PartitionMode.SYSTEM_TIME);
    odpsDesc.setTimeRange(timeRange);
    Map<String, String> partitionConfig = new LinkedHashMap<String, String>();
    partitionConfig.put("pt", "%Y%m%d");
    partitionConfig.put("ct", "%H%M");
    odpsDesc.setPartitionConfig(partitionConfig);
    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
  }

  @Test(expectedExceptions = InvalidParameterException.class)
  public void testSinkOdpsConnectorAutoPartitionSystemTimeNoPartition() {
    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    CreateTable(odpsSchema);

    int timeRange = 60;
    odpsDesc.setPartitionMode(OdpsDesc.PartitionMode.SYSTEM_TIME);
    odpsDesc.setTimeRange(timeRange);

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
  }

  @Test(expectedExceptions = InvalidParameterException.class)
  public void testSinkOdpsConnectorAutoPartitionSystemTimeInvalidRange() {
    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("ct", OdpsType.STRING));
    CreateTable(odpsSchema);

    int timeRange = 5;
    odpsDesc.setPartitionMode(OdpsDesc.PartitionMode.SYSTEM_TIME);
    odpsDesc.setTimeRange(timeRange);
    Map<String, String> partitionConfig = new LinkedHashMap<String, String>();
    partitionConfig.put("pt", "%Y%m%d");
    partitionConfig.put("ct", "%H%M");
    odpsDesc.setPartitionConfig(partitionConfig);
    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
  }

  @Test(expectedExceptions = InvalidParameterException.class)
  public void testSinkOdpsConnectorAutoPartitionEventTimeNotExist() {
    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("ct", OdpsType.STRING));
    CreateTable(odpsSchema);

    int timeRange = 15;
    odpsDesc.setPartitionMode(OdpsDesc.PartitionMode.EVENT_TIME);
    odpsDesc.setTimeRange(timeRange);
    Map<String, String> partitionConfig = new LinkedHashMap<String, String>();
    partitionConfig.put("pt", "%Y%m%d");
    partitionConfig.put("ct", "%H%M");
    odpsDesc.setPartitionConfig(partitionConfig);
    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
  }

  @Test(expectedExceptions = InvalidParameterException.class)
  public void testSinkOdpsConnectorAutoPartitionEventTimeTypeInvalid() {
    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("event_time", FieldType.BIGINT));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("ct", OdpsType.STRING));
    CreateTable(odpsSchema);

    int timeRange = 15;
    odpsDesc.setPartitionMode(OdpsDesc.PartitionMode.EVENT_TIME);
    odpsDesc.setTimeRange(timeRange);
    Map<String, String> partitionConfig = new LinkedHashMap<String, String>();
    partitionConfig.put("pt", "%Y%m%d");
    partitionConfig.put("ct", "%H%M");
    odpsDesc.setPartitionConfig(partitionConfig);
    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
  }

  @Test
  public void testSinkOdpsConnectorNormalAutoPartitionEventTimeAppendField() {
    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    //schema.addField(new Field("event_time", FieldType.TIMESTAMP));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("ct", OdpsType.STRING));
    CreateTable(odpsSchema);

    int timeRange = 60;
    odpsDesc.setPartitionMode(OdpsDesc.PartitionMode.EVENT_TIME);
    odpsDesc.setTimeRange(timeRange);
    Map<String, String> partitionConfig = new LinkedHashMap<String, String>();
    partitionConfig.put("pt", "%Y%m%d");
    partitionConfig.put("ct", "%H%M");
    odpsDesc.setPartitionConfig(partitionConfig);

    long currentSystemTime = System.currentTimeMillis()/1000;
    currentSystemTime = (currentSystemTime - currentSystemTime%(timeRange*60)) * 1000;

    putRecords(schema);
    schema.addField(new Field("event_time", FieldType.TIMESTAMP));
    client.appendField(new AppendFieldRequest(projectName, topicName, new Field("event_time", FieldType.TIMESTAMP)));
    Random rand = new Random();
    // rand in one day
    long randSecond = rand.nextInt(3600 * 24);
    long currentTime = System.currentTimeMillis()/1000 - randSecond;
    currentTime = (currentTime - currentTime%(timeRange*60)) * 1000;
    putRecordsWithEventTime(schema, currentTime*1000);

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);
    GetDataConnectorResult getDataConnectorResult = client.getDataConnector(projectName, topicName, connectorType);
    Assert.assertEquals(getDataConnectorResult.getColumnFields(), columnFields);

    ConnectorExtInfoJsonDeser.ConnectorExtInfoResult detailRs = GetConnectorExtInfo(projectName, topicName);
    Assert.assertEquals(detailRs.getOffsetStoreType(), "coordinator");
    Assert.assertFalse(detailRs.getSubscriptionId().isEmpty());

    try {
      Thread.sleep(shipperWaitTime);
    } catch (InterruptedException e) {

    }
    // ����Ϊsystem time
    try {
      PartitionSpec partitionSpec = new PartitionSpec();
      SimpleDateFormat pt = new SimpleDateFormat("yyyyMMdd");
      partitionSpec.set("pt", pt.format(new Date(currentSystemTime)));
      SimpleDateFormat ct = new SimpleDateFormat("HHmm");
      partitionSpec.set("ct", ct.format(new Date(currentSystemTime)));
      System.out.println(partitionSpec.toString());
      downloadCheckData(recordCount * shardCount, odpsSchema, partitionSpec);
    } catch (IOException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    } catch (TunnelException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }

    try {
      PartitionSpec partitionSpec = new PartitionSpec();
      SimpleDateFormat pt = new SimpleDateFormat("yyyyMMdd");
      partitionSpec.set("pt", pt.format(new Date(currentTime)));
      SimpleDateFormat ct = new SimpleDateFormat("HHmm");
      partitionSpec.set("ct", ct.format(new Date(currentTime)));
      System.out.println(partitionSpec.toString());
      downloadCheckData(recordCount * shardCount, odpsSchema, partitionSpec);
    } catch (IOException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    } catch (TunnelException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }

    getDataConnectorResult = client.getDataConnector(projectName, topicName, connectorType);
    Assert.assertEquals(getDataConnectorResult.getColumnFields(), columnFields);
    for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
      Assert.assertEquals(sc.getCurSequence(), recordCount*2);
      Assert.assertEquals(GetCurSequenceBySubscriptionId(projectName, topicName,
              detailRs.getSubscriptionId(), sc.getShardId()), sc.getCurSequence());
    }

    Assert.assertTrue(IsSubscriptionExist(projectName, topicName, detailRs.getSubscriptionId()));
    client.deleteDataConnector(projectName, topicName, connectorType);
    Assert.assertFalse(IsSubscriptionExist(projectName, topicName, detailRs.getSubscriptionId()));
  }

  @Test
  public void testSinkOdpsDeleteTopicWithConnector() {
    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    CreateTable(odpsSchema);

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);

    ConnectorExtInfoJsonDeser.ConnectorExtInfoResult detailRs = GetConnectorExtInfo(projectName, topicName);
    Assert.assertEquals(detailRs.getOffsetStoreType(), "coordinator");
    Assert.assertFalse(detailRs.getSubscriptionId().isEmpty());

    try {
      client.deleteTopic(projectName, topicName);
      Assert.assertTrue(false);
    } catch (OperationDeniedException e) {
      e.printStackTrace();
      Assert.assertTrue(true);
    }

    Assert.assertTrue(IsSubscriptionExist(projectName, topicName, detailRs.getSubscriptionId()));
    client.deleteDataConnector(projectName, topicName, connectorType);
    Assert.assertFalse(IsSubscriptionExist(projectName, topicName, detailRs.getSubscriptionId()));

    client.deleteTopic(projectName, topicName);
  }

  @Test
  public void testMarkDoneSystemTime() {
    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("ct", OdpsType.STRING));
    CreateTable(odpsSchema);

    int timeRange = 60;
    odpsDesc.setPartitionMode(OdpsDesc.PartitionMode.SYSTEM_TIME);
    odpsDesc.setTimeRange(timeRange);
    Map<String, String> partitionConfig = new LinkedHashMap<String, String>();
    partitionConfig.put("pt", "%Y%m%d");
    partitionConfig.put("ct", "%H%M");
    odpsDesc.setPartitionConfig(partitionConfig);
    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);

    GetDataConnectorResult getDataConnectorResult = client.getDataConnector(projectName, topicName, connectorType);
    Assert.assertEquals(getDataConnectorResult.getColumnFields(), columnFields);

    Assert.assertEquals(client.getDataConnectorDoneTime(
        new GetDataConnectorDoneTimeRequest(projectName, topicName, connectorType)).getDoneTime().longValue(), 0L);

    ConnectorExtInfoJsonDeser.ConnectorExtInfoResult detailRs = GetConnectorExtInfo(projectName, topicName);
    Assert.assertEquals(detailRs.getOffsetStoreType(), "coordinator");
    Assert.assertFalse(detailRs.getSubscriptionId().isEmpty());

    long currentTime = System.currentTimeMillis()/1000;
    putRecords(schema);
    try {
      Thread.sleep(shipperWaitTime);
    } catch (InterruptedException e) {

    }
    long doneTime = client.getDataConnectorDoneTime(
        new GetDataConnectorDoneTimeRequest(projectName, topicName, connectorType)).getDoneTime().longValue();
    Assert.assertTrue(doneTime > currentTime - 1 && doneTime < currentTime + 1);

    putRecords(schema);
    doneTime = client.getDataConnectorDoneTime(
        new GetDataConnectorDoneTimeRequest(projectName, topicName, connectorType)).getDoneTime().longValue();
    Assert.assertTrue(doneTime > currentTime - 1 && doneTime < currentTime + 1);
    currentTime = System.currentTimeMillis()/1000;
    try {
      Thread.sleep(shipperWaitTime);
    } catch (InterruptedException e) {

    }
    doneTime = client.getDataConnectorDoneTime(
        new GetDataConnectorDoneTimeRequest(projectName, topicName, connectorType)).getDoneTime().longValue();
    Assert.assertTrue(doneTime > currentTime - 1 && doneTime < currentTime + 1);

    long timestamp = Long.MAX_VALUE;
    for (int i = 0; i < shardCount; i++) {
      OffsetContext.Offset offset = GetOffset(projectName, topicName,
              detailRs.getSubscriptionId(), String.valueOf(i));
      if (offset.getTimestamp() < timestamp)
        timestamp = offset.getTimestamp();
    }
    Assert.assertEquals(timestamp/1000, doneTime);

    Assert.assertTrue(IsSubscriptionExist(projectName, topicName, detailRs.getSubscriptionId()));
    client.deleteDataConnector(projectName, topicName, connectorType);
    Assert.assertFalse(IsSubscriptionExist(projectName, topicName, detailRs.getSubscriptionId()));
  }

  @Test
  public void testMarkDoneEventTime() {
    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("event_time", FieldType.TIMESTAMP));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("dt", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("hh", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("mm", OdpsType.STRING));
    CreateTable(odpsSchema);

    int timeRange = 60;
    odpsDesc.setPartitionMode(OdpsDesc.PartitionMode.EVENT_TIME);
    odpsDesc.setTimeRange(timeRange);
    Map<String, String> partitionConfig = new LinkedHashMap<String, String>();
    partitionConfig.put("dt", "%Y%m%d");
    partitionConfig.put("hh", "%H");
    partitionConfig.put("mm", "%M");
    odpsDesc.setPartitionConfig(partitionConfig);
    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);

    ConnectorExtInfoJsonDeser.ConnectorExtInfoResult detailRs = GetConnectorExtInfo(projectName, topicName);
    Assert.assertEquals(detailRs.getOffsetStoreType(), "coordinator");
    Assert.assertFalse(detailRs.getSubscriptionId().isEmpty());

    Random rand = new Random();
    long eventTime = System.currentTimeMillis()/1000 + rand.nextInt(900);
    putRecordsWithEventTime(schema, eventTime*1000*1000);
    try {
      Thread.sleep(shipperWaitTime);
    } catch (InterruptedException e) {

    }
    long doneTime = client.getDataConnectorDoneTime(
        new GetDataConnectorDoneTimeRequest(projectName, topicName, connectorType)).getDoneTime().longValue();
    Assert.assertEquals(doneTime, eventTime - eventTime%15 + 15);

    long timestamp = Long.MAX_VALUE;
    for (int i = 0; i < shardCount; i++) {
      OffsetContext.Offset offset = GetOffset(projectName, topicName,
              detailRs.getSubscriptionId(), String.valueOf(i));
      if (offset.getTimestamp() < timestamp)
        timestamp = offset.getTimestamp();
    }
    Assert.assertEquals(timestamp/1000, doneTime);
  }

  @Test
  public void testSinkOdpsGetDiscardCount() {
    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("pt");

    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("pt", FieldType.STRING));
    CreateTopic(schema);

    TableSchema odpsSchema = new TableSchema();
    odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
    odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    CreateTable(odpsSchema);

    client.createDataConnector(projectName, topicName, connectorType, columnFields, odpsDesc);

    ConnectorExtInfoJsonDeser.ConnectorExtInfoResult detailRs = GetConnectorExtInfo(projectName, topicName);
    Assert.assertEquals(detailRs.getOffsetStoreType(), "coordinator");
    Assert.assertFalse(detailRs.getSubscriptionId().isEmpty());

    ListShardResult shards = client.listShard(projectName, topicName);
    for (ShardEntry shard : shards.getShards()) {
      List<RecordEntry> recordEntries = new ArrayList<RecordEntry>();
      for (long n = 0; n < recordCount; n++) {
        //RecordData
        RecordEntry entry = new RecordEntry(schema);

        entry.setString("f1", "test1");
        if (n < 10) {
          // pt == null
          //entry.setString("pt", "pt");
        } else {
          entry.setString("pt", "pt");
        }
        entry.setShardId(shard.getShardId());

        recordEntries.add(entry);
      }
      PutRecordsResult result = client.putRecords(projectName, topicName, recordEntries);
      if (result.getFailedRecordCount() != 0) {
        System.out.println(result.getFailedRecordError().get(0).getMessage());
      }
      Assert.assertEquals(result.getFailedRecordCount(), 0);
    }

    try {
      Thread.sleep(shipperWaitTime);
    } catch (InterruptedException e) {

    }
    for (ShardEntry shard : shards.getShards()) {
      GetDataConnectorShardStatusResult getDataConnectorShardStatusResult =
          client.getDataConnectorShardStatus(projectName, topicName, connectorType, shard.getShardId());
      Assert.assertEquals(getDataConnectorShardStatusResult.getDiscardCount(), 10);

    }

    Assert.assertTrue(IsSubscriptionExist(projectName, topicName, detailRs.getSubscriptionId()));
    client.deleteDataConnector(projectName, topicName, connectorType);
    Assert.assertFalse(IsSubscriptionExist(projectName, topicName, detailRs.getSubscriptionId()));
  }
}
