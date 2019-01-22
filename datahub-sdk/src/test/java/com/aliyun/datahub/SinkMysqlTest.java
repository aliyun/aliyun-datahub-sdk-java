package com.aliyun.datahub;

import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.common.data.FieldType;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.common.data.RecordType;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.exception.InvalidParameterException;
import com.aliyun.datahub.model.*;
import com.aliyun.datahub.util.DatahubTestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.*;
import java.util.*;

public class SinkMysqlTest {
  // datahub config
  private String projectName = null;
  private String topicName = null;
  private ConnectorType connectoType = null;
  private DatahubClient client = null;
  // DB config
  private DatabaseDesc desc = null;
  private String driver = "com.mysql.jdbc.Driver";
  private Connection dbConn = null;

  private Random random = null;
  private int shardCount = 3;
  private long timestamp = System.currentTimeMillis();
  private long recordCount = 100;
  private long sleepTime = 35000; // commit interval 30s

  // dataSet
  private Map<String, Long> pk2Version = new HashMap<String, Long>();
  private Map<String, DBRecord> pk2Data = new HashMap<String, DBRecord>();

  class DBRecord {
    Map<String, String> values;

    public DBRecord() {
      values = new HashMap<String, String>();
    }
  }

  @BeforeMethod
  public void SetUp() {
    try {
      client = new DatahubClient(DatahubTestUtils.getConf());
      random = new Random(System.currentTimeMillis());
      String hint = Long.toHexString(random.nextLong());

      System.out.println("Hint is " + hint);
      System.out.println("Timestamp is " + timestamp);

      projectName = DatahubTestUtils.getProjectName();
      topicName = String.format("ut_test_topic_%s", hint);
      System.out.println("Topic Name is " + topicName);

      String tableName = String.format("ut_test_table_%s", hint);
      System.out.println("Table Name is " + tableName);

      desc = new DatabaseDesc();
      desc.setHost(DatahubTestUtils.getMysqlHost());
      desc.setPort(Integer.valueOf(DatahubTestUtils.getMysqlPort()));
      desc.setDatabase(DatahubTestUtils.getMysqlDatabase());
      desc.setUser(DatahubTestUtils.getMysqlUser());
      desc.setPassword(DatahubTestUtils.getMysqlPassword());
      desc.setTable(tableName);
      pk2Version.clear();
      pk2Data.clear();
      connectoType = ConnectorType.SINK_MYSQL;

      dbConn = openConnection();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @AfterMethod
  public void TearDown() {
    try {
      client.deleteDataConnector(projectName, topicName, connectoType);
    } catch (DatahubClientException e) {

    }
    try {
      client.deleteTopic(projectName, topicName);
    } catch (DatahubClientException e) {

    }
    try {
      DeleteTable();
    } catch (Exception e) {

    }
    try {
      dbConn.close();
    } catch (SQLException e) {

    }
  }

  private Connection openConnection() {
    try {
      // ������������
      Class.forName(driver);
      Connection conn = DriverManager.getConnection("jdbc:mysql://" + desc.getHost() + ":" + desc.getPort().toString() + "/" + desc.getDatabase() + "?useUnicode=true&characterEncoding=UTF-8",
          desc.getUser(), desc.getPassword());
      if (conn.isClosed()) {
        System.out.println("Failed connector to the Database!");
        Assert.assertTrue(false);
      }
      return conn;
    } catch (SQLException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }
    return null;
  }

  private ResultSet select(Connection conn, String sql) {
    try {
      PreparedStatement statement = conn.prepareStatement(sql);
      return statement.executeQuery();
    } catch (SQLException e) {
      throw new DatahubClientException("Execute sql failed :" + sql + " error:" + e.getMessage());
    }
  }

  private void execute(Connection conn, String sql) {
    try {
      System.out.println(sql);
      PreparedStatement statement = conn.prepareStatement(sql);
      statement.execute();
    } catch (SQLException e) {
      throw new DatahubClientException("Execute sql failed :" + sql + " error:" + e.getMessage());
    }
  }

  private void DeleteTable() {
    String sql = "drop table " + desc.getTable();
    execute(dbConn, sql);
  }

  private void CreateTable(RecordSchema schema, boolean hasPK) {
    String sql = "create table " + desc.getTable() + " (";
    for (Field field :schema.getFields()) {
      //hack for sink timestamp to bigint
      if (field.getType() == FieldType.TIMESTAMP && field.getNotnull()) {
        sql += field.getName() + " bigint, ";
      } else if (field.getType() != FieldType.STRING) {
        sql += field.getName() + " " + field.getType().toString() + ", ";
      } else {
        sql += field.getName() + " varchar(255), ";
      }
    }
    if (hasPK) {
      sql += "primary key (pk))";
    } else {
      sql = sql.substring(0, sql.length() - 2) + ")";
    }
    execute(dbConn, sql);
  }

  private void CreateTopic() {
    RecordType type = RecordType.BLOB;
    client.createTopic(projectName, topicName, shardCount, 3, type, "");
    client.waitForShardReady(projectName, topicName);
  }

  private void CreateTopic(RecordSchema schema) {
    RecordType type = RecordType.TUPLE;
    client.createTopic(projectName, topicName, shardCount, 3, type, schema, "");
    client.waitForShardReady(projectName, topicName);
  }

  private void CreateTopic(RecordSchema schema, int shard) {
    RecordType type = RecordType.TUPLE;
    client.createTopic(projectName, topicName, shard, 3, type, schema, "");
    client.waitForShardReady(projectName, topicName);
  }

  private String genPK() {
    // rand from [0, recordCount/10]
    String id = String.valueOf(random.nextInt((int) (recordCount * shardCount)));
    if (pk2Version.containsKey(id)) {
      Long count = pk2Version.get(id);
      count += 1;
      pk2Version.put(id, count);
    } else {
      pk2Version.put(id, 1L);
    }

    return String.valueOf(id);
  }

  private RecordEntry genData(RecordSchema schema, List<String> ignoreList) {
    RecordEntry entry = new RecordEntry(schema);
    DBRecord dbRecord = new DBRecord();
    for (Field i : schema.getFields()) {
      if (i.getName().equals("pk")) {
        entry.setString("pk", genPK());
        dbRecord.values.put(i.getName(), entry.getString("pk"));
      } else if (ignoreList.contains(i.getName())) {
        dbRecord.values.put(i.getName(), null);
      } else{
        switch (i.getType()) {
          case STRING:
            entry.setString(i.getName(), "test\"test");
            dbRecord.values.put(i.getName(), entry.getString(i.getName()));
            break;
          case BOOLEAN:
            entry.setBoolean(i.getName(), random.nextBoolean());
            dbRecord.values.put(i.getName(), entry.getBoolean(i.getName()) ? "1" : "0");
            break;
          case DOUBLE:
            entry.setDouble(i.getName(), Double.valueOf(random.nextInt(100)/100.0));
            dbRecord.values.put(i.getName(), String.valueOf(entry.getDouble(i.getName())));
            break;
          case TIMESTAMP:
            entry.setTimeStamp(i.getName(), timestamp * 1000);
            dbRecord.values.put(i.getName(), entry.getTimeStamp(i.getName()).toString());
            break;
          case BIGINT:
            entry.setBigint(i.getName(), random.nextLong());
            dbRecord.values.put(i.getName(), entry.getBigint(i.getName()).toString());
            break;
          default:
            Assert.assertTrue(false);
        }
      }
    }
    entry.setPartitionKey(entry.getString("pk"));
    if (desc.isIgnore()) {
      if (!pk2Data.containsKey(entry.getString("pk"))) {
        pk2Data.put(entry.getString("pk"), dbRecord);
      }
      // else ignore, store first version
    } else {
      pk2Data.put(entry.getString("pk"), dbRecord);
    }

    return entry;
  }

  private RecordEntry genData(RecordSchema schema) {
    return genData(schema, new ArrayList<String>());
  }

  private void putRecords(RecordSchema schema, List<String> ignoreList) {
    ListShardResult shards = client.listShard(projectName, topicName);
    for (ShardEntry shard : shards.getShards()) {
      List<RecordEntry> recordEntries = new ArrayList<RecordEntry>();
      for (long n = 0; n < recordCount; n++) {
        //RecordData
        RecordEntry entry = genData(schema, ignoreList);

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
    putRecords(schema, new ArrayList<String>());
  }

  private void checkDBRecordCount(Long count) {
    String sql = "select count(1) as c from " + desc.getTable();
    try {
      ResultSet rs = select(dbConn, sql);
      if (rs.next()) {
        Long countDB = rs.getLong("c");
        Assert.assertEquals(countDB, count);
      } else {
        Assert.assertTrue(false);
      }
      rs.close();
    } catch (SQLException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }
  }

  private void checkDBRecordValue(RecordSchema schema) {
    String sql = "select * from " + desc.getTable();
    try {
      ResultSet rs = select(dbConn, sql);
      while (rs.next()) {
        String pk = rs.getString("pk");
        DBRecord dbRecord = pk2Data.get(pk);
        for (Map.Entry<String, String> entry : dbRecord.values.entrySet()) {
          if (!schema.containsField(entry.getKey())) {
            continue;
          }
          if (entry.getValue() == null) {
            Assert.assertEquals(rs.getString(entry.getKey()), null);
          }
          if (schema.getField(entry.getKey()).getType() == FieldType.TIMESTAMP) {
            if (schema.getField(entry.getKey()).getNotnull()) {
              Assert.assertEquals(Long.valueOf(entry.getValue()), Long.valueOf(rs.getString(entry.getKey())));
            } else {
              Assert.assertEquals(Long.valueOf(entry.getValue())/ 1000000, rs.getTimestamp(entry.getKey()).getTime() / 1000);
            }
          } else if (schema.getField(entry.getKey()).getType() == FieldType.DOUBLE) {
            Assert.assertEquals(Double.valueOf(entry.getValue()), Double.valueOf(rs.getString(entry.getKey())));
          } else if (schema.getField(entry.getKey()).getType() == FieldType.BIGINT) {
            Assert.assertEquals(Long.valueOf(entry.getValue()), Long.valueOf(rs.getString(entry.getKey())));
          } else {
            Assert.assertEquals(entry.getValue(), rs.getString(entry.getKey()));
          }
        }
      }
      rs.close();
    } catch (SQLException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }
  }

  @Test
  public void testSinkMysqlConnectorNormal() {
    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("f2", FieldType.BIGINT));
    schema.addField(new Field("f3", FieldType.DOUBLE));
    schema.addField(new Field("f4", FieldType.TIMESTAMP));
    schema.addField(new Field("f5", FieldType.BOOLEAN));
    schema.addField(new Field("f6", FieldType.TIMESTAMP, true));
    schema.addField(new Field("pk", FieldType.STRING));
    CreateTopic(schema);

    CreateTable(schema, true);
    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("f2");
    columnFields.add("f3");
    columnFields.add("f4");
    columnFields.add("f5");
    columnFields.add("f6");
    columnFields.add("pk");

    client.createDataConnector(projectName, topicName, connectoType, columnFields, desc);

    ListDataConnectorResult listDataConnectorResult = client.listDataConnector(projectName, topicName);
    List<String> connectors = listDataConnectorResult.getDataConnectors();
    Assert.assertTrue(connectors.contains(connectoType.toString().toLowerCase()));

    GetDataConnectorResult getDataConnectorResult = client.getDataConnector(projectName, topicName, connectoType);
    Assert.assertEquals(getDataConnectorResult.getColumnFields(), columnFields);
    DatabaseDesc dbDesc = (DatabaseDesc)getDataConnectorResult.getConnectorConfig();
    Assert.assertEquals(dbDesc.getDatabase(), desc.getDatabase());
    Assert.assertEquals(dbDesc.getHost(), desc.getHost());
    Assert.assertEquals(dbDesc.getPort(), desc.getPort());
    Assert.assertEquals(dbDesc.getTable(), desc.getTable());

    putRecords(schema);
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException e) {

    }
    // check db data
    long total = shardCount*recordCount;
    long commit_count = 0;
    getDataConnectorResult = client.getDataConnector(projectName, topicName, connectoType);
    for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
      commit_count += sc.getCurSequence();
    }
    Assert.assertEquals(commit_count, total);

    checkDBRecordCount(Long.valueOf(pk2Version.size()));
    checkDBRecordValue(schema);
  }

  @Test
  public void testSinkMysqlConnectorNormalWithOutPK() {
    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("f2", FieldType.BIGINT));
    schema.addField(new Field("f3", FieldType.DOUBLE));
    schema.addField(new Field("f4", FieldType.TIMESTAMP));
    schema.addField(new Field("f5", FieldType.BOOLEAN));
    schema.addField(new Field("pk", FieldType.STRING));
    CreateTopic(schema);

    CreateTable(schema, false);
    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("f2");
    columnFields.add("f3");
    columnFields.add("f4");
    columnFields.add("f5");
    columnFields.add("pk");

    client.createDataConnector(projectName, topicName, connectoType, columnFields, desc);

    ListDataConnectorResult listDataConnectorResult = client.listDataConnector(projectName, topicName);
    List<String> connectors = listDataConnectorResult.getDataConnectors();
    Assert.assertTrue(connectors.contains(connectoType.toString().toLowerCase()));

    GetDataConnectorResult getDataConnectorResult = client.getDataConnector(projectName, topicName, connectoType);
    Assert.assertEquals(getDataConnectorResult.getColumnFields(), columnFields);

    putRecords(schema);
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException e) {

    }
    // check db data
    long total = shardCount*recordCount;
    long commit_count = 0;
    getDataConnectorResult = client.getDataConnector(projectName, topicName, connectoType);
    for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
      commit_count += sc.getCurSequence();
    }
    Assert.assertEquals(commit_count, total);

    checkDBRecordCount(total);
  }

  @Test(expectedExceptions = InvalidParameterException.class)
  public void testSinkMysqlConnectorSelectColSizeNotMatch() {
    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("f2", FieldType.BIGINT));
    schema.addField(new Field("f3", FieldType.DOUBLE));
    schema.addField(new Field("f4", FieldType.TIMESTAMP));
    schema.addField(new Field("f5", FieldType.BOOLEAN));
    schema.addField(new Field("pk", FieldType.STRING));
    CreateTopic(schema);

    CreateTable(schema, false);
    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("f2");
    columnFields.add("f3");
    columnFields.add("f4");
    columnFields.add("f5");

    client.createDataConnector(projectName, topicName, connectoType, columnFields, desc);
  }

  @Test(expectedExceptions = InvalidParameterException.class)
  public void testSinkMysqlConnectorTypeNotMatch() {
    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("f2", FieldType.BIGINT));
    schema.addField(new Field("f3", FieldType.DOUBLE));
    schema.addField(new Field("f4", FieldType.TIMESTAMP));
    schema.addField(new Field("f5", FieldType.BOOLEAN));
    schema.addField(new Field("pk", FieldType.STRING));
    CreateTopic(schema);

    RecordSchema schema1 = new RecordSchema();
    schema1.addField(new Field("f1", FieldType.STRING));
    schema1.addField(new Field("f2", FieldType.BIGINT));
    schema1.addField(new Field("f3", FieldType.DOUBLE));
    schema1.addField(new Field("f4", FieldType.TIMESTAMP));
    schema1.addField(new Field("f5", FieldType.BOOLEAN));
    schema1.addField(new Field("pk", FieldType.BIGINT));
    CreateTable(schema1, false);

    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("f2");
    columnFields.add("f3");
    columnFields.add("f4");
    columnFields.add("f5");
    columnFields.add("pk");
    client.createDataConnector(projectName, topicName, connectoType, columnFields, desc);
  }

  @Test(expectedExceptions = InvalidParameterException.class)
  public void testSinkMysqlConnectorNameNotMatch() {
    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("f2", FieldType.BIGINT));
    schema.addField(new Field("f3", FieldType.DOUBLE));
    schema.addField(new Field("f4", FieldType.TIMESTAMP));
    schema.addField(new Field("f5", FieldType.BOOLEAN));
    schema.addField(new Field("pk", FieldType.STRING));
    CreateTopic(schema);

    RecordSchema schema1 = new RecordSchema();
    schema1.addField(new Field("f1", FieldType.STRING));
    schema1.addField(new Field("f2", FieldType.BIGINT));
    schema1.addField(new Field("f3", FieldType.DOUBLE));
    schema1.addField(new Field("f4", FieldType.TIMESTAMP));
    schema1.addField(new Field("f6", FieldType.BOOLEAN));
    schema1.addField(new Field("pk", FieldType.STRING));
    CreateTable(schema1, false);

    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("f2");
    columnFields.add("f3");
    columnFields.add("f4");
    columnFields.add("f5");
    columnFields.add("pk");
    client.createDataConnector(projectName, topicName, connectoType, columnFields, desc);
  }

  @Test(expectedExceptions = InvalidParameterException.class)
  public void testSinkMysqlConnectorOrderNotMatch() {
    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("f2", FieldType.BIGINT));
    schema.addField(new Field("f3", FieldType.DOUBLE));
    schema.addField(new Field("f4", FieldType.TIMESTAMP));
    schema.addField(new Field("f5", FieldType.BOOLEAN));
    schema.addField(new Field("pk", FieldType.STRING));
    CreateTopic(schema);

    RecordSchema schema1 = new RecordSchema();
    schema1.addField(new Field("f1", FieldType.STRING));
    schema1.addField(new Field("f2", FieldType.BIGINT));
    schema1.addField(new Field("f3", FieldType.DOUBLE));
    schema1.addField(new Field("f4", FieldType.TIMESTAMP));
    schema1.addField(new Field("f5", FieldType.BOOLEAN));
    schema1.addField(new Field("pk", FieldType.STRING));
    CreateTable(schema1, false);

    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("f2");
    columnFields.add("f3");
    columnFields.add("f5");
    columnFields.add("f4");
    columnFields.add("pk");
    client.createDataConnector(projectName, topicName, connectoType, columnFields, desc);
  }

  @Test
  public void testSinkMysqlConnectorSelectOrder() {
    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("pk", FieldType.STRING));
    schema.addField(new Field("f2", FieldType.BIGINT));
    schema.addField(new Field("f3", FieldType.DOUBLE));
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("f5", FieldType.BOOLEAN));
    schema.addField(new Field("f4", FieldType.TIMESTAMP));
    CreateTopic(schema);

    RecordSchema schema1 = new RecordSchema();
    schema1.addField(new Field("f1", FieldType.STRING));
    schema1.addField(new Field("pk", FieldType.STRING));
    schema1.addField(new Field("f2", FieldType.BIGINT));
    schema1.addField(new Field("f3", FieldType.DOUBLE));
    schema1.addField(new Field("f4", FieldType.TIMESTAMP));
    schema1.addField(new Field("f5", FieldType.BOOLEAN));

    CreateTable(schema1, true);

    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("pk");
    columnFields.add("f2");
    columnFields.add("f3");
    columnFields.add("f4");
    columnFields.add("f5");
    client.createDataConnector(projectName, topicName, connectoType, columnFields, desc);

    putRecords(schema);
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException e) {

    }
    // check db data
    long total = shardCount*recordCount;
    long commit_count = 0;
    GetDataConnectorResult getDataConnectorResult = client.getDataConnector(projectName, topicName, connectoType);
    for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
      commit_count += sc.getCurSequence();
    }
    Assert.assertEquals(commit_count, total);
    checkDBRecordCount(Long.valueOf(pk2Version.size()));
    checkDBRecordValue(schema);
  }

  @Test(expectedExceptions = InvalidParameterException.class)
  public void testSinkMysqlConnectorMoreCol() {
    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("f2", FieldType.BIGINT));
    schema.addField(new Field("f3", FieldType.DOUBLE));
    schema.addField(new Field("f4", FieldType.TIMESTAMP));
    schema.addField(new Field("f5", FieldType.BOOLEAN));
    schema.addField(new Field("pk", FieldType.STRING));
    CreateTopic(schema);

    RecordSchema schema1 = new RecordSchema();
    schema1.addField(new Field("f1", FieldType.STRING));
    schema1.addField(new Field("f2", FieldType.BIGINT));
    schema1.addField(new Field("f3", FieldType.DOUBLE));
    schema1.addField(new Field("f4", FieldType.TIMESTAMP));
    schema1.addField(new Field("pk", FieldType.STRING));
    CreateTable(schema1, false);

    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("f2");
    columnFields.add("f3");
    columnFields.add("f4");
    columnFields.add("f5");
    columnFields.add("pk");
    client.createDataConnector(projectName, topicName, connectoType, columnFields, desc);
  }

  @Test
  public void testSinkMysqlConnectorSelectCol() {
    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("pk", FieldType.STRING));
    schema.addField(new Field("f2", FieldType.BIGINT));
    schema.addField(new Field("f3", FieldType.DOUBLE));
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("f5", FieldType.BOOLEAN));
    schema.addField(new Field("f4", FieldType.TIMESTAMP));
    schema.addField(new Field("f6", FieldType.TIMESTAMP));
    schema.addField(new Field("f7", FieldType.TIMESTAMP));
    CreateTopic(schema);

    RecordSchema schema1 = new RecordSchema();
    schema1.addField(new Field("f1", FieldType.STRING));
    schema1.addField(new Field("pk", FieldType.STRING));
    schema1.addField(new Field("f2", FieldType.BIGINT));
    schema1.addField(new Field("f3", FieldType.DOUBLE));
    schema1.addField(new Field("f4", FieldType.TIMESTAMP));
    schema1.addField(new Field("f5", FieldType.BOOLEAN));

    CreateTable(schema1, true);

    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("pk");
    columnFields.add("f2");
    columnFields.add("f3");
    columnFields.add("f4");
    columnFields.add("f5");
    client.createDataConnector(projectName, topicName, connectoType, columnFields, desc);

    putRecords(schema);
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException e) {

    }
    // check db data
    long total = shardCount*recordCount;
    long commit_count = 0;
    GetDataConnectorResult getDataConnectorResult = client.getDataConnector(projectName, topicName, connectoType);
    for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
      commit_count += sc.getCurSequence();
    }
    Assert.assertEquals(commit_count, total);
    checkDBRecordCount(Long.valueOf(pk2Version.size()));
    checkDBRecordValue(schema1);
  }

  @Test
  public void testSinkMysqlConnectorNormalReplaceByPKNotIgnore() {
    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("f2", FieldType.BIGINT));
    schema.addField(new Field("f3", FieldType.DOUBLE));
    schema.addField(new Field("f4", FieldType.TIMESTAMP));
    schema.addField(new Field("f5", FieldType.BOOLEAN));
    schema.addField(new Field("pk", FieldType.STRING));
    CreateTopic(schema);

    CreateTable(schema, true);
    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("f2");
    columnFields.add("f3");
    columnFields.add("f4");
    columnFields.add("f5");
    columnFields.add("pk");

    desc.setIgnore(false);
    client.createDataConnector(projectName, topicName, connectoType, columnFields, desc);

    ListDataConnectorResult listDataConnectorResult = client.listDataConnector(projectName, topicName);
    List<String> connectors = listDataConnectorResult.getDataConnectors();
    Assert.assertTrue(connectors.contains(connectoType.toString().toLowerCase()));

    GetDataConnectorResult getDataConnectorResult = client.getDataConnector(projectName, topicName, connectoType);
    Assert.assertEquals(getDataConnectorResult.getColumnFields(), columnFields);
    DatabaseDesc dbDesc = (DatabaseDesc)getDataConnectorResult.getConnectorConfig();
    Assert.assertEquals(dbDesc.getDatabase(), desc.getDatabase());
    Assert.assertEquals(dbDesc.getHost(), desc.getHost());
    Assert.assertEquals(dbDesc.getPort(), desc.getPort());
    Assert.assertEquals(dbDesc.getTable(), desc.getTable());

    putRecords(schema);
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException e) {

    }
    // check db data
    long total = shardCount*recordCount;
    long commit_count = 0;
    getDataConnectorResult = client.getDataConnector(projectName, topicName, connectoType);
    for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
      commit_count += sc.getCurSequence();
    }
    Assert.assertEquals(commit_count, total);

    checkDBRecordCount(Long.valueOf(pk2Version.size()));
    checkDBRecordValue(schema);
  }

  @Test(expectedExceptions = InvalidParameterException.class)
  public void testSinkMysqlConnectorBlobTopic() {
    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("f2", FieldType.BIGINT));
    schema.addField(new Field("f3", FieldType.DOUBLE));
    schema.addField(new Field("f4", FieldType.TIMESTAMP));
    schema.addField(new Field("f5", FieldType.BOOLEAN));
    schema.addField(new Field("pk", FieldType.STRING));
    CreateTopic();

    CreateTable(schema, true);
    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("f2");
    columnFields.add("f3");
    columnFields.add("f4");
    columnFields.add("f5");
    columnFields.add("pk");

    desc.setIgnore(false);
    client.createDataConnector(projectName, topicName, connectoType, columnFields, desc);
  }

  @Test
  public void testSinkMysqlConnectorNormalWithNull() {
    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("f2", FieldType.BIGINT));
    schema.addField(new Field("f3", FieldType.DOUBLE));
    schema.addField(new Field("f4", FieldType.TIMESTAMP));
    schema.addField(new Field("f5", FieldType.BOOLEAN));
    schema.addField(new Field("pk", FieldType.STRING));
    CreateTopic(schema);

    CreateTable(schema, true);
    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("f2");
    columnFields.add("f3");
    columnFields.add("f4");
    columnFields.add("f5");
    columnFields.add("pk");

    client.createDataConnector(projectName, topicName, connectoType, columnFields, desc);

    ListDataConnectorResult listDataConnectorResult = client.listDataConnector(projectName, topicName);
    List<String> connectors = listDataConnectorResult.getDataConnectors();
    Assert.assertTrue(connectors.contains(connectoType.toString().toLowerCase()));

    GetDataConnectorResult getDataConnectorResult = client.getDataConnector(projectName, topicName, connectoType);
    Assert.assertEquals(getDataConnectorResult.getColumnFields(), columnFields);
    DatabaseDesc dbDesc = (DatabaseDesc)getDataConnectorResult.getConnectorConfig();
    Assert.assertEquals(dbDesc.getDatabase(), desc.getDatabase());
    Assert.assertEquals(dbDesc.getHost(), desc.getHost());
    Assert.assertEquals(dbDesc.getPort(), desc.getPort());
    Assert.assertEquals(dbDesc.getTable(), desc.getTable());

    List<String> ignore = new ArrayList<String>();
    ignore.add("f1");
    putRecords(schema, ignore);
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException e) {

    }
    // check db data
    long total = shardCount*recordCount;
    long commit_count = 0;
    getDataConnectorResult = client.getDataConnector(projectName, topicName, connectoType);
    for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
      commit_count += sc.getCurSequence();
    }
    Assert.assertEquals(commit_count, total);

    checkDBRecordCount(Long.valueOf(pk2Version.size()));
    checkDBRecordValue(schema);
  }

  @Test
  public void testSinkMysqlConnectorNormalAppendField() {
    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("f2", FieldType.BIGINT));
    schema.addField(new Field("f3", FieldType.DOUBLE));
    schema.addField(new Field("f4", FieldType.TIMESTAMP));
    schema.addField(new Field("f5", FieldType.BOOLEAN));
    schema.addField(new Field("pk", FieldType.STRING));
    CreateTopic(schema);

    putRecords(schema);
    schema.addField(new Field("f6", FieldType.STRING));
    client.appendField(new AppendFieldRequest(projectName, topicName, new Field("f6", FieldType.STRING)));

    CreateTable(schema, true);
    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("f2");
    columnFields.add("f3");
    columnFields.add("f4");
    columnFields.add("f5");
    columnFields.add("pk");
    columnFields.add("f6");

    List<String> ignore = new ArrayList<String>();
    ignore.add("f1");
    pk2Version.clear();
    pk2Data.clear();
    putRecords(schema, ignore);

    client.createDataConnector(projectName, topicName, connectoType, columnFields, desc);
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException e) {

    }
    // check db data
    long total = shardCount*recordCount * 2;
    long commit_count = 0;
    GetDataConnectorResult getDataConnectorResult = client.getDataConnector(projectName, topicName, connectoType);
    for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
      commit_count += sc.getCurSequence();
    }
    Assert.assertEquals(commit_count, total);

    checkDBRecordCount(Long.valueOf(pk2Version.size()));
    checkDBRecordValue(schema);
  }

  @Test
  public void testSinkMysqlConnectorSealed() {
    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("pk", FieldType.STRING));
    CreateTopic(schema, 1);

    CreateTable(schema, true);
    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("pk");
    client.createDataConnector(projectName, topicName, connectoType, columnFields, desc);

    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {

    }

    putRecords(schema);
    client.splitShard(projectName, topicName, "0");
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException e) {

    }
   // client.splitShard(projectName, topicName, "0");
    // check db data
    long total = recordCount;
    long commit_count = 0;
    GetDataConnectorResult getDataConnectorResult = client.getDataConnector(projectName, topicName, connectoType);
    for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
      commit_count += sc.getCurSequence();
    }
    Assert.assertEquals(commit_count, total);

    GetDataConnectorShardStatusResult getDataConnectorShardStatusResult = client.getDataConnectorShardStatus(projectName, topicName, connectoType, "0");
    Assert.assertEquals(getDataConnectorShardStatusResult.getState(), ConnectorShardStatus.CONTEXT_FINISHED);
    checkDBRecordCount(Long.valueOf(pk2Version.size()));
    checkDBRecordValue(schema);
  }

  @Test
  public void testSinkMysqlConnectorSealed2() {
    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("pk", FieldType.STRING));
    CreateTopic(schema, 1);

    CreateTable(schema, true);
    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("pk");
    client.createDataConnector(projectName, topicName, connectoType, columnFields, desc);

    putRecords(schema);

    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException e) {

    }
    client.splitShard(projectName, topicName, "0");
    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {

    }
    // check db data
    long total = recordCount;
    long commit_count = 0;
    GetDataConnectorResult getDataConnectorResult = client.getDataConnector(projectName, topicName, connectoType);
    for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
      commit_count += sc.getCurSequence();
    }
    Assert.assertEquals(commit_count, total);
    GetDataConnectorShardStatusResult getDataConnectorShardStatusResult = client.getDataConnectorShardStatus(projectName, topicName, connectoType, "0");
    Assert.assertEquals(getDataConnectorShardStatusResult.getState(), ConnectorShardStatus.CONTEXT_FINISHED);
    checkDBRecordCount(Long.valueOf(pk2Version.size()));
    checkDBRecordValue(schema);
  }

  @Test
  public void testSinkMysqlConnectorNormalWithCommitSize() {
    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("f2", FieldType.BIGINT));
    schema.addField(new Field("f3", FieldType.DOUBLE));
    schema.addField(new Field("f4", FieldType.TIMESTAMP));
    schema.addField(new Field("f5", FieldType.BOOLEAN));
    schema.addField(new Field("f6", FieldType.TIMESTAMP, true));
    schema.addField(new Field("pk", FieldType.STRING));
    CreateTopic(schema);

    CreateTable(schema, true);
    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("f2");
    columnFields.add("f3");
    columnFields.add("f4");
    columnFields.add("f5");
    columnFields.add("f6");
    columnFields.add("pk");
    desc.setMaxCommitSize(100L);
    client.createDataConnector(projectName, topicName, connectoType, columnFields, desc);

    ListDataConnectorResult listDataConnectorResult = client.listDataConnector(projectName, topicName);
    List<String> connectors = listDataConnectorResult.getDataConnectors();
    Assert.assertTrue(connectors.contains(connectoType.toString().toLowerCase()));

    GetDataConnectorResult getDataConnectorResult = client.getDataConnector(projectName, topicName, connectoType);
    Assert.assertEquals(getDataConnectorResult.getColumnFields(), columnFields);
    DatabaseDesc dbDesc = (DatabaseDesc)getDataConnectorResult.getConnectorConfig();
    Assert.assertEquals(dbDesc.getDatabase(), desc.getDatabase());
    Assert.assertEquals(dbDesc.getHost(), desc.getHost());
    Assert.assertEquals(dbDesc.getPort(), desc.getPort());
    Assert.assertEquals(dbDesc.getTable(), desc.getTable());

    putRecords(schema);
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException e) {

    }
    // check db data
    long total = shardCount*recordCount;
    long commit_count = 0;
    getDataConnectorResult = client.getDataConnector(projectName, topicName, connectoType);
    for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
      commit_count += sc.getCurSequence();
    }
    Assert.assertEquals(commit_count, total);

    checkDBRecordCount(Long.valueOf(pk2Version.size()));
    checkDBRecordValue(schema);
  }
}
