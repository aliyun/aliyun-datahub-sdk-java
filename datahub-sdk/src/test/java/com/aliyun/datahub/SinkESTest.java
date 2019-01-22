package com.aliyun.datahub;

import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.common.data.FieldType;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.common.data.RecordType;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.*;
import com.aliyun.datahub.model.*;
import com.aliyun.datahub.util.DatahubTestUtils;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.*;

public class SinkESTest {
  // datahub config
  private String projectName = null;
  private String topicName = null;
  private ConnectorType connectoType = null;
  private DatahubClient client = null;
  // ES config
  private ElasticSearchDesc esDesc = null;
  private String index = null;
  private RestClient esClient = null;
  private String sign = null;

  private Random random = null;
  private int shardCount = 3;
  private long timestamp = System.currentTimeMillis();
  private long recordCount = 100;
  private long sleepTime = 35000; // commit interval 30s

  // dataSet
  private Map<String, Long> id2Version = new HashMap<String, Long>();

  class ESRecord {
    boolean found;
    int version;
    Map<String, String> values;

    public ESRecord() {
      found = false;
      version = 0;
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
      connectoType = ConnectorType.SINK_ES;
      System.out.println("Topic Name is " + topicName);

      index = String.format("dh_test_index_%s", hint);
      esDesc = new ElasticSearchDesc();
      esDesc.setEndpoint("http://" + DatahubTestUtils.getESEndpoint());
      esDesc.setIndex(index);
      esDesc.setUser(DatahubTestUtils.getESUser());
      esDesc.setPassword(DatahubTestUtils.getESPassword());
      id2Version.clear();
      sign = "Basic " + Base64.encodeBase64String((DatahubTestUtils.getESUser() + ":" + DatahubTestUtils.getESPassword()).getBytes());
      esClient = RestClient.builder(
          new HttpHost(DatahubTestUtils.getESEndpoint(), DatahubTestUtils.getESPort(), "http")).build();

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @AfterMethod
  public void TearDown() {
    try {
      client.deleteTopic(projectName, topicName);
    } catch (DatahubClientException e) {

    }
    try {
      deleteESIndex();
      esClient.close();
    } catch (IOException e) {

    }
  }

  private void deleteESIndex() throws IOException {
    esClient.performRequest("DELETE", "/" + index, new BasicHeader("Authorization", sign));
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

  private String genDataId() {
    // rand from [0, recordCount/10] , to test duplicated key(update version)
    int id = random.nextInt((int) (recordCount / 10));
    return String.valueOf(id);
  }

  private RecordEntry genData(RecordSchema schema) {
    RecordEntry entry = new RecordEntry(schema);
    String idStr = "";
    String typeStr = "";
    for (Field i : schema.getFields()) {
      if (i.getName().startsWith("id")) {
        String id = genDataId();
        entry.setString(i.getName(), id);
        if (idStr.isEmpty()) {
          idStr += id;
        } else {
          idStr += "|" + id;
        }
        continue;
      }
      if (i.getName().startsWith("type")) {
        String type = genDataId();
        entry.setString(i.getName(), type);
        if (typeStr.isEmpty()) {
          typeStr += type;
        } else {
          typeStr += "|" + type;
        }
        continue;
      }
      switch (i.getType()) {
        case STRING:
          entry.setString(i.getName(), String.valueOf(timestamp));
          break;
        case BOOLEAN:
          entry.setBoolean(i.getName(), Boolean.valueOf(timestamp%2==0));
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
        default:
          Assert.assertTrue(false);
      }
    }
    Long value = Long.valueOf(0);
    if (id2Version.containsKey(typeStr + "/" + idStr)) {
      value = id2Version.get(typeStr + "/" + idStr);
    }
    value++;
    id2Version.put(typeStr + "/" + idStr, value);
    return entry;
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

        entry.setPartitionKey(entry.getString("id") + "/" + entry.getString("type"));

        recordEntries.add(entry);
      }
      PutRecordsResult result = client.putRecords(projectName, topicName, recordEntries);
      if (result.getFailedRecordCount() != 0) {
        System.out.println(result.getFailedRecordError().get(0).getMessage());
      }
      Assert.assertEquals(result.getFailedRecordCount(), 0);
    }
  }

  private ESRecord getESRecord(String type, String id) throws IOException {
    String fid = URLEncoder.encode(id, "utf-8");
    String ftype = URLEncoder.encode(type, "utf-8");
    Response response = esClient.performRequest("GET", "/" + index + "/" + ftype + "/" + fid, new BasicHeader("Authorization", sign));

    ESRecord record = new ESRecord();
    ObjectMapper mapper = JacksonParser.getObjectMapper();
    JsonNode tree = null;
    try {
      tree = mapper.readTree(response.getEntity().getContent());
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    }
    record.found = tree.get("found").asBoolean();
    if (record.found) {
      record.version = tree.get("_version").asInt();
      JsonNode values = tree.get("_source");
      Iterator<String> it = values.getFieldNames();
      while(it.hasNext()) {
        String key = it.next();
        record.values.put(key, values.get(key).asText());
      }
    }
    return record;
  }

  private long getESCount() {
    long count = 0l;
    try {
      Response response = esClient.performRequest("GET", "/" + index + "/_count", new BasicHeader("Authorization", sign));
      ObjectMapper mapper = JacksonParser.getObjectMapper();
      JsonNode tree = null;
      tree = mapper.readTree(response.getEntity().getContent());
      count = tree.get("count").asLong();
    } catch (IOException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }

    return count;
  }

  private void checkESData(RecordSchema schema, int valueSize) {
    for (Map.Entry<String, Long> entry : id2Version.entrySet()) {
      try {
        String idKey = entry.getKey();
        Long value = entry.getValue();
        String [] id2type = idKey.split("/");
        ESRecord record = getESRecord(id2type[0], id2type[1]);
        Assert.assertEquals(record.found, true);
        Assert.assertEquals(record.version, value.intValue());
        checkESDataValue(schema, record, valueSize);

      } catch (IOException e) {
        e.printStackTrace();
        Assert.assertTrue(false);
      }
    }
  }

  private void checkESDataValue(RecordSchema schema, ESRecord record, int valueSize) {
    Assert.assertEquals(record.values.size(), valueSize);
    for (Map.Entry<String, String> data : record.values.entrySet()) {
      Field field = schema.getField(data.getKey());
      switch (field.getType()) {
        case STRING:
          Assert.assertEquals(data.getValue(), String.valueOf(timestamp));
          break;
        case BOOLEAN:
          Assert.assertEquals(data.getValue(), String.valueOf(Boolean.valueOf(timestamp%2 == 0)));
          break;
        case DOUBLE:
          Assert.assertEquals(data.getValue(), String.valueOf(Double.valueOf(timestamp)));
          break;
        case TIMESTAMP:
          Assert.assertEquals(data.getValue(), String.valueOf(Long.valueOf(timestamp)));
          break;
        case BIGINT:
          Assert.assertEquals(data.getValue(), String.valueOf(Long.valueOf(timestamp)));
          break;
        default:
          Assert.assertTrue(false);
      }
    }
  }

  @Test(enabled = false)
  public void testSinkESConnectorNormal() {
    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("f2", FieldType.BIGINT));
    schema.addField(new Field("f3", FieldType.DOUBLE));
    schema.addField(new Field("f4", FieldType.TIMESTAMP));
    schema.addField(new Field("f5", FieldType.BOOLEAN));
    schema.addField(new Field("id", FieldType.STRING));
    schema.addField(new Field("type", FieldType.STRING));
    CreateTopic(schema);

    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("f2");
    columnFields.add("f3");
    columnFields.add("f4");
    columnFields.add("f5");
    columnFields.add("id");
    columnFields.add("type");

    List<String> idFields = new ArrayList<String>();
    idFields.add("id");
    List<String> typeFields = new ArrayList<String>();
    typeFields.add("type");
    esDesc.setIdFields(idFields);
    esDesc.setTypeFields(typeFields);
    client.createDataConnector(projectName, topicName, connectoType, columnFields, esDesc);

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
    // check odps data
    long total = shardCount*recordCount;
    long commit_count = 0;
    getDataConnectorResult = client.getDataConnector(projectName, topicName, connectoType);
    for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
      commit_count += sc.getCurSequence();
    }
    Assert.assertEquals(commit_count, total);

    checkESData(schema, columnFields.size() - idFields.size() - typeFields.size());

    client.deleteDataConnector(projectName, topicName, connectoType);

    listDataConnectorResult = client.listDataConnector(projectName, topicName);
    connectors = listDataConnectorResult.getDataConnectors();
    Assert.assertFalse(connectors.contains(connectoType.toString().toLowerCase()));
  }

  @Test(enabled = false)
  public void testSinkESConnectorNormalWithOutID() {
    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("f2", FieldType.BIGINT));
    schema.addField(new Field("f3", FieldType.DOUBLE));
    schema.addField(new Field("f4", FieldType.TIMESTAMP));
    schema.addField(new Field("f5", FieldType.BOOLEAN));
    schema.addField(new Field("id", FieldType.STRING));
    schema.addField(new Field("type", FieldType.STRING));
    CreateTopic(schema);

    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("f2");
    columnFields.add("f3");
    columnFields.add("f4");
    columnFields.add("f5");
    columnFields.add("id");
    columnFields.add("type");

    List<String> typeFields = new ArrayList<String>();
    typeFields.add("type");
    esDesc.setTypeFields(typeFields);
    client.createDataConnector(projectName, topicName, connectoType, columnFields, esDesc);

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
    // check odps data
    long total = shardCount*recordCount;
    long commit_count = 0;
    getDataConnectorResult = client.getDataConnector(projectName, topicName, connectoType);
    for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
      commit_count += sc.getCurSequence();
    }
    Assert.assertEquals(commit_count, total);
    Assert.assertEquals(getESCount(), total);
  }

  @Test(enabled = false, expectedExceptions = InvalidParameterException.class)
  public void testSinkESConnectorBlob() {
    CreateTopic();
    esDesc.setIdFields(new ArrayList<String>());
    esDesc.setTypeFields(new ArrayList<String>());
    client.createDataConnector(projectName, topicName, connectoType, new ArrayList<String>(), esDesc);
  }

  @Test(enabled = false, expectedExceptions = ResourceExistException.class)
  public void testSinkESConnectorExist() {
    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("id", FieldType.STRING));
    schema.addField(new Field("type", FieldType.STRING));
    CreateTopic(schema);

    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("id");
    columnFields.add("type");

    List<String> idFields = new ArrayList<String>();
    idFields.add("id");
    List<String> typeFields = new ArrayList<String>();
    typeFields.add("type");
    esDesc.setIdFields(idFields);
    esDesc.setTypeFields(typeFields);
    client.createDataConnector(projectName, topicName, connectoType, columnFields, esDesc);
    client.createDataConnector(projectName, topicName, connectoType, columnFields, esDesc);
  }

  @Test(enabled = false)
  public void testSinkESConnectorMissingIdFields() {
    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("id", FieldType.STRING));
    schema.addField(new Field("type", FieldType.STRING));
    CreateTopic(schema);

    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("id");
    columnFields.add("type");

    List<String> idFields = new ArrayList<String>();
    List<String> typeFields = new ArrayList<String>();
    typeFields.add("type");
    esDesc.setIdFields(idFields);
    esDesc.setTypeFields(typeFields);
    client.createDataConnector(projectName, topicName, connectoType, columnFields, esDesc);
  }

  @Test(enabled = false, expectedExceptions = InvalidParameterException.class)
  public void testSinkESConnectorInvalidIdFields() {
    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("id", FieldType.STRING));
    schema.addField(new Field("type", FieldType.STRING));
    CreateTopic(schema);

    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("id");
    columnFields.add("type");

    List<String> idFields = new ArrayList<String>();
    idFields.add("id1");
    List<String> typeFields = new ArrayList<String>();
    typeFields.add("type");
    esDesc.setIdFields(idFields);
    esDesc.setTypeFields(typeFields);
    client.createDataConnector(projectName, topicName, connectoType, columnFields, esDesc);
  }

  @Test(enabled = false)
  public void testSinkESConnectorMultiIdFields() {
    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("id", FieldType.STRING));
    schema.addField(new Field("id1", FieldType.STRING));
    schema.addField(new Field("type", FieldType.STRING));
    CreateTopic(schema);

    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("id");
    columnFields.add("id1");
    columnFields.add("type");

    List<String> idFields = new ArrayList<String>();
    idFields.add("id");
    idFields.add("id1");
    List<String> typeFields = new ArrayList<String>();
    typeFields.add("type");
    esDesc.setIdFields(idFields);
    esDesc.setTypeFields(typeFields);
    client.createDataConnector(projectName, topicName, connectoType, columnFields, esDesc);

    putRecords(schema);
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException e) {

    }
    // check odps data
    long total = shardCount*recordCount;
    long commit_count = 0;
    GetDataConnectorResult getDataConnectorResult = client.getDataConnector(projectName, topicName, connectoType);
    for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
      commit_count += sc.getCurSequence();
    }
    Assert.assertEquals(commit_count, total);

    checkESData(schema, columnFields.size() - idFields.size() - typeFields.size());
  }

  @Test(enabled = false, expectedExceptions = InvalidParameterException.class)
  public void testSinkESConnectorMissingTypeFields() {
    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("id", FieldType.STRING));
    schema.addField(new Field("type", FieldType.STRING));
    CreateTopic(schema);

    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("id");
    columnFields.add("type");

    List<String> idFields = new ArrayList<String>();
    idFields.add("id");
    List<String> typeFields = new ArrayList<String>();
    esDesc.setIdFields(idFields);
    esDesc.setTypeFields(typeFields);
    client.createDataConnector(projectName, topicName, connectoType, columnFields, esDesc);
  }

  @Test(enabled = false, expectedExceptions = InvalidParameterException.class)
  public void testSinkESConnectorInvalidTypeFields() {
    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("id", FieldType.STRING));
    schema.addField(new Field("type", FieldType.STRING));
    CreateTopic(schema);

    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("id");
    columnFields.add("type");

    List<String> idFields = new ArrayList<String>();
    idFields.add("id");
    List<String> typeFields = new ArrayList<String>();
    typeFields.add("type1");
    esDesc.setIdFields(idFields);
    esDesc.setTypeFields(typeFields);
    client.createDataConnector(projectName, topicName, connectoType, columnFields, esDesc);
  }

  @Test(enabled = false)
  public void testSinkESConnectorMultiTypeFields() {
    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("id", FieldType.STRING));
    schema.addField(new Field("type", FieldType.STRING));
    schema.addField(new Field("type1", FieldType.STRING));
    CreateTopic(schema);

    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("id");
    columnFields.add("type");
    columnFields.add("type1");

    List<String> idFields = new ArrayList<String>();
    idFields.add("id");
    List<String> typeFields = new ArrayList<String>();
    typeFields.add("type");
    typeFields.add("type1");
    esDesc.setIdFields(idFields);
    esDesc.setTypeFields(typeFields);
    client.createDataConnector(projectName, topicName, connectoType, columnFields, esDesc);

    putRecords(schema);
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException e) {

    }
    // check odps data
    long total = shardCount*recordCount;
    long commit_count = 0;
    GetDataConnectorResult getDataConnectorResult = client.getDataConnector(projectName, topicName, connectoType);
    for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
      commit_count += sc.getCurSequence();
    }
    Assert.assertEquals(commit_count, total);

    checkESData(schema, columnFields.size() - idFields.size() - typeFields.size());
  }

  @Test(enabled = false)
  public void testSinkESConnectorDupIdTypeFields() {
    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("id", FieldType.STRING));
    schema.addField(new Field("type", FieldType.STRING));
    CreateTopic(schema);

    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("id");
    columnFields.add("type");

    List<String> idFields = new ArrayList<String>();
    idFields.add("id");
    idFields.add("id");
    List<String> typeFields = new ArrayList<String>();
    typeFields.add("type");
    typeFields.add("type");
    esDesc.setIdFields(idFields);
    esDesc.setTypeFields(typeFields);
    client.createDataConnector(projectName, topicName, connectoType, columnFields, esDesc);

    putRecords(schema);
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException e) {

    }
    // check odps data
    long total = shardCount*recordCount;
    long commit_count = 0;
    GetDataConnectorResult getDataConnectorResult = client.getDataConnector(projectName, topicName, connectoType);
    for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
      commit_count += sc.getCurSequence();
    }
    Assert.assertEquals(commit_count, total);

    checkESData(schema, columnFields.size() - 2);
  }

  @Test(enabled = false)
  public void testSinkESConnectorIdTypeFieldsNotSelect() {
    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("f2", FieldType.STRING));
    schema.addField(new Field("id", FieldType.STRING));
    schema.addField(new Field("type", FieldType.STRING));
    CreateTopic(schema);

    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f2");
    // no need to select id/type field
    //columnFields.add("id");
    //columnFields.add("type");

    List<String> idFields = new ArrayList<String>();
    idFields.add("id");
    List<String> typeFields = new ArrayList<String>();
    typeFields.add("type");
    esDesc.setIdFields(idFields);
    esDesc.setTypeFields(typeFields);
    client.createDataConnector(projectName, topicName, connectoType, columnFields, esDesc);

    putRecords(schema);
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException e) {

    }
    // check odps data
    long total = shardCount*recordCount;
    long commit_count = 0;
    GetDataConnectorResult getDataConnectorResult = client.getDataConnector(projectName, topicName, connectoType);
    for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
      commit_count += sc.getCurSequence();
    }
    Assert.assertEquals(commit_count, total);

    checkESData(schema, columnFields.size());
  }

  @Test(enabled = false)
  public void testSinkESConnectorNotSelectColumn() {
    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("f2", FieldType.STRING));
    schema.addField(new Field("id", FieldType.STRING));
    schema.addField(new Field("type", FieldType.STRING));
    CreateTopic(schema);

    List<String> columnFields = new ArrayList<String>();
    // no col selected, empty record is support by es

    List<String> idFields = new ArrayList<String>();
    idFields.add("id");
    List<String> typeFields = new ArrayList<String>();
    typeFields.add("type");
    esDesc.setIdFields(idFields);
    esDesc.setTypeFields(typeFields);
    client.createDataConnector(projectName, topicName, connectoType, columnFields, esDesc);

    putRecords(schema);
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException e) {

    }
    // check odps data
    long total = shardCount*recordCount;
    long commit_count = 0;
    GetDataConnectorResult getDataConnectorResult = client.getDataConnector(projectName, topicName, connectoType);
    for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
      commit_count += sc.getCurSequence();
    }
    Assert.assertEquals(commit_count, total);

    checkESData(schema, columnFields.size());
  }

  @Test(enabled = false)
  public void testSinkESConnectorNormalAfterAppendField() {
    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("f2", FieldType.BIGINT));
    schema.addField(new Field("f3", FieldType.DOUBLE));
    schema.addField(new Field("f4", FieldType.TIMESTAMP));
    schema.addField(new Field("f5", FieldType.BOOLEAN));
    schema.addField(new Field("id", FieldType.STRING));
    schema.addField(new Field("type", FieldType.STRING));
    CreateTopic(schema);
    putRecords(schema);

    client.appendField(new AppendFieldRequest(projectName, topicName, new Field("app", FieldType.STRING)));
    RecordSchema schema2 = schema;
    schema2.addField(new Field("app", FieldType.STRING));
    putRecords(schema2);

    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("f2");
    columnFields.add("f3");
    columnFields.add("f4");
    columnFields.add("f5");
    columnFields.add("id");
    columnFields.add("type");
    columnFields.add("app");

    List<String> idFields = new ArrayList<String>();
    idFields.add("id");
    List<String> typeFields = new ArrayList<String>();
    typeFields.add("type");
    esDesc.setIdFields(idFields);
    esDesc.setTypeFields(typeFields);
    client.createDataConnector(projectName, topicName, connectoType, columnFields, esDesc);

    ListDataConnectorResult listDataConnectorResult = client.listDataConnector(projectName, topicName);
    List<String> connectors = listDataConnectorResult.getDataConnectors();
    Assert.assertTrue(connectors.contains(connectoType.toString().toLowerCase()));

    GetDataConnectorResult getDataConnectorResult = client.getDataConnector(projectName, topicName, connectoType);
    Assert.assertEquals(getDataConnectorResult.getColumnFields(), columnFields);

    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException e) {

    }
    // check odps data
    long total = shardCount*recordCount*2;
    long commit_count = 0;
    getDataConnectorResult = client.getDataConnector(projectName, topicName, connectoType);
    for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
      commit_count += sc.getCurSequence();
    }
    Assert.assertEquals(commit_count, total);

    //checkESData(schema, columnFields.size() - idFields.size() - typeFields.size());

    client.deleteDataConnector(projectName, topicName, connectoType);

    listDataConnectorResult = client.listDataConnector(projectName, topicName);
    connectors = listDataConnectorResult.getDataConnectors();
    Assert.assertFalse(connectors.contains(connectoType.toString().toLowerCase()));
  }

  @Test(enabled = false)
  public void testSinkESConnectorNormalWithCommitSize() {
    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("f1", FieldType.STRING));
    schema.addField(new Field("f2", FieldType.BIGINT));
    schema.addField(new Field("f3", FieldType.DOUBLE));
    schema.addField(new Field("f4", FieldType.TIMESTAMP));
    schema.addField(new Field("f5", FieldType.BOOLEAN));
    schema.addField(new Field("id", FieldType.STRING));
    schema.addField(new Field("type", FieldType.STRING));
    CreateTopic(schema);

    List<String> columnFields = new ArrayList<String>();
    columnFields.add("f1");
    columnFields.add("f2");
    columnFields.add("f3");
    columnFields.add("f4");
    columnFields.add("f5");
    columnFields.add("id");
    columnFields.add("type");

    List<String> idFields = new ArrayList<String>();
    idFields.add("id");
    List<String> typeFields = new ArrayList<String>();
    typeFields.add("type");
    esDesc.setIdFields(idFields);
    esDesc.setTypeFields(typeFields);
    esDesc.setMaxCommitSize(100L);
    client.createDataConnector(projectName, topicName, connectoType, columnFields, esDesc);

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
    // check odps data
    long total = shardCount*recordCount;
    long commit_count = 0;
    getDataConnectorResult = client.getDataConnector(projectName, topicName, connectoType);
    for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
      commit_count += sc.getCurSequence();
    }
    Assert.assertEquals(commit_count, total);

    checkESData(schema, columnFields.size() - idFields.size() - typeFields.size());

    client.deleteDataConnector(projectName, topicName, connectoType);

    listDataConnectorResult = client.listDataConnector(projectName, topicName);
    connectors = listDataConnectorResult.getDataConnectors();
    Assert.assertFalse(connectors.contains(connectoType.toString().toLowerCase()));
  }
}
