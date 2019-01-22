package com.aliyun.datahub;

import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.common.data.FieldType;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.common.data.RecordType;
import com.aliyun.datahub.exception.InvalidParameterException;
import com.aliyun.datahub.model.*;
import com.aliyun.datahub.util.DatahubTestUtils;
import com.aliyun.odps.*;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.*;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;

@Test
public class AppendFieldTest {
    private String projectName = null;
    private String topicName = null;
    private DatahubClient client = null;
    private int shardCount = 1;
    private int lifeCycle = 3;
    private Long timestamp = System.currentTimeMillis() * 1000;
    private long shipperWaitTime = 1000*60;
    //Odps
    private Odps odps = null;
    private TableTunnel tunnel = null;
    private String odpsProject = DatahubTestUtils.getOdpsProjectName();
    private String odpsTable = null;
    private String odpsEndpoint = DatahubTestUtils.getOdpsEndpoint();
    private String tunnelEndpoint = DatahubTestUtils.getOdpsTunnelEndpoint();
    private String accessId = DatahubTestUtils.getAccessId();
    private String accessKey = DatahubTestUtils.getAccessKey();
    private OdpsDesc odpsDesc = null;

    AppendFieldTest() {

    }

    @BeforeClass
    public void init() {
        DatahubConfiguration conf = DatahubTestUtils.getConf();
        projectName = DatahubTestUtils.getProjectName();
        client = new DatahubClient(conf);
        Account account = new AliyunAccount(DatahubTestUtils.getAccessId(), DatahubTestUtils.getAccessKey());
        odps = new Odps(account);
        odps.setDefaultProject(odpsProject);
        odps.setEndpoint(odpsEndpoint);
        tunnel = new TableTunnel(odps);
        if (tunnelEndpoint != null && !tunnelEndpoint.isEmpty()) {
            tunnel.setEndpoint(tunnelEndpoint);
        }
    }

    @BeforeMethod
    public void setUp() {
        timestamp = System.currentTimeMillis() * 1000;
        topicName = DatahubTestUtils.getRamdomTopicName();
        System.out.println("Timestamp is " + timestamp);
        // setUp for odps
        odpsTable = String.format("ut_test_table_%s", timestamp);
        System.out.println("OdpsTable Name is " + odpsTable);
        System.out.println("Topic Name is " + topicName);

        odpsDesc = new OdpsDesc();
        odpsDesc.setProject(odpsProject);
        odpsDesc.setTable(odpsTable);
        odpsDesc.setOdpsEndpoint(odpsEndpoint);
        odpsDesc.setTunnelEndpoint(tunnelEndpoint);
        odpsDesc.setAccessId(accessId);
        odpsDesc.setAccessKey(accessKey);
    }

    @AfterMethod (alwaysRun = true)
    public void tearDown() {
        try {
            client.deleteTopic(projectName, topicName);
            DropTable();
        } catch (Exception e) {

        }
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

    private void AppendTableColumn(Column col) {
        try {
            String sql = String.format("alter table %s add columns (%s %s);", odpsTable, col.getName(), col.getType());
            Instance i = SQLTask.run(odps, odps.getDefaultProject(), sql, null, null);
            i.waitForSuccess();
        } catch (OdpsException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }

    private void DropTable() {
        try {
            odps.tables().delete(odpsProject, odpsTable);
        } catch (OdpsException e) {
        }
    }

    private void downloadCheckData(long start, long recordCount, TableSchema odpsSchema) throws TunnelException, IOException {
        TableTunnel.DownloadSession down = null;
        if (odpsSchema.getPartitionColumns().isEmpty()) {
            down = tunnel.createDownloadSession(odpsProject, odpsTable);
        } else {
            PartitionSpec partitionSpec = new PartitionSpec();
            for (Column i : odpsSchema.getPartitionColumns()) {
                partitionSpec.set(i.getName(), String.valueOf(timestamp));
            }
            down = tunnel.createDownloadSession(odpsProject, odpsTable, partitionSpec);
        }
        Assert.assertNotNull(down);

        TableSchema schema = down.getSchema();
        Assert.assertNotNull(schema);

        RecordReader reader = down.openRecordReader(start, recordCount);
        Assert.assertNotNull(reader);

        long i = 0;
        com.aliyun.odps.data.Record r;
        List<String> nullList = new ArrayList<String>();
        if (odpsSchema.getColumns().size() != schema.getColumns().size()) {
            for (Column col : schema.getColumns()) {
                if (!odpsSchema.containsColumn(col.getName())) {
                    nullList.add(col.getName());
                }
            }
        }
        long count = 0;
        System.out.println(nullList);
        for (; (r = reader.read()) != null; ++i) {
            count++;
            checkDataWithNull(r, odpsSchema, nullList);
        }
        Assert.assertEquals(count, recordCount);
        reader.close();

        org.junit.Assert.assertEquals(recordCount, i);
    }

    private void checkData(com.aliyun.odps.data.Record r, TableSchema schema) {
        for (Column i : schema.getColumns()) {
            if (i.getName().equals("alwaysnull")) {
                Assert.assertEquals(r.get("alwaysnull"), null);
                continue;
            }
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
                default:
                    Assert.assertTrue(false);
            }
        }
    }

    private void checkDataWithNull(com.aliyun.odps.data.Record r, TableSchema schema, List<String> nullList) {
        checkData(r, schema);
        for(String i : nullList) {
            Assert.assertEquals(r.get(i), null);
        }
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
                default:
                    Assert.assertTrue(false);
            }
        }
        entry.setShardId("0");
        return entry;
    }

    private RecordSchema genTopicSchema() {
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("f3", FieldType.DOUBLE));
        schema.addField(new Field("f1", FieldType.STRING));
        schema.addField(new Field("f2", FieldType.BIGINT));
        schema.addField(new Field("pt", FieldType.STRING));
        schema.addField(new Field("ct", FieldType.STRING));
        schema.addField(new Field("f4", FieldType.TIMESTAMP));
        schema.addField(new Field("f5", FieldType.BOOLEAN));
        return schema;
    }

    private List<String> genSelectCol() {
        List<String> columnFields = new ArrayList<String>();
        columnFields.add("f2");
        columnFields.add("f3");
        columnFields.add("f4");
        columnFields.add("f5");
        columnFields.add("pt");
        columnFields.add("ct");
        columnFields.add("f1");
        return columnFields;
    }

    private TableSchema genTableSchema() {
        TableSchema odpsSchema = new TableSchema();
        odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
        odpsSchema.addColumn(new Column("f2", OdpsType.BIGINT));
        odpsSchema.addColumn(new Column("f3", OdpsType.DOUBLE));
        odpsSchema.addColumn(new Column("f4", OdpsType.DATETIME));
        odpsSchema.addColumn(new Column("f5", OdpsType.BOOLEAN));
        odpsSchema.addColumn(new Column("alwaysnull", OdpsType.STRING));
        odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
        odpsSchema.addPartitionColumn(new Column("ct", OdpsType.STRING));
        return odpsSchema;
    }

    @Test(expectedExceptions = InvalidParameterException.class)
    public void testAppendField() {
        RecordSchema schema = genTopicSchema();
        CreateTopic(schema);

        List<RecordEntry> recordEntriesoOld = new ArrayList<RecordEntry>();
        int recordNum = 10;
        RecordEntry entryOld = null;
        for (int n = 0; n < recordNum; n++) {
            entryOld = genData(schema);
            recordEntriesoOld.add(entryOld);
        }
        PutRecordsResult result = client.putRecords(projectName, topicName, recordEntriesoOld);
        Assert.assertEquals(result.getFailedRecordCount(), 0);

        client.appendField(new AppendFieldRequest(projectName, topicName, new Field("test", FieldType.BIGINT)));
        client.appendField(new AppendFieldRequest(projectName, topicName, new Field("test", FieldType.BIGINT)));
    }

    @Test
    public void testGetRecordAfterAppendField() {
        RecordSchema schema = genTopicSchema();
        CreateTopic(schema);

        List<RecordEntry> recordEntriesoOld = new ArrayList<RecordEntry>();
        int recordNum = 10;
        RecordEntry entryOld = null;
        for (int n = 0; n < recordNum; n++) {
            entryOld = genData(schema);
            recordEntriesoOld.add(entryOld);
        }
        PutRecordsResult result = client.putRecords(projectName, topicName, recordEntriesoOld);
        Assert.assertEquals(result.getFailedRecordCount(), 0);

        client.appendField(new AppendFieldRequest(projectName, topicName, new Field("test", FieldType.BIGINT)));
        GetTopicResult getTopicResult = client.getTopic(projectName, topicName);
        Assert.assertEquals(getTopicResult.getRecordType(), RecordType.TUPLE);
        RecordSchema recordSchema = getTopicResult.getRecordSchema();
        Assert.assertNotEquals(schema, recordSchema);

        List<RecordEntry> recordEntriesoNew = new ArrayList<RecordEntry>();
        RecordEntry entryNew = null;
        for (int n = 0; n < recordNum; n++) {
            entryNew = genData(recordSchema);
            recordEntriesoNew.add(entryNew);
        }
        result = client.putRecords(projectName, topicName, recordEntriesoOld);
        Assert.assertEquals(result.getFailedRecordCount(), 0);
        result = client.putRecords(projectName, topicName, recordEntriesoNew);
        Assert.assertEquals(result.getFailedRecordCount(), 0);

        result = client.putRecords(projectName, topicName, recordEntriesoOld);
        Assert.assertEquals(result.getFailedRecordCount(), 10);
        for (ErrorEntry e :result.getFailedRecordError()) {
            Assert.assertEquals(e.getErrorcode(), "MalformedRecord");
        }

        GetCursorResult getCursorResult = client.getCursor(projectName, topicName, "0", GetCursorRequest.CursorType.OLDEST);
        GetRecordsResult getRecordsResult = client.getRecords(projectName, topicName, "0", getCursorResult.getCursor(), 100, schema);
        for (int n = 0; n < recordNum*3; n++) {
            Assert.assertEquals(entryOld.toJsonNode().toString(), getRecordsResult.getRecords().get(n).toJsonNode().toString());
        }

        getRecordsResult = client.getRecords(projectName, topicName, "0", getCursorResult.getCursor(), 100, recordSchema);
        for (int n = 0; n < recordNum*2; n++) {
            Assert.assertNotEquals(entryOld.toJsonNode().toString(), getRecordsResult.getRecords().get(n).toJsonNode().toString());
            Assert.assertEquals(getRecordsResult.getRecords().get(n).getBigint("test"), null);
        }
        for (int n = 0; n < recordNum; n++) {
            Assert.assertEquals(entryNew.toJsonNode().toString(), getRecordsResult.getRecords().get(n + recordNum*2).toJsonNode().toString());
        }
    }

    @Test
    public void testConnectorAfterAppendFieldWithoutChangeOfTableAndConnector() {
        List<String> columnFields = genSelectCol();

        RecordSchema schema = genTopicSchema();
        CreateTopic(schema);

        TableSchema odpsSchema = genTableSchema();
        CreateTable(odpsSchema);

        client.createDataConnector(projectName, topicName, ConnectorType.SINK_ODPS, columnFields, odpsDesc);

        List<RecordEntry> recordEntriesoOld = new ArrayList<RecordEntry>();
        int recordNum = 10;
        RecordEntry entryOld = null;
        for (int n = 0; n < recordNum; n++) {
            entryOld = genData(schema);
            recordEntriesoOld.add(entryOld);
        }
        PutRecordsResult result = client.putRecords(projectName, topicName, recordEntriesoOld);
        Assert.assertEquals(result.getFailedRecordCount(), 0);

        try {
            Thread.sleep(shipperWaitTime);
        } catch (InterruptedException e) {

        }
        // check odps data
        try {
            downloadCheckData(0, recordNum, odpsSchema);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } catch (TunnelException e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }

        GetDataConnectorResult getDataConnectorResult = client.getDataConnector(projectName, topicName, ConnectorType.SINK_ODPS);
        Assert.assertEquals(getDataConnectorResult.getColumnFields(), columnFields);
        for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
            Assert.assertEquals(sc.getCurSequence(), recordNum);
        }

        client.appendField(new AppendFieldRequest(projectName, topicName, new Field("test", FieldType.BIGINT)));
        GetTopicResult getTopicResult = client.getTopic(projectName, topicName);
        Assert.assertEquals(getTopicResult.getRecordType(), RecordType.TUPLE);
        RecordSchema recordSchema = getTopicResult.getRecordSchema();
        Assert.assertNotEquals(schema, recordSchema);

        List<RecordEntry> recordEntriesoNew = new ArrayList<RecordEntry>();
        RecordEntry entryNew = null;
        for (int n = 0; n < recordNum; n++) {
            entryNew = genData(recordSchema);
            recordEntriesoNew.add(entryNew);
        }
        result = client.putRecords(projectName, topicName, recordEntriesoOld);
        Assert.assertEquals(result.getFailedRecordCount(), 0);
        result = client.putRecords(projectName, topicName, recordEntriesoNew);
        Assert.assertEquals(result.getFailedRecordCount(), 0);

        result = client.putRecords(projectName, topicName, recordEntriesoOld);
        Assert.assertEquals(result.getFailedRecordCount(), 10);
        for (ErrorEntry e :result.getFailedRecordError()) {
            Assert.assertEquals(e.getErrorcode(), "MalformedRecord");
        }

        try {
            Thread.sleep(shipperWaitTime);
        } catch (InterruptedException e) {

        }
        // check odps data
        try {
            downloadCheckData(0, recordNum * 3, odpsSchema);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } catch (TunnelException e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }

        getDataConnectorResult = client.getDataConnector(projectName, topicName, ConnectorType.SINK_ODPS);
        Assert.assertEquals(getDataConnectorResult.getColumnFields(), columnFields);
        for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
            Assert.assertEquals(sc.getCurSequence(), recordNum*3);
        }
    }

    @Test
    public void testConnectorAfterAppendFieldWithoutChangeOfTableAndConnectorreloadDataConnector() {
        List<String> columnFields = genSelectCol();

        RecordSchema schema = genTopicSchema();
        CreateTopic(schema);

        TableSchema odpsSchema = genTableSchema();
        CreateTable(odpsSchema);

        client.createDataConnector(projectName, topicName, ConnectorType.SINK_ODPS, columnFields, odpsDesc);

        List<RecordEntry> recordEntriesoOld = new ArrayList<RecordEntry>();
        int recordNum = 10;
        RecordEntry entryOld = null;
        for (int n = 0; n < recordNum; n++) {
            entryOld = genData(schema);
            recordEntriesoOld.add(entryOld);
        }
        PutRecordsResult result = client.putRecords(projectName, topicName, recordEntriesoOld);
        Assert.assertEquals(result.getFailedRecordCount(), 0);

        try {
            Thread.sleep(shipperWaitTime);
        } catch (InterruptedException e) {

        }
        // check odps data
        try {
            downloadCheckData(0, recordNum, odpsSchema);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } catch (TunnelException e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }

        GetDataConnectorResult getDataConnectorResult = client.getDataConnector(projectName, topicName, ConnectorType.SINK_ODPS);
        Assert.assertEquals(getDataConnectorResult.getColumnFields(), columnFields);
        for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
            Assert.assertEquals(sc.getCurSequence(), recordNum);
        }

        client.appendField(new AppendFieldRequest(projectName, topicName, new Field("test", FieldType.BIGINT)));
        GetTopicResult getTopicResult = client.getTopic(projectName, topicName);
        Assert.assertEquals(getTopicResult.getRecordType(), RecordType.TUPLE);
        RecordSchema recordSchema = getTopicResult.getRecordSchema();
        Assert.assertNotEquals(schema, recordSchema);

        client.reloadDataConnector(projectName, topicName, ConnectorType.SINK_ODPS);

        List<RecordEntry> recordEntriesoNew = new ArrayList<RecordEntry>();
        RecordEntry entryNew = null;
        for (int n = 0; n < recordNum; n++) {
            entryNew = genData(recordSchema);
            recordEntriesoNew.add(entryNew);
        }
        result = client.putRecords(projectName, topicName, recordEntriesoOld);
        Assert.assertEquals(result.getFailedRecordCount(), 0);
        result = client.putRecords(projectName, topicName, recordEntriesoNew);
        Assert.assertEquals(result.getFailedRecordCount(), 0);

        result = client.putRecords(projectName, topicName, recordEntriesoOld);
        Assert.assertEquals(result.getFailedRecordCount(), 10);
        for (ErrorEntry e :result.getFailedRecordError()) {
            Assert.assertEquals(e.getErrorcode(), "MalformedRecord");
        }

        try {
            Thread.sleep(shipperWaitTime);
        } catch (InterruptedException e) {

        }
        // check odps data
        try {
            downloadCheckData(0, recordNum * 3, odpsSchema);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } catch (TunnelException e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }

        getDataConnectorResult = client.getDataConnector(projectName, topicName, ConnectorType.SINK_ODPS);
        Assert.assertEquals(getDataConnectorResult.getColumnFields(), columnFields);
        for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
            Assert.assertEquals(sc.getCurSequence(), recordNum*3);
        }
    }

    @Test
    public void testConnectorAfterAppendFieldAddTableColumn() {
        List<String> columnFields = genSelectCol();

        RecordSchema schema = genTopicSchema();
        CreateTopic(schema);

        TableSchema odpsSchema = genTableSchema();
        CreateTable(odpsSchema);

        client.createDataConnector(projectName, topicName, ConnectorType.SINK_ODPS, columnFields, odpsDesc);

        List<RecordEntry> recordEntriesoOld = new ArrayList<RecordEntry>();
        int recordNum = 10;
        RecordEntry entryOld = null;
        for (int n = 0; n < recordNum; n++) {
            entryOld = genData(schema);
            recordEntriesoOld.add(entryOld);
        }
        PutRecordsResult result = client.putRecords(projectName, topicName, recordEntriesoOld);
        Assert.assertEquals(result.getFailedRecordCount(), 0);

        try {
            Thread.sleep(shipperWaitTime);
        } catch (InterruptedException e) {

        }
        // check odps data
        try {
            downloadCheckData(0, recordNum, odpsSchema);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } catch (TunnelException e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }

        GetDataConnectorResult getDataConnectorResult = client.getDataConnector(projectName, topicName, ConnectorType.SINK_ODPS);
        Assert.assertEquals(getDataConnectorResult.getColumnFields(), columnFields);
        for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
            Assert.assertEquals(sc.getCurSequence(), recordNum);
        }

        client.appendField(new AppendFieldRequest(projectName, topicName, new Field("test", FieldType.BIGINT)));
        GetTopicResult getTopicResult = client.getTopic(projectName, topicName);
        Assert.assertEquals(getTopicResult.getRecordType(), RecordType.TUPLE);
        RecordSchema recordSchema = getTopicResult.getRecordSchema();
        Assert.assertNotEquals(schema, recordSchema);

        AppendTableColumn(new Column("test", OdpsType.BIGINT));
        client.reloadDataConnector(projectName, topicName, ConnectorType.SINK_ODPS);

        List<RecordEntry> recordEntriesoNew = new ArrayList<RecordEntry>();
        RecordEntry entryNew = null;
        for (int n = 0; n < recordNum; n++) {
            entryNew = genData(recordSchema);
            recordEntriesoNew.add(entryNew);
        }
        result = client.putRecords(projectName, topicName, recordEntriesoOld);
        Assert.assertEquals(result.getFailedRecordCount(), 0);
        result = client.putRecords(projectName, topicName, recordEntriesoNew);
        Assert.assertEquals(result.getFailedRecordCount(), 0);

        result = client.putRecords(projectName, topicName, recordEntriesoOld);
        Assert.assertEquals(result.getFailedRecordCount(), 10);
        for (ErrorEntry e :result.getFailedRecordError()) {
            Assert.assertEquals(e.getErrorcode(), "MalformedRecord");
        }

        try {
            Thread.sleep(shipperWaitTime);
        } catch (InterruptedException e) {

        }
        // check odps data
        try {
            downloadCheckData(0, recordNum * 3, odpsSchema);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } catch (TunnelException e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }

        getDataConnectorResult = client.getDataConnector(projectName, topicName, ConnectorType.SINK_ODPS);
        Assert.assertEquals(getDataConnectorResult.getColumnFields(), columnFields);
        for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
            Assert.assertEquals(sc.getCurSequence(), recordNum*3);
        }
    }

    @Test
    public void testConnectorAfterAppendFieldAddTableColumnappendDataConnectorField() {
        List<String> columnFields = genSelectCol();

        RecordSchema schema = genTopicSchema();
        CreateTopic(schema);

        TableSchema odpsSchema = genTableSchema();
        CreateTable(odpsSchema);

        client.createDataConnector(projectName, topicName, ConnectorType.SINK_ODPS, columnFields, odpsDesc);

        List<RecordEntry> recordEntriesoOld = new ArrayList<RecordEntry>();
        int recordNum = 10;
        RecordEntry entryOld = null;
        for (int n = 0; n < recordNum; n++) {
            entryOld = genData(schema);
            recordEntriesoOld.add(entryOld);
        }
        PutRecordsResult result = client.putRecords(projectName, topicName, recordEntriesoOld);
        Assert.assertEquals(result.getFailedRecordCount(), 0);

        try {
            Thread.sleep(shipperWaitTime);
        } catch (InterruptedException e) {

        }
        // check odps data
        try {
            downloadCheckData(0, recordNum, odpsSchema);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } catch (TunnelException e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }

        GetDataConnectorResult getDataConnectorResult = client.getDataConnector(projectName, topicName, ConnectorType.SINK_ODPS);
        Assert.assertEquals(getDataConnectorResult.getColumnFields(), columnFields);
        for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
            Assert.assertEquals(sc.getCurSequence(), recordNum);
        }

        client.appendField(new AppendFieldRequest(projectName, topicName, new Field("test", FieldType.BIGINT)));
        GetTopicResult getTopicResult = client.getTopic(projectName, topicName);
        Assert.assertEquals(getTopicResult.getRecordType(), RecordType.TUPLE);
        RecordSchema recordSchema = getTopicResult.getRecordSchema();
        Assert.assertNotEquals(schema, recordSchema);

        AppendTableColumn(new Column("test", OdpsType.BIGINT));
        client.appendDataConnectorField(new AppendDataConnectorFieldRequest(projectName, topicName, ConnectorType.SINK_ODPS, "test"));

        List<RecordEntry> recordEntriesoNew = new ArrayList<RecordEntry>();
        RecordEntry entryNew = null;
        for (int n = 0; n < recordNum; n++) {
            entryNew = genData(recordSchema);
            recordEntriesoNew.add(entryNew);
        }
        result = client.putRecords(projectName, topicName, recordEntriesoOld);
        Assert.assertEquals(result.getFailedRecordCount(), 0);
        result = client.putRecords(projectName, topicName, recordEntriesoNew);
        Assert.assertEquals(result.getFailedRecordCount(), 0);

        result = client.putRecords(projectName, topicName, recordEntriesoOld);
        Assert.assertEquals(result.getFailedRecordCount(), 10);
        for (ErrorEntry e :result.getFailedRecordError()) {
            Assert.assertEquals(e.getErrorcode(), "MalformedRecord");
        }

        try {
            Thread.sleep(shipperWaitTime);
        } catch (InterruptedException e) {

        }
        // check odps data
        try {
            downloadCheckData(0, recordNum * 2, odpsSchema);
            odpsSchema.addColumn(new Column("test", OdpsType.BIGINT));
            downloadCheckData(recordNum * 2, recordNum, odpsSchema);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } catch (TunnelException e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }

        getDataConnectorResult = client.getDataConnector(projectName, topicName, ConnectorType.SINK_ODPS);
        columnFields.add("test");
        Assert.assertEquals(getDataConnectorResult.getColumnFields(), columnFields);
        for (ShardContext sc : getDataConnectorResult.getShardContexts()) {
            Assert.assertEquals(sc.getCurSequence(), recordNum*3);
        }
    }

    @Test(expectedExceptions = InvalidParameterException.class)
    public void testConnectorAppendConnectorInvalidField() {
        List<String> columnFields = genSelectCol();

        RecordSchema schema = genTopicSchema();
        CreateTopic(schema);

        TableSchema odpsSchema = genTableSchema();
        CreateTable(odpsSchema);

        client.createDataConnector(projectName, topicName, ConnectorType.SINK_ODPS, columnFields, odpsDesc);

        client.appendDataConnectorField(new AppendDataConnectorFieldRequest(projectName, topicName, ConnectorType.SINK_ODPS, "test"));
    }

    @Test(expectedExceptions = InvalidParameterException.class)
    public void testConnectorappendDataConnectorFieldInvalidType() {
        List<String> columnFields = genSelectCol();

        RecordSchema schema = genTopicSchema();
        CreateTopic(schema);

        TableSchema odpsSchema = genTableSchema();
        CreateTable(odpsSchema);

        client.createDataConnector(projectName, topicName, ConnectorType.SINK_ODPS, columnFields, odpsDesc);
        AppendTableColumn(new Column("test", OdpsType.STRING));
        client.appendField(new AppendFieldRequest(projectName, topicName, new Field("test", FieldType.BIGINT)));
        client.appendDataConnectorField(new AppendDataConnectorFieldRequest(projectName, topicName, ConnectorType.SINK_ODPS, "test"));
    }

    @Test(expectedExceptions = InvalidParameterException.class)
    public void testConnectorappendDataConnectorFieldNotExistInTable() {
        List<String> columnFields = genSelectCol();

        RecordSchema schema = genTopicSchema();
        CreateTopic(schema);

        TableSchema odpsSchema = genTableSchema();
        CreateTable(odpsSchema);

        client.createDataConnector(projectName, topicName, ConnectorType.SINK_ODPS, columnFields, odpsDesc);

        client.appendField(new AppendFieldRequest(projectName, topicName, new Field("test", FieldType.BIGINT)));
        client.appendDataConnectorField(new AppendDataConnectorFieldRequest(projectName, topicName, ConnectorType.SINK_ODPS, "test"));
    }

    @Test(expectedExceptions = InvalidParameterException.class)
    public void testConnectorappendDataConnectorFieldEmpty() {
        List<String> columnFields = genSelectCol();

        RecordSchema schema = genTopicSchema();
        CreateTopic(schema);

        TableSchema odpsSchema = genTableSchema();
        CreateTable(odpsSchema);

        client.createDataConnector(projectName, topicName, ConnectorType.SINK_ODPS, columnFields, odpsDesc);

        client.appendDataConnectorField(new AppendDataConnectorFieldRequest(projectName, topicName, ConnectorType.SINK_ODPS, ""));
    }

    @Test(expectedExceptions = InvalidParameterException.class)
    public void testConnectorAppendConnectorDuplicateField() {
        List<String> columnFields = genSelectCol();

        RecordSchema schema = genTopicSchema();
        CreateTopic(schema);

        TableSchema odpsSchema = genTableSchema();
        CreateTable(odpsSchema);

        client.createDataConnector(projectName, topicName, ConnectorType.SINK_ODPS, columnFields, odpsDesc);
        client.appendField(new AppendFieldRequest(projectName, topicName, new Field("test", FieldType.BIGINT)));
        client.appendDataConnectorField(new AppendDataConnectorFieldRequest(projectName, topicName, ConnectorType.SINK_ODPS, "test"));
        client.appendDataConnectorField(new AppendDataConnectorFieldRequest(projectName, topicName, ConnectorType.SINK_ODPS, "test"));
    }
}
