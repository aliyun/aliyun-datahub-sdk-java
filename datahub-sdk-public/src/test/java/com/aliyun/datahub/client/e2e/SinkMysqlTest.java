package com.aliyun.datahub.client.e2e;

import com.aliyun.datahub.client.e2e.common.Configure;
import com.aliyun.datahub.client.e2e.common.Constant;
import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.exception.InvalidParameterException;
import com.aliyun.datahub.client.model.*;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.sql.*;
import java.util.*;

import static org.testng.Assert.fail;

public class SinkMysqlTest extends BaseConnectorTest {
    static final String PK_COL = "pk";
    private Connection dbConn;
    protected SinkMysqlConfig config;
    protected boolean enableDecimal = true;

    private Map<String, RecordEntry> pkDataMap = new HashMap<>();

    @BeforeMethod
    @Override
    public void setUpBeforeMethod() {
        super.setUpBeforeMethod();
        pkDataMap.clear();
        initSinkConfig();
        dbConn = openDbConnection();
    }

    @AfterMethod
    @Override
    public void tearDownAfterMethod() {
        try {
            ListTopicResult listTopicResult = client.listTopic(TEST_PROJECT_NAME);
            for (String topic : listTopicResult.getTopicNames()) {
                ListConnectorResult listConnectorResult = client.listConnector(TEST_PROJECT_NAME, topic);
                for (String connector : listConnectorResult.getConnectorNames()) {
                    client.deleteConnector(TEST_PROJECT_NAME, topic, ConnectorType.valueOf(connector.toUpperCase()));
                }

                client.deleteTopic(TEST_PROJECT_NAME, topic);
            }

        } catch (Exception e) {
            //
        }

        // delete table
        deleteTables();
    }

    void initSinkConfig() {
        connectorType = ConnectorType.SINK_MYSQL;
        config = new SinkMysqlConfig() {{
            setHost(Configure.getString(Constant.MYSQL_HOST));
            setPort(Configure.getInteger(Constant.MYSQL_PORT));
            setUser(Configure.getString(Constant.MYSQL_USER));
            setPassword(Configure.getString(Constant.MYSQL_PASSWORD));
            setDatabase(Configure.getString(Constant.MYSQL_DATABASE));
            setTable(tupleTopicName);
            setInsertMode(InsertMode.IGNORE);
        }};
    }

    private Connection openDbConnection() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            String dbUrl = String.format("jdbc:mysql://%s:%d/%s?useUnicode=true&characterEncoding=UTF-8", config.getHost(), config.getPort(), config.getDatabase());
            Connection conn = DriverManager.getConnection(dbUrl, config.getUser(), config.getPassword());
            if (conn.isClosed()) {
                throw new DatahubClientException("Open db connection fail");
            }
            return conn;
        } catch (Exception e) {
            throw new DatahubClientException("Open db connection fail");
        }
    }

    void createTableBySchema(RecordSchema schema, boolean hasPK) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(config.getTable()).append(" (");

        for (int i = 0; i < schema.getFields().size(); ++i) {
            Field field = schema.getField(i);
            if (field.getType() == FieldType.DECIMAL) {
                sb.append(field.getName()).append(" DECIMAL(30, 30)");
            } else if (field.getType() != FieldType.STRING) {
                sb.append(field.getName()).append(" ").append(field.getType().toString());
            } else {
                sb.append(field.getName()).append(" VARCHAR(255)");
            }

            if (i != schema.getFields().size() - 1) {
                sb.append(",");
            }
        }

        if (hasPK) {
            sb.append(", PRIMARY KEY(").append(PK_COL).append("))");
        } else {
            sb.append(")");
        }

        doDbExecutor(sb.toString());
    }

    void deleteTables() {
        String sql = String.format("SELECT TABLE_NAME AS name FROM information_schema.TABLES WHERE table_schema = '%s'", config.getDatabase());
        ResultSet rs = selectDbData(sql);
        try {
            while (rs.next()) {
                String tableName = rs.getString("name");
                String dropSql = "DROP TABLE " + tableName;
                doDbExecutor(dropSql);
            }
        } catch (Exception e) {
            //
        }
    }

    void doDbExecutor(String sql) {
        try {
            PreparedStatement statement = dbConn.prepareStatement(sql);
            statement.execute();
        } catch (SQLException e) {
            throw new DatahubClientException("Execute sql failed :" + sql + " error:" + e.getMessage());
        }
    }

    private String genPK() {
        int pkId = random.nextInt(RECORD_COUNT);
        return String.valueOf(pkId);
    }

    private RecordEntry genData(RecordSchema schema, List<String> ignoreList) {
        final TupleRecordData recordData = new TupleRecordData(schema);
        for (Field field : schema.getFields()) {
            String fieldName = field.getName();
            if (ignoreList != null && ignoreList.contains(fieldName)) {
                recordData.setField(fieldName, null);
            } else if (fieldName.equals(PK_COL)) {
                recordData.setField(PK_COL, genPK());
            } else {
                switch (field.getType()) {
                    case STRING:
                        recordData.setField(fieldName, "test\"test");
                        break;
                    case BOOLEAN:
                        recordData.setField(fieldName, random.nextBoolean());
                        break;
                    case DOUBLE:
                        // Prevent double decimal places from being too long, it will be truncated after entering Mysql
                        recordData.setField(fieldName, random.nextInt(1000) / 100.0);
                        break;
                    case TIMESTAMP:
                        recordData.setField(fieldName, System.currentTimeMillis() * 1000); // us
                        break;
                    case BIGINT:
                        recordData.setField(fieldName, random.nextLong());
                        break;
                    case DECIMAL:
                        recordData.setField(fieldName, new BigDecimal(random.nextDouble()));
                        break;
                }
            }
        }

        RecordEntry recordEntry = new RecordEntry() {{ setRecordData(recordData); }};
        if (schema.containsField(PK_COL)) {
            String pkKey = (String) recordData.getField(PK_COL);
            // same pk into same shard
            recordEntry.setPartitionKey(pkKey);

            if (config.getInsertMode() == SinkMysqlConfig.InsertMode.IGNORE) {
                if (!pkDataMap.containsKey(pkKey)) {
                    pkDataMap.put(pkKey, recordEntry);
                }
            } else {
                pkDataMap.put(pkKey, recordEntry);
            }
        } else {
            recordEntry.setShardId(String.valueOf(random.nextInt(SHARD_COUNT)));
        }

        return recordEntry;
    }

    ResultSet selectDbData(String sql) {
        try {
            PreparedStatement statement = dbConn.prepareStatement(sql);
            return statement.executeQuery();
        } catch (SQLException e) {
            throw new DatahubClientException("Execute sql fail:" + sql + " error:" + e.getMessage());
        }
    }

    private long getDbCount() {
        String sql = "SELECT COUNT(1) AS cnt FROM " + config.getTable();
        try {
            ResultSet rs = selectDbData(sql);
            if (!rs.next()) {
                throw new Exception("Get count from db fail:" + sql);
            }
            return rs.getLong("cnt");
        } catch (Exception e) {
            throw new DatahubClientException("Execute sql fail:" + sql + " error:" + e.getMessage());
        }
    }

    long convertTimestamp(ResultSet rs, String dbCol) throws SQLException {
        return rs.getTimestamp(dbCol).getTime() / 1000;
    }

    private void checkSinkedData(RecordSchema schema, List<String> dbCols) {
        String sql = "SELECT * FROM " + config.getTable();
        ResultSet rs = selectDbData(sql);

        try {
            while (rs.next()) {
                String pk = rs.getString(PK_COL);
                TupleRecordData recordData = (TupleRecordData)(pkDataMap.get(pk).getRecordData());

                for (String dbCol : dbCols) {
                    Field field = schema.getField(dbCol);
                    String dbValue = rs.getString(dbCol);
                    if (dbValue == null) {
                        Assert.assertNull(recordData.getField(dbCol));
                        continue;
                    }

                    switch (field.getType()) {
                        case TIMESTAMP:
                            Assert.assertEquals(convertTimestamp(rs, dbCol), ((long)(recordData.getField(dbCol))) / 1000000);
//                            Assert.assertEquals(rs.getTimestamp(dbCol).getTime() / 1000, ((long)(recordData.getField(dbCol))) / 1000000);
                            break;
                        case DOUBLE:
                            Assert.assertEquals(Double.valueOf(dbValue), recordData.getField(dbCol));
                            break;
                        case DECIMAL:
                            Assert.assertEquals(new BigDecimal(dbValue), ((BigDecimal)(recordData.getField(dbCol))).setScale(30, BigDecimal.ROUND_HALF_UP));
                            break;
                        case BIGINT:
                            Assert.assertEquals(Long.valueOf(dbValue), recordData.getField(dbCol));
                            break;
                        case BOOLEAN:
                            boolean bValue = dbValue.equals("1");
                            Assert.assertEquals(bValue, recordData.getField(dbCol));
                            break;
                        default:
                            Assert.assertEquals(dbValue, recordData.getField(dbCol));
                            break;
                    }
                }
            }

            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail("check sinked data fail. error:" + e.getMessage());
        }
    }

    @Test
    public void testSinkNormal() {
        RecordSchema schema = new RecordSchema() {{
            addField(new Field("f1", FieldType.STRING));
            addField(new Field("f2", FieldType.BIGINT));
            addField(new Field("f3", FieldType.DOUBLE));
            addField(new Field("f4", FieldType.TIMESTAMP));
            addField(new Field("f5", FieldType.BOOLEAN));
            if (enableDecimal) {
                addField(new Field("f6", FieldType.DECIMAL));
            }
            addField(new Field(PK_COL, FieldType.STRING));
        }};

        createTableBySchema(schema, true);
        createTupleTopicBySchema(schema);

        List<String> columnFields = enableDecimal ?
                Arrays.asList("f1", "f2", "f3", "f4", "f5", "f6", "pk") : Arrays.asList("f1", "f2", "f3", "f4", "f5", "pk");

        client.createConnector(TEST_PROJECT_NAME, tupleTopicName, connectorType, columnFields, config);

        sleepInMs(MAX_OPERATOR_INTERVAL);
        // get connector
        GetConnectorResult getConnectorResult = client.getConnector(TEST_PROJECT_NAME, tupleTopicName, connectorType);
        SinkMysqlConfig getConfig = (SinkMysqlConfig)getConnectorResult.getConfig();
        Assert.assertEquals(getConfig.getHost(), config.getHost());
        Assert.assertEquals(getConfig.getTable(), config.getTable());
        Assert.assertEquals(getConnectorResult.getColumnFields(), columnFields);
        Assert.assertEquals(getConnectorResult.getState(), ConnectorState.RUNNING);

        // put records
        List<RecordEntry> recordEntries = new ArrayList<>();
        for (int i = 0; i < RECORD_COUNT; ++i) {
            recordEntries.add(genData(schema, null));
        }
        PutRecordsResult putRecordsResult = client.putRecords(TEST_PROJECT_NAME, tupleTopicName, recordEntries);
        Assert.assertEquals(putRecordsResult.getFailedRecordCount(), 0);

        // wait for sink
        waitForAllShardSinked(tupleTopicName, MAX_SINK_TIMEOUT);

        // check data
        checkSinkedData(schema, columnFields);

        // stop connector
        client.updateConnectorState(TEST_PROJECT_NAME, tupleTopicName, connectorType, ConnectorState.STOPPED);
        sleepInMs(MAX_OPERATOR_INTERVAL);

        getConnectorResult = client.getConnector(TEST_PROJECT_NAME, tupleTopicName, connectorType);
        Assert.assertEquals(getConnectorResult.getState(), ConnectorState.STOPPED);

        GetConnectorShardStatusResult getConnectorShardStatusResult = client.getConnectorShardStatus(TEST_PROJECT_NAME, tupleTopicName, connectorType);
        for (ConnectorShardStatusEntry entry : getConnectorShardStatusResult.getStatusEntryMap().values()) {
            Assert.assertEquals(entry.getState(), ConnectorShardState.STOPPED);
        }
        // update connector offset
        ConnectorOffset offset = new ConnectorOffset() {{ setSequence(10); setTimestamp(1000);}};
        client.updateConnectorOffset(TEST_PROJECT_NAME, tupleTopicName, connectorType, null, offset);

        getConnectorShardStatusResult = client.getConnectorShardStatus(TEST_PROJECT_NAME, tupleTopicName, connectorType);
        for (ConnectorShardStatusEntry entry : getConnectorShardStatusResult.getStatusEntryMap().values()) {
            Assert.assertEquals(entry.getCurrSequence(), 10);
            Assert.assertEquals(entry.getCurrTimestamp(), 1000);
        }

        // split shard
        client.splitShard(TEST_PROJECT_NAME, tupleTopicName, "0");

        sleepInMs(MAX_OPERATOR_INTERVAL);
        // start connector
        client.updateConnectorState(TEST_PROJECT_NAME, tupleTopicName, connectorType, ConnectorState.RUNNING);

        // wait reload
        sleepInMs(MAX_OPERATOR_INTERVAL);

        // write data to new shard
        client.putRecords(TEST_PROJECT_NAME, tupleTopicName, recordEntries);
        Assert.assertEquals(putRecordsResult.getFailedRecordCount(), 0);

        waitForAllShardSinked(tupleTopicName, MAX_SINK_TIMEOUT);

        // check sealed state
        getConnectorShardStatusResult = client.getConnectorShardStatus(TEST_PROJECT_NAME, tupleTopicName, connectorType);
        for (Map.Entry<String, ConnectorShardStatusEntry> entrySet : getConnectorShardStatusResult.getStatusEntryMap().entrySet()) {
            if (entrySet.getKey().equals("0")) {
                Assert.assertEquals(entrySet.getValue().getState(), ConnectorShardState.FINISHED);
            } else {
                Assert.assertEquals(entrySet.getValue().getState(), ConnectorShardState.EXECUTING);
            }
        }
    }

    @Test
    public void testSinkWithIgnoreFalse() {
        RecordSchema schema = new RecordSchema() {{
            addField(new Field("f1", FieldType.STRING));
            addField(new Field("f2", FieldType.BIGINT));
            addField(new Field("f3", FieldType.DOUBLE));
            addField(new Field("f4", FieldType.TIMESTAMP));
            addField(new Field("f5", FieldType.BOOLEAN));
            if (enableDecimal) {
                addField(new Field("f6", FieldType.DECIMAL));
            }
            addField(new Field(PK_COL, FieldType.STRING));
        }};

        createTableBySchema(schema, true);
        createTupleTopicBySchema(schema);

        List<String> columnFields = enableDecimal ?
                Arrays.asList("f1", "f2", "f3", "f4", "f5", "f6", "pk") : Arrays.asList("f1", "f2", "f3", "f4", "f5", "pk");


        config.setInsertMode(SinkMysqlConfig.InsertMode.OVERWRITE);
        client.createConnector(TEST_PROJECT_NAME, tupleTopicName, connectorType, columnFields, config);

        // put records
        List<RecordEntry> recordEntries = new ArrayList<>();
        for (int i = 0; i < RECORD_COUNT; ++i) {
            recordEntries.add(genData(schema, null));
        }
        PutRecordsResult putRecordsResult = client.putRecords(TEST_PROJECT_NAME, tupleTopicName, recordEntries);
        Assert.assertEquals(putRecordsResult.getFailedRecordCount(), 0);

        // wait for sink
        waitForAllShardSinked(tupleTopicName, MAX_SINK_TIMEOUT);

        // check data
        checkSinkedData(schema, columnFields);
    }

    @Test
    public void testUpdateConnector() {
        //TODO:
    }

    @Test
    public void testSinkWithNoPk() {
        RecordSchema schema = new RecordSchema() {{
            addField(new Field("f1", FieldType.STRING));
            addField(new Field("f2", FieldType.BIGINT));
            addField(new Field("f3", FieldType.DOUBLE));
            addField(new Field("f4", FieldType.TIMESTAMP));
            addField(new Field("f5", FieldType.BOOLEAN));
            if (enableDecimal) {
                addField(new Field("f6", FieldType.DECIMAL));
            }
            addField(new Field(PK_COL, FieldType.STRING));
        }};

        createTableBySchema(schema, false);
        createTupleTopicBySchema(schema);

        List<String> columnFields = enableDecimal ?
                Arrays.asList("f1", "f2", "f3", "f4", "f5", "f6", "pk") : Arrays.asList("f1", "f2", "f3", "f4", "f5", "pk");

        client.createConnector(TEST_PROJECT_NAME, tupleTopicName, connectorType, columnFields, config);

        // put records
        List<RecordEntry> recordEntries = new ArrayList<>();
        for (int i = 0; i < RECORD_COUNT; ++i) {
            recordEntries.add(genData(schema, null));
        }
        PutRecordsResult putRecordsResult = client.putRecords(TEST_PROJECT_NAME, tupleTopicName, recordEntries);
        Assert.assertEquals(putRecordsResult.getFailedRecordCount(), 0);

        waitForAllShardSinked(tupleTopicName, MAX_SINK_TIMEOUT);
        long dbCount = getDbCount();
        Assert.assertEquals(dbCount, RECORD_COUNT);

        long seqCount = 0;
        GetConnectorShardStatusResult getConnectorShardStatusResult = client.getConnectorShardStatus(TEST_PROJECT_NAME, tupleTopicName, connectorType);
        for (ConnectorShardStatusEntry entry : getConnectorShardStatusResult.getStatusEntryMap().values()) {
            seqCount += (entry.getCurrSequence() + 1);
        }
        Assert.assertEquals(seqCount, RECORD_COUNT);
    }

    @Test
    public void testSinkWithFieldValueNull() {
        RecordSchema schema = new RecordSchema() {{
            addField(new Field("f1", FieldType.STRING));
            addField(new Field("f2", FieldType.BIGINT));
            addField(new Field("f3", FieldType.DOUBLE));
            addField(new Field("f4", FieldType.TIMESTAMP));
            addField(new Field("f5", FieldType.BOOLEAN));
            if (enableDecimal) {
                addField(new Field("f6", FieldType.DECIMAL));
            }
            addField(new Field(PK_COL, FieldType.STRING));
        }};

        createTableBySchema(schema, true);
        createTupleTopicBySchema(schema);

        List<String> columnFields = enableDecimal ?
                Arrays.asList("f1", "f2", "f3", "f4", "f5", "f6", "pk") : Arrays.asList("f1", "f2", "f3", "f4", "f5", "pk");
        client.createConnector(TEST_PROJECT_NAME, tupleTopicName, connectorType, columnFields, config);

        // put records
        List<RecordEntry> recordEntries = new ArrayList<>();
        for (int i = 0; i < RECORD_COUNT; ++i) {
            recordEntries.add(genData(schema, Collections.singletonList("f1")));
        }

        PutRecordsResult putRecordsResult = client.putRecords(TEST_PROJECT_NAME, tupleTopicName, recordEntries);
        Assert.assertEquals(putRecordsResult.getFailedRecordCount(), 0);

        waitForAllShardSinked(tupleTopicName, MAX_SINK_TIMEOUT);

        // check data
        checkSinkedData(schema, columnFields);
    }

    /**
     * TODO: sinkMysql not support null
     */
    @Test(enabled = false)
    public void testSinkWithPkNull() {
        RecordSchema schema = new RecordSchema() {{
            addField(new Field("f1", FieldType.STRING));
            addField(new Field("f2", FieldType.BIGINT));
            addField(new Field("f3", FieldType.DOUBLE));
            addField(new Field("f4", FieldType.TIMESTAMP));
            addField(new Field("f5", FieldType.BOOLEAN));
            if (enableDecimal) {
                addField(new Field("f6", FieldType.DECIMAL));
            }
            addField(new Field(PK_COL, FieldType.STRING));
        }};

        createTableBySchema(schema, true);
        createTupleTopicBySchema(schema);

        List<String> columnFields = enableDecimal ?
                Arrays.asList("f1", "f2", "f3", "f4", "f5", "f6", "pk") : Arrays.asList("f1", "f2", "f3", "f4", "f5", "pk");
        client.createConnector(TEST_PROJECT_NAME, tupleTopicName, connectorType, columnFields, config);

        // put records
        List<RecordEntry> recordEntries = new ArrayList<>();
        for (int i = 0; i < RECORD_COUNT; ++i) {
            recordEntries.add(genData(schema, Collections.singletonList(PK_COL)));
        }

        PutRecordsResult putRecordsResult = client.putRecords(TEST_PROJECT_NAME, tupleTopicName, recordEntries);
        Assert.assertEquals(putRecordsResult.getFailedRecordCount(), 0);

        waitForAllShardSinked(tupleTopicName, MAX_SINK_TIMEOUT);

        long dbCount = getDbCount();
        Assert.assertEquals(dbCount, 0);
    }

    @Test
    public void testSinkWithNotAllFieldSinked() {
        RecordSchema schema = new RecordSchema() {{
            addField(new Field("f1", FieldType.STRING));
            addField(new Field("f2", FieldType.BIGINT));
            addField(new Field(PK_COL, FieldType.STRING));
        }};

        createTableBySchema(schema, true);
        schema.addField(new Field("f3", FieldType.DOUBLE));
        schema.addField(new Field("f4", FieldType.TIMESTAMP));
        schema.addField(new Field("f5", FieldType.BOOLEAN));
        if (enableDecimal) {
            schema.addField(new Field("f6", FieldType.DECIMAL));
        }
        createTupleTopicBySchema(schema);

        List<String> columnFields = Arrays.asList("f1", "f2", "pk");
        client.createConnector(TEST_PROJECT_NAME, tupleTopicName, connectorType, columnFields, config);

        // put records
        List<RecordEntry> recordEntries = new ArrayList<>();
        for (int i = 0; i < RECORD_COUNT; ++i) {
            recordEntries.add(genData(schema, null));
        }
        PutRecordsResult putRecordsResult = client.putRecords(TEST_PROJECT_NAME, tupleTopicName, recordEntries);
        Assert.assertEquals(putRecordsResult.getFailedRecordCount(), 0);

        // wait for sink
        waitForAllShardSinked(tupleTopicName, MAX_SINK_TIMEOUT);

        // check data
        checkSinkedData(schema, columnFields);
    }

    @Test
    public void testSinkWithColumnOrderNotSame() {
        RecordSchema schema = new RecordSchema() {{
            addField(new Field("f1", FieldType.STRING));
            addField(new Field("f2", FieldType.BIGINT));
            addField(new Field("f3", FieldType.DOUBLE));
            addField(new Field("f4", FieldType.TIMESTAMP));
            addField(new Field("f5", FieldType.BOOLEAN));
            if (enableDecimal) {
                addField(new Field("f6", FieldType.DECIMAL));
            }
            addField(new Field(PK_COL, FieldType.STRING));
        }};

        createTableBySchema(schema, true);
        createTupleTopicBySchema(schema);

        List<String> columnFields = enableDecimal ?
                Arrays.asList("f1", "pk", "f6", "f4", "f5", "f3", "f2") : Arrays.asList("f1", "pk", "f4", "f5", "f3", "f2");
        client.createConnector(TEST_PROJECT_NAME, tupleTopicName, connectorType, columnFields, config);

        // put records
        List<RecordEntry> recordEntries = new ArrayList<>();
        for (int i = 0; i < RECORD_COUNT; ++i) {
            recordEntries.add(genData(schema, null));
        }
        PutRecordsResult putRecordsResult = client.putRecords(TEST_PROJECT_NAME, tupleTopicName, recordEntries);
        Assert.assertEquals(putRecordsResult.getFailedRecordCount(), 0);

        // wait for sink
        waitForAllShardSinked(tupleTopicName, MAX_SINK_TIMEOUT);

        // check data
        checkSinkedData(schema, columnFields);
    }

    @Test
    public void testSinkWithColumnFieldSizeNotMatch() {
        RecordSchema schema = new RecordSchema() {{
            addField(new Field("f1", FieldType.STRING));
            addField(new Field("f2", FieldType.BIGINT));
            addField(new Field("f3", FieldType.DOUBLE));
            addField(new Field("f4", FieldType.TIMESTAMP));
            addField(new Field("f5", FieldType.BOOLEAN));
            if (enableDecimal) {
                addField(new Field("f6", FieldType.DECIMAL));
            }
            addField(new Field(PK_COL, FieldType.STRING));
        }};

        createTableBySchema(schema, true);
        createTupleTopicBySchema(schema);

        List<String> columnFields = enableDecimal ?
                Arrays.asList("f1", "f3", "f4", "f5", "f6", "pk") : Arrays.asList("f1", "f3", "f4", "f5", "pk");
        try {
            client.createConnector(TEST_PROJECT_NAME, tupleTopicName, connectorType, columnFields, config);
            fail("ColumnFieldSizeNotMatch should throw exception");
        } catch (DatahubClientException e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
            Assert.assertTrue(e.getMessage().contains("Check column size fail"));
        }
    }

    @Test
    public void testSinkWithFieldTypeNotMatch() {
        RecordSchema schema = new RecordSchema() {{
            addField(new Field("f1", FieldType.STRING));
            addField(new Field("f2", FieldType.BIGINT));
            addField(new Field("f3", FieldType.DOUBLE));
            addField(new Field("f4", FieldType.TIMESTAMP));
            addField(new Field("f5", FieldType.BOOLEAN));
            if (enableDecimal) {
                addField(new Field("f6", FieldType.DECIMAL));
            }
            addField(new Field(PK_COL, FieldType.STRING));
        }};
        createTableBySchema(schema, true);

        RecordSchema schema2 = new RecordSchema() {{
            addField(new Field("f1", FieldType.STRING));
            addField(new Field("f2", FieldType.BIGINT));
            addField(new Field("f3", FieldType.DOUBLE));
            addField(new Field("f4", FieldType.TIMESTAMP));
            addField(new Field("f5", FieldType.BOOLEAN));
            if (enableDecimal) {
                addField(new Field("f6", FieldType.DECIMAL));
            }
            addField(new Field(PK_COL, FieldType.BIGINT));
        }};
        createTupleTopicBySchema(schema2);

        List<String> columnFields = enableDecimal ?
                Arrays.asList("f1", "f2", "f3", "f4", "f5", "f6", "pk") : Arrays.asList("f1", "f2", "f3", "f4", "f5", "pk");
        try {
            client.createConnector(TEST_PROJECT_NAME, tupleTopicName, connectorType, columnFields, config);
            fail("FieldTypeNotMatch should throw exception");
        } catch (DatahubClientException e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
            Assert.assertTrue(e.getMessage().contains("Check column type fail"));
        }
    }

    @Test
    public void testSinkWithFieldNameNotMatch() {
        RecordSchema schema = new RecordSchema() {{
            addField(new Field("f1", FieldType.STRING));
            addField(new Field("f2", FieldType.BIGINT));
            addField(new Field("f3", FieldType.DOUBLE));
            addField(new Field("f4", FieldType.TIMESTAMP));
            addField(new Field("f5", FieldType.BOOLEAN));
            addField(new Field(PK_COL, FieldType.STRING));
        }};
        createTableBySchema(schema, true);

        RecordSchema schema2 = new RecordSchema() {{
            addField(new Field("f1", FieldType.STRING));
            addField(new Field("f2", FieldType.BIGINT));
            addField(new Field("f3", FieldType.DOUBLE));
            addField(new Field("f4", FieldType.TIMESTAMP));
            addField(new Field("f7", FieldType.BOOLEAN));
            addField(new Field(PK_COL, FieldType.STRING));
        }};
        createTupleTopicBySchema(schema2);

        List<String> columnFields = Arrays.asList("f1", "f2", "f3", "f4", "f5", "pk");
        try {
            client.createConnector(TEST_PROJECT_NAME, tupleTopicName, connectorType, columnFields, config);
            fail("FieldNameNotMatch should throw exception");
        } catch (DatahubClientException e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
            Assert.assertTrue(e.getMessage().contains("Datahub not have field:f5"));
        }
    }

    @Test
    public void testSinkWithDbMoreField() {
        RecordSchema schema = new RecordSchema() {{
            addField(new Field("f1", FieldType.STRING));
            addField(new Field("f2", FieldType.BIGINT));
            addField(new Field("f3", FieldType.DOUBLE));
            addField(new Field("f4", FieldType.TIMESTAMP));
            addField(new Field("f5", FieldType.BOOLEAN));
            addField(new Field("f6", FieldType.STRING));
            addField(new Field(PK_COL, FieldType.STRING));
        }};
        createTableBySchema(schema, true);

        RecordSchema schema2 = new RecordSchema() {{
            addField(new Field("f1", FieldType.STRING));
            addField(new Field("f2", FieldType.BIGINT));
            addField(new Field("f3", FieldType.DOUBLE));
            addField(new Field("f4", FieldType.TIMESTAMP));
            addField(new Field("f5", FieldType.BOOLEAN));
        }};
        createTupleTopicBySchema(schema2);

        List<String> columnFields = Arrays.asList("f1", "f2", "f3", "f4", "f5", "f6", "pk");
        try {
            client.createConnector(TEST_PROJECT_NAME, tupleTopicName, connectorType, columnFields, config);
            fail("DbMoreField should throw exception");
        } catch (DatahubClientException e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
            Assert.assertTrue(e.getMessage().contains("Datahub not have field:f6"));
        }
    }

    @Test
    public void testSinkWithAppendDatahubField() {
        RecordSchema schema = new RecordSchema() {{
            addField(new Field("f1", FieldType.STRING));
            addField(new Field("f2", FieldType.BIGINT));
            addField(new Field("f3", FieldType.DOUBLE));
            addField(new Field("f4", FieldType.TIMESTAMP));
            addField(new Field("f5", FieldType.BOOLEAN));
            addField(new Field(PK_COL, FieldType.STRING));
        }};
        createTupleTopicBySchema(schema);

        // put records, this records with be discard
        List<RecordEntry> recordEntries = new ArrayList<>();
        for (int i = 0; i < RECORD_COUNT; ++i) {
            recordEntries.add(genData(schema, null));
        }
        PutRecordsResult putRecordsResult = client.putRecords(TEST_PROJECT_NAME, tupleTopicName, recordEntries);
        Assert.assertEquals(putRecordsResult.getFailedRecordCount(), 0);

        // append datahub field
        client.appendField(TEST_PROJECT_NAME, tupleTopicName, new Field("f6", FieldType.STRING));

        // create table
        schema.addField(new Field("f6", FieldType.STRING));
        createTableBySchema(schema, true);

        // create connector
        List<String> columnFields = Arrays.asList("f1", "f2", "f3", "f4", "f5", "f6", "pk");
        client.createConnector(TEST_PROJECT_NAME, tupleTopicName, connectorType, columnFields, config);

        recordEntries.clear();
        for (int i = 0; i < RECORD_COUNT; ++i) {
            recordEntries.add(genData(schema, null));
        }
        putRecordsResult = client.putRecords(TEST_PROJECT_NAME, tupleTopicName, recordEntries);
        Assert.assertEquals(putRecordsResult.getFailedRecordCount(), 0);

        // wait for sink
        waitForAllShardSinked(tupleTopicName, MAX_SINK_TIMEOUT);

        long total = 0;
        long discardCount = 0;
        GetConnectorShardStatusResult getConnectorShardStatusResult = client.getConnectorShardStatus(TEST_PROJECT_NAME, tupleTopicName, connectorType);
        for (ConnectorShardStatusEntry entry : getConnectorShardStatusResult.getStatusEntryMap().values()) {
            discardCount += entry.getDiscardCount();
            total += entry.getCurrSequence() + 1;
        }
        // support old data
        Assert.assertEquals(discardCount, 0);
        Assert.assertEquals(total, RECORD_COUNT*2);
        // check data
        checkSinkedData(schema, columnFields);
    }
}
