package com.aliyun.datahub.client.e2e;

import com.aliyun.datahub.client.e2e.common.Configure;
import com.aliyun.datahub.client.e2e.common.Constant;
import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.exception.InvalidParameterException;
import com.aliyun.datahub.client.model.*;
import com.aliyun.odps.*;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.junit.Assert.fail;

@Test
public class SinkOdpsTest extends BaseConnectorTest {
    private SinkOdpsConfig config;
    private Odps odps;
    private TableTunnel tunnel;
    private String odpsProject = Configure.getString(Constant.ODPS_PROJECT);
    private String odpsTable;
    private static final long TIMESTAMP = System.currentTimeMillis() * 1000;
    private static boolean hasRowKey = true;

    @BeforeClass
    public static void setUpBeforeClass() {
        sUseNewExecutor = false; // now not support v2
        BaseConnectorTest.setUpBeforeClass();
    }

    @BeforeMethod
    @Override
    public void setUpBeforeMethod() {
        super.setUpBeforeMethod();
        odpsTable = tupleTopicName;
        connectorType = ConnectorType.SINK_ODPS;
        config = new SinkOdpsConfig() {{
            setEndpoint(Configure.getString(Constant.ODPS_ENDPOINT));
            setTunnelEndpoint(Configure.getString(Constant.ODPS_TUNNEL_ENDPOINT, null));
            setProject(odpsProject);
            setTable(odpsTable);
            setAccessId(Configure.getString(Constant.ODPS_ACCESS_ID));
            setAccessKey(Configure.getString(Constant.ODPS_ACCESS_KEY));
            setPartitionMode(PartitionMode.USER_DEFINE);
        }};

        odps = new Odps(new AliyunAccount(config.getAccessId(), config.getAccessKey())) {{
            setEndpoint(config.getEndpoint());
            setDefaultProject(config.getProject());
        }};
        tunnel = new TableTunnel(odps);
        hasRowKey = true;
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

        // delete odps tables;
        dropOpdsTable(odpsTable);
    }

    private void createOdpsTable(TableSchema odpsSchema) {
        try {
            odps.tables().create(odpsProject, odpsTable, odpsSchema);
        } catch (OdpsException e) {
            throw new DatahubClientException(e.getMessage());
        }
    }

    private void dropOpdsTable(String odpsTable) {
        try {
            odps.tables().delete(odpsProject, odpsTable);
        } catch (Exception e) {
            //
        }
    }

    private void putRecords(RecordSchema schema) {
        putRecords(schema, new HashSet<String>());
    }

    private void putRecords(RecordSchema schema, Set<String> nullCol) {
        // put records
        List<RecordEntry> recordEntries = new ArrayList<>();
        for (int i = 0; i < RECORD_COUNT; ++i) {
            recordEntries.add(genData(schema, -1, nullCol));
        }
        PutRecordsResult putRecordsResult = client.putRecords(TEST_PROJECT_NAME, tupleTopicName, recordEntries);
        Assert.assertEquals(putRecordsResult.getFailedRecordCount(), 0);
    }

    private void putRecordsByShard(RecordSchema schema) {
        ListShardResult shards = client.listShard(TEST_PROJECT_NAME, tupleTopicName);
        for (ShardEntry shard : shards.getShards()) {
            if (shard.getState() == ShardState.ACTIVE) {
                // put records
                List<RecordEntry> recordEntries = new ArrayList<>();
                for (int i = 0; i < RECORD_COUNT; ++i) {
                    RecordEntry entry = genData(schema, -1, new HashSet<String>());
                    entry.setShardId(shard.getShardId());
                    recordEntries.add(entry);
                }
                PutRecordsResult putRecordsResult = client.putRecords(TEST_PROJECT_NAME, tupleTopicName, recordEntries);
                Assert.assertEquals(putRecordsResult.getFailedRecordCount(), 0);
            }
        }

    }

    private void putRecordsWithEventTime(RecordSchema schema, long eventTime) {
        List<RecordEntry> recordEntries = new ArrayList<>();
        for (int i = 0; i < RECORD_COUNT; ++i) {
            recordEntries.add(genData(schema, eventTime, new HashSet<String>()));
        }
        PutRecordsResult putRecordsResult = client.putRecords(TEST_PROJECT_NAME, tupleTopicName, recordEntries);
        Assert.assertEquals(putRecordsResult.getFailedRecordCount(), 0);
    }

    private RecordEntry genData(RecordSchema schema, long eventTime, Set<String> nullCol) {
        final TupleRecordData recordData = new TupleRecordData(schema);

        for (Field field : schema.getFields()) {
            String fieldName = field.getName();
            if (nullCol.contains(fieldName)) {
                continue;
            }
            if (fieldName.equals("event_time")) {
                recordData.setField(fieldName, eventTime);
                continue;
            }
            switch (field.getType()) {
                case STRING:
                    recordData.setField(fieldName, String.valueOf(TIMESTAMP));
                    break;
                case BOOLEAN:
                    recordData.setField(fieldName, false);
                    break;
                case DOUBLE:
                    recordData.setField(fieldName, (double)TIMESTAMP);
                    break;
                case TIMESTAMP:
                    recordData.setField(fieldName, TIMESTAMP);
                    break;
                case BIGINT:
                    recordData.setField(fieldName, TIMESTAMP);
                    break;
                case DECIMAL:
                    recordData.setField(fieldName, BigDecimal.valueOf(Math.sqrt(TIMESTAMP)));
                    break;
            }
        }

        RecordEntry recordEntry = new RecordEntry() {{ setRecordData(recordData); }};
        recordEntry.setPartitionKey(String.valueOf(random.nextInt(RECORD_COUNT)));
        return recordEntry;
    }

    private RecordEntry genInvalidPartitionData(RecordSchema schema) {
        final TupleRecordData recordData = new TupleRecordData(schema);

        for (Field field : schema.getFields()) {
            String fieldName = field.getName();
            switch (field.getType()) {
                case STRING:
                    recordData.setField(fieldName, "*");
                    break;
                case BOOLEAN:
                    recordData.setField(fieldName, false);
                    break;
                case DOUBLE:
                    recordData.setField(fieldName, (double)TIMESTAMP);
                    break;
                case TIMESTAMP:
                    recordData.setField(fieldName, TIMESTAMP);
                    break;
                case BIGINT:
                    recordData.setField(fieldName, TIMESTAMP);
                    break;
                case DECIMAL:
                    recordData.setField(fieldName, BigDecimal.valueOf(Math.sqrt(TIMESTAMP)));
                    break;
            }
        }

        RecordEntry recordEntry = new RecordEntry() {{ setRecordData(recordData); }};
        recordEntry.setPartitionKey(String.valueOf(random.nextInt(RECORD_COUNT)));
        return recordEntry;
    }

    private void putInvalidRecords(RecordSchema schema) {
        List<RecordEntry> recordEntries = new ArrayList<>();
        for (int i = 0; i < RECORD_COUNT; ++i) {
            recordEntries.add(genInvalidPartitionData(schema));
        }
        PutRecordsResult putRecordsResult = client.putRecords(TEST_PROJECT_NAME, tupleTopicName, recordEntries);
        Assert.assertEquals(putRecordsResult.getFailedRecordCount(), 0);
    }

    private void checkSinkedData(TableSchema odpsSchema, long recordCount) {
        PartitionSpec partitionSpec = null;
        if (!odpsSchema.getPartitionColumns().isEmpty()) {
            partitionSpec = new PartitionSpec();
            for (Column column : odpsSchema.getPartitionColumns()) {
                partitionSpec.set(column.getName(), String.valueOf(TIMESTAMP));
            }
        }

        try {
            downloadAndCheckData(odpsSchema, partitionSpec, recordCount);
        } catch (Exception e) {
            throw new DatahubClientException(e.getMessage());
        }
    }

    private void downloadAndCheckRecordCount(TableSchema odpsSchema, PartitionSpec partitionSpec, long recordCount) throws TunnelException, IOException {
        TableTunnel.DownloadSession down = null;
        if (odpsSchema.getPartitionColumns().isEmpty()) {
            down = tunnel.createDownloadSession(odpsProject, odpsTable);
        } else {
            down = tunnel.createDownloadSession(odpsProject, odpsTable, partitionSpec);
        }

        TableSchema schema = down.getSchema();
        long count = down.getRecordCount();
        Assert.assertEquals(recordCount, count);
    }

    private void downloadAndCheckData(TableSchema odpsSchema, PartitionSpec partitionSpec, long recordCount) throws TunnelException, IOException {
        TableTunnel.DownloadSession down = null;
        if (odpsSchema.getPartitionColumns().isEmpty()) {
            down = tunnel.createDownloadSession(odpsProject, odpsTable);
        } else {
            down = tunnel.createDownloadSession(odpsProject, odpsTable, partitionSpec);
        }

        TableSchema schema = down.getSchema();
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
    }

    private void checkData(Record r, TableSchema schema) {
        for (Column i : schema.getColumns()) {
            if (i.getName().equals("__system_rowkey__")) {
                Assert.assertEquals(r.getString(i.getName()) != null, hasRowKey);
            } else {
                switch (i.getType()) {
                    case BIGINT:
                        Assert.assertEquals(r.getBigint(i.getName()), Long.valueOf(TIMESTAMP));
                        break;
                    case BOOLEAN:
                        Assert.assertEquals(r.getBoolean(i.getName()), Boolean.valueOf(false));
                        break;
                    case STRING:
                        Assert.assertEquals(r.getString(i.getName()), String.valueOf(TIMESTAMP));
                        break;
                    case DATETIME:
                        Assert.assertEquals(r.getDatetime(i.getName()), new Date(TIMESTAMP/1000));
                        break;
                    case DOUBLE:
                        Assert.assertEquals(r.getDouble(i.getName()), (double)TIMESTAMP);
                        break;
                    case DECIMAL:
                        BigDecimal bd = BigDecimal.valueOf(Math.sqrt(TIMESTAMP));
                        if (bd.scale() > 18) {
                            bd = bd.setScale(18, BigDecimal.ROUND_HALF_UP);
                        }
                        Assert.assertEquals(r.getDecimal(i.getName()), bd);
                        break;
                }
            }
        }
    }

    @Test
    public void testNormalWithUserDefine() {
        RecordSchema schema = new RecordSchema() {{
            addField(new Field("f1", FieldType.STRING));
            addField(new Field("f2", FieldType.BIGINT));
            addField(new Field("f3", FieldType.DOUBLE));
            addField(new Field("f4", FieldType.TIMESTAMP));
            addField(new Field("f5", FieldType.BOOLEAN));
            addField(new Field("f6", FieldType.DECIMAL));
            addField(new Field("pt", FieldType.STRING));
            addField(new Field("ct", FieldType.STRING));
        }};

        createTupleTopicBySchema(schema);
        TableSchema odpsSchema = new TableSchema();
        odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
        odpsSchema.addColumn(new Column("f2", OdpsType.BIGINT));
        odpsSchema.addColumn(new Column("f3", OdpsType.DOUBLE));
        odpsSchema.addColumn(new Column("f4", OdpsType.DATETIME));
        odpsSchema.addColumn(new Column("f5", OdpsType.BOOLEAN));
        odpsSchema.addColumn(new Column("f6", OdpsType.DECIMAL));
        odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
        odpsSchema.addPartitionColumn(new Column("ct", OdpsType.STRING));
        createOdpsTable(odpsSchema);

        List<String> columnFields = Arrays.asList("f1", "f2", "f3", "f4", "f5", "f6", "pt", "ct");
        config.setPartitionMode(SinkOdpsConfig.PartitionMode.USER_DEFINE);
        client.createConnector(TEST_PROJECT_NAME, tupleTopicName, connectorType, columnFields, config);

        sleepInMs(MAX_OPERATOR_INTERVAL);
        // get connector
        GetConnectorResult getConnectorResult = client.getConnector(TEST_PROJECT_NAME, tupleTopicName, connectorType);
        SinkOdpsConfig getConfig = (SinkOdpsConfig)getConnectorResult.getConfig();
        Assert.assertEquals(getConfig.getEndpoint(), config.getEndpoint());
        Assert.assertEquals(getConfig.getProject(), config.getProject());
        Assert.assertEquals(getConfig.getTable(), config.getTable());
        Assert.assertEquals(getConfig.getPartitionMode(), SinkOdpsConfig.PartitionMode.USER_DEFINE);
        Assert.assertEquals(getConnectorResult.getColumnFields(), columnFields);
        Assert.assertEquals(getConnectorResult.getState(), ConnectorState.RUNNING);

        // put records
        putRecords(schema);

        // wait for sink
        waitForAllShardSinked(tupleTopicName, MAX_SINK_TIMEOUT);

        // check data
        checkSinkedData(odpsSchema, RECORD_COUNT);


        // stop connector
        client.updateConnectorState(TEST_PROJECT_NAME, tupleTopicName, connectorType, ConnectorState.STOPPED);
        sleepInMs(MAX_OPERATOR_INTERVAL);

        getConnectorResult = client.getConnector(TEST_PROJECT_NAME, tupleTopicName, connectorType);
        Assert.assertEquals(getConnectorResult.getState(), ConnectorState.STOPPED);

        GetConnectorShardStatusResult getConnectorShardStatusResult = client.getConnectorShardStatus(TEST_PROJECT_NAME, tupleTopicName, connectorType);

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
        putRecords(schema);

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
    public void testSinkWithEventTimeMode() throws Exception {
        // create topic
        RecordSchema schema = new RecordSchema() {{
            addField(new Field("f1", FieldType.STRING));
            addField(new Field("event_time", FieldType.TIMESTAMP));
        }};
        createTupleTopicBySchema(schema);

        // create odps table
        TableSchema odpsSchema = new TableSchema();
        odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
        odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
        odpsSchema.addPartitionColumn(new Column("ct", OdpsType.STRING));
        createOdpsTable(odpsSchema);

        // create connector
        List<String> columnFields = new ArrayList<String>() {{
            add("f1");
        }};

        int timeRange = 60;
        config.setPartitionMode(SinkOdpsConfig.PartitionMode.EVENT_TIME);
        config.setTimeRange(timeRange);
        SinkOdpsConfig.PartitionConfig partitionConfig = new SinkOdpsConfig.PartitionConfig() {{
            addConfig("pt", "%Y%m%d");
            addConfig("ct", "%H%M");
        }};
        config.setPartitionConfig(partitionConfig);
        client.createConnector(TEST_PROJECT_NAME, tupleTopicName, ConnectorType.SINK_ODPS, columnFields, config);

        // get connector
        GetConnectorResult getConnectorResult = client.getConnector(TEST_PROJECT_NAME, tupleTopicName, ConnectorType.SINK_ODPS);
        Assert.assertEquals(columnFields, getConnectorResult.getColumnFields());

        Random random = new Random();
        long nextRandom = random.nextInt(3600 * 24);
        long eventTime = System.currentTimeMillis() / 1000 - nextRandom;
        eventTime = (eventTime - eventTime % (timeRange * 60)) * 1000;
        // put records
        putRecordsWithEventTime(schema, eventTime * 1000);

        nextRandom = random.nextInt(3600 * 24);
        long eventTime2 = System.currentTimeMillis() / 1000 - nextRandom;
        eventTime2 = (eventTime2 - eventTime2 % (timeRange * 60)) * 1000;
        putRecordsWithEventTime(schema, eventTime2 * 1000);

        // wait for sinked
        waitForAllShardSinked(tupleTopicName, MAX_SINK_TIMEOUT);

        // check odps data
        // check eventTime1
        PartitionSpec partitionSpec = new PartitionSpec();
        SimpleDateFormat pt = new SimpleDateFormat("yyyyMMdd");
        partitionSpec.set("pt", pt.format(new Date(eventTime)));
        SimpleDateFormat ct = new SimpleDateFormat("HHmm");
        partitionSpec.set("ct", ct.format(new Date(eventTime)));
        downloadAndCheckData(odpsSchema, partitionSpec, RECORD_COUNT);
        // check eventTime2
        partitionSpec = new PartitionSpec();
        pt = new SimpleDateFormat("yyyyMMdd");
        partitionSpec.set("pt", pt.format(new Date(eventTime2)));
        ct = new SimpleDateFormat("HHmm");
        partitionSpec.set("ct", ct.format(new Date(eventTime2)));
        downloadAndCheckData(odpsSchema, partitionSpec, RECORD_COUNT);
    }

    @Test
    public void testSinkWithSystemTimeMode() throws Exception {
        List<String> columnFields = new ArrayList<String>();
        columnFields.add("f1");

        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("f1", FieldType.STRING));
        createTupleTopicBySchema(schema);

        TableSchema odpsSchema = new TableSchema();
        odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
        odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
        odpsSchema.addPartitionColumn(new Column("ct", OdpsType.STRING));
        createOdpsTable(odpsSchema);

        int timeRange = 60;
        config.setPartitionMode(SinkOdpsConfig.PartitionMode.SYSTEM_TIME);
        config.setTimeRange(60);
        SinkOdpsConfig.PartitionConfig partitionConfig = new SinkOdpsConfig.PartitionConfig() {{
            addConfig("pt", "%Y%m%d");
            addConfig("ct", "%H%M");
        }};
        config.setPartitionConfig(partitionConfig);
        client.createConnector(TEST_PROJECT_NAME, tupleTopicName, ConnectorType.SINK_ODPS, columnFields, config);

        long currentTime = System.currentTimeMillis()/1000;
        currentTime = (currentTime - currentTime%(timeRange*60)) * 1000;
        // put records
        List<RecordEntry> recordEntries = new ArrayList<>();
        for (int i = 0; i < RECORD_COUNT; ++i) {
            recordEntries.add(genData(schema, -1, new HashSet<String>()));
        }
        PutRecordsResult putRecordsResult = client.putRecords(TEST_PROJECT_NAME, tupleTopicName, recordEntries);
        Assert.assertEquals(putRecordsResult.getFailedRecordCount(), 0);

        waitForAllShardSinked(tupleTopicName, MAX_SINK_TIMEOUT);

        PartitionSpec partitionSpec = new PartitionSpec();
        SimpleDateFormat pt = new SimpleDateFormat("yyyyMMdd");
        partitionSpec.set("pt", pt.format(new Date(currentTime)));
        SimpleDateFormat ct = new SimpleDateFormat("HHmm");
        partitionSpec.set("ct", ct.format(new Date(currentTime)));
        System.out.println(partitionSpec.toString());
        downloadAndCheckData(odpsSchema, partitionSpec, RECORD_COUNT);
    }

    @Test
    public void testSinkAfterAppendField() throws Exception {
        List<String> columnFields = new ArrayList<String>();
        columnFields.add("f1");

        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("f1", FieldType.STRING));
        createTupleTopicBySchema(schema);

        TableSchema odpsSchema = new TableSchema();
        odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
        odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
        odpsSchema.addPartitionColumn(new Column("ct", OdpsType.STRING));
        createOdpsTable(odpsSchema);

        int timeRange = 60;
        config.setPartitionMode(SinkOdpsConfig.PartitionMode.EVENT_TIME);
        config.setTimeRange(60);

        SinkOdpsConfig.PartitionConfig partitionConfig = new SinkOdpsConfig.PartitionConfig() {{
            addConfig("pt", "%Y%m%d");
            addConfig("ct", "%H%M");
        }};
        config.setPartitionConfig(partitionConfig);

        long currentSystemTime = System.currentTimeMillis()/1000;
        currentSystemTime = (currentSystemTime - currentSystemTime%(timeRange*60)) * 1000;

        putRecords(schema);
        schema.addField(new Field("event_time", FieldType.TIMESTAMP));
        client.appendField(TEST_PROJECT_NAME, tupleTopicName, new Field("event_time", FieldType.TIMESTAMP));

        Random rand = new Random();
        // rand in one day
        long randSecond = rand.nextInt(3600 * 24);
        long currentTime = System.currentTimeMillis()/1000 - randSecond;
        currentTime = (currentTime - currentTime%(timeRange*60)) * 1000;
        putRecordsWithEventTime(schema, currentTime*1000);

        client.createConnector(TEST_PROJECT_NAME, tupleTopicName, ConnectorType.SINK_ODPS, columnFields, config);
        waitForAllShardSinked(tupleTopicName, MAX_SINK_TIMEOUT);

        // default use system_time
        PartitionSpec partitionSpec = new PartitionSpec();
        SimpleDateFormat pt = new SimpleDateFormat("yyyyMMdd");
        partitionSpec.set("pt", pt.format(new Date(currentSystemTime)));
        SimpleDateFormat ct = new SimpleDateFormat("HHmm");
        partitionSpec.set("ct", ct.format(new Date(currentSystemTime)));
        downloadAndCheckData(odpsSchema, partitionSpec, RECORD_COUNT);

        // below use event_time
        partitionSpec = new PartitionSpec();
        pt = new SimpleDateFormat("yyyyMMdd");
        partitionSpec.set("pt", pt.format(new Date(currentTime)));
        ct = new SimpleDateFormat("HHmm");
        partitionSpec.set("ct", ct.format(new Date(currentTime)));
        downloadAndCheckData(odpsSchema, partitionSpec, RECORD_COUNT);
    }

    @Test
    public void testSinkWithPartitionNotSet() throws Exception {
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
        createTupleTopicBySchema(schema);

        TableSchema odpsSchema = new TableSchema();
        odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
        odpsSchema.addColumn(new Column("f2", OdpsType.BIGINT));
        odpsSchema.addColumn(new Column("f3", OdpsType.DOUBLE));
        odpsSchema.addColumn(new Column("f4", OdpsType.DATETIME));
        odpsSchema.addColumn(new Column("f5", OdpsType.BOOLEAN));
        odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
        odpsSchema.addPartitionColumn(new Column("ct", OdpsType.STRING));
        createOdpsTable(odpsSchema);

        try {
            config.setPartitionMode(SinkOdpsConfig.PartitionMode.USER_DEFINE);
            client.createConnector(TEST_PROJECT_NAME, tupleTopicName, ConnectorType.SINK_ODPS, columnFields, config);
            fail("Create connector with partition not set should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }
    }

    @Test
    public void testSinkWithPartitionNotMatch() throws Exception {
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
        createTupleTopicBySchema(schema);

        TableSchema odpsSchema = new TableSchema();
        odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
        odpsSchema.addColumn(new Column("f2", OdpsType.BIGINT));
        odpsSchema.addColumn(new Column("f3", OdpsType.DOUBLE));
        odpsSchema.addColumn(new Column("f4", OdpsType.DATETIME));
        odpsSchema.addColumn(new Column("f5", OdpsType.BOOLEAN));
        odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
        odpsSchema.addPartitionColumn(new Column("ct", OdpsType.STRING));
        createOdpsTable(odpsSchema);

        try {
            config.setPartitionMode(SinkOdpsConfig.PartitionMode.USER_DEFINE);
            client.createConnector(TEST_PROJECT_NAME, tupleTopicName, ConnectorType.SINK_ODPS, columnFields, config);
            fail("Create connector with partition not match should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }
    }

    @Test
    public void testSinkWithPartitionNotExist() throws Exception {
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
        createTupleTopicBySchema(schema);

        TableSchema odpsSchema = new TableSchema();
        odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
        odpsSchema.addColumn(new Column("f2", OdpsType.BIGINT));
        odpsSchema.addColumn(new Column("f3", OdpsType.DOUBLE));
        odpsSchema.addColumn(new Column("f4", OdpsType.DATETIME));
        odpsSchema.addColumn(new Column("f5", OdpsType.BOOLEAN));
        odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
        odpsSchema.addPartitionColumn(new Column("ct", OdpsType.STRING));
        createOdpsTable(odpsSchema);

        try {
            config.setPartitionMode(SinkOdpsConfig.PartitionMode.USER_DEFINE);
            client.createConnector(TEST_PROJECT_NAME, tupleTopicName, ConnectorType.SINK_ODPS, columnFields, config);
            fail("Create connector with partition not exist should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }
    }

    @Test
    public void testSinkWithColumnOrderNotMatchOdps() throws Exception {
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
        createTupleTopicBySchema(schema);

        TableSchema odpsSchema = new TableSchema();
        odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
        odpsSchema.addColumn(new Column("f2", OdpsType.BIGINT));
        odpsSchema.addColumn(new Column("f3", OdpsType.DOUBLE));
        odpsSchema.addColumn(new Column("f4", OdpsType.DATETIME));
        odpsSchema.addColumn(new Column("f5", OdpsType.BOOLEAN));
        odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
        odpsSchema.addPartitionColumn(new Column("ct", OdpsType.STRING));
        createOdpsTable(odpsSchema);

        config.setPartitionMode(SinkOdpsConfig.PartitionMode.USER_DEFINE);
        client.createConnector(TEST_PROJECT_NAME, tupleTopicName, ConnectorType.SINK_ODPS, columnFields, config);
    }

    @Test
    public void testSinkWithSubUser() throws Exception {
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
        createTupleTopicBySchema(schema);

        TableSchema odpsSchema = new TableSchema();
        odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
        odpsSchema.addColumn(new Column("f2", OdpsType.BIGINT));
        odpsSchema.addColumn(new Column("f3", OdpsType.DOUBLE));
        odpsSchema.addColumn(new Column("f4", OdpsType.DATETIME));
        odpsSchema.addColumn(new Column("f5", OdpsType.BOOLEAN));
        odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
        odpsSchema.addPartitionColumn(new Column("ct", OdpsType.STRING));
        createOdpsTable(odpsSchema);

        try {
            config.setPartitionMode(SinkOdpsConfig.PartitionMode.USER_DEFINE);
            config.setAccessId(Configure.getString(Constant.DATAHUB_SUBUSER_ACCESS_ID));
            config.setAccessKey(Configure.getString(Constant.DATAHUB_SUBUSER_ACCESS_KEY));
            client.createConnector(TEST_PROJECT_NAME, tupleTopicName, ConnectorType.SINK_ODPS, columnFields, config);
            fail("Create connector with subuser should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof DatahubClientException);
            Assert.assertTrue(e.getMessage().contains("CreateInstance permission"));
        }
    }

    @Test
    public void testSinkWithPartitionOrderNotMatchOdps() throws Exception {
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
        createTupleTopicBySchema(schema);

        TableSchema odpsSchema = new TableSchema();
        odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
        odpsSchema.addColumn(new Column("f2", OdpsType.BIGINT));
        odpsSchema.addColumn(new Column("f3", OdpsType.DOUBLE));
        odpsSchema.addColumn(new Column("f4", OdpsType.DATETIME));
        odpsSchema.addColumn(new Column("f5", OdpsType.BOOLEAN));
        odpsSchema.addPartitionColumn(new Column("ct", OdpsType.STRING));
        odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
        createOdpsTable(odpsSchema);

        config.setPartitionMode(SinkOdpsConfig.PartitionMode.USER_DEFINE);
        client.createConnector(TEST_PROJECT_NAME, tupleTopicName, ConnectorType.SINK_ODPS, columnFields, config);
    }

    @Test
    public void testSinkWithPartitionNotMatchOdps() throws Exception {
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
        createTupleTopicBySchema(schema);

        TableSchema odpsSchema = new TableSchema();
        odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
        odpsSchema.addColumn(new Column("f2", OdpsType.BIGINT));
        odpsSchema.addColumn(new Column("f3", OdpsType.DOUBLE));
        odpsSchema.addColumn(new Column("f4", OdpsType.DATETIME));
        odpsSchema.addColumn(new Column("f5", OdpsType.BOOLEAN));
        odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
        createOdpsTable(odpsSchema);

        try {
            config.setPartitionMode(SinkOdpsConfig.PartitionMode.USER_DEFINE);
            client.createConnector(TEST_PROJECT_NAME, tupleTopicName, ConnectorType.SINK_ODPS, columnFields, config);
            fail("Create connector with partition not match odps should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }
    }

    @Test
    public void testSinkWithOdpsPartitionFiledNotString() throws Exception {
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
        createTupleTopicBySchema(schema);

        TableSchema odpsSchema = new TableSchema();
        odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
        odpsSchema.addColumn(new Column("f2", OdpsType.BIGINT));
        odpsSchema.addColumn(new Column("f3", OdpsType.DOUBLE));
        odpsSchema.addColumn(new Column("f4", OdpsType.DATETIME));
        odpsSchema.addColumn(new Column("f5", OdpsType.BOOLEAN));
        odpsSchema.addPartitionColumn(new Column("pt", OdpsType.BIGINT));
        createOdpsTable(odpsSchema);

        config.setPartitionMode(SinkOdpsConfig.PartitionMode.USER_DEFINE);
        client.createConnector(TEST_PROJECT_NAME, tupleTopicName, ConnectorType.SINK_ODPS, columnFields, config);
    }

    @Test
    public void testSinkWithPartitionFiledNotString() throws Exception {
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
        createTupleTopicBySchema(schema);

        TableSchema odpsSchema = new TableSchema();
        odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
        odpsSchema.addColumn(new Column("f2", OdpsType.BIGINT));
        odpsSchema.addColumn(new Column("f3", OdpsType.DOUBLE));
        odpsSchema.addColumn(new Column("f4", OdpsType.DATETIME));
        odpsSchema.addColumn(new Column("f5", OdpsType.BOOLEAN));
        odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
        createOdpsTable(odpsSchema);
        try {
            config.setPartitionMode(SinkOdpsConfig.PartitionMode.USER_DEFINE);
            client.createConnector(TEST_PROJECT_NAME, tupleTopicName, ConnectorType.SINK_ODPS, columnFields, config);
            fail("testPartitionFiledNotString should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }
    }

    @Test
    public void testSinkWithFieldNotExit() throws Exception {
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
        createTupleTopicBySchema(schema);

        TableSchema odpsSchema = new TableSchema();
        odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
        odpsSchema.addColumn(new Column("f2", OdpsType.BIGINT));
        odpsSchema.addColumn(new Column("f3", OdpsType.DOUBLE));
        odpsSchema.addColumn(new Column("f4", OdpsType.DATETIME));
        odpsSchema.addColumn(new Column("f5", OdpsType.BOOLEAN));
        odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
        createOdpsTable(odpsSchema);

        try {
            config.setPartitionMode(SinkOdpsConfig.PartitionMode.USER_DEFINE);
            client.createConnector(TEST_PROJECT_NAME, tupleTopicName, ConnectorType.SINK_ODPS, columnFields, config);
            fail("testFieldNotExit should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }
    }

    @Test
    public void testSinkWithTableColSizeNotMatch() throws Exception {
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
        createTupleTopicBySchema(schema);

        TableSchema odpsSchema = new TableSchema();
        odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
        odpsSchema.addColumn(new Column("f2", OdpsType.BIGINT));
        odpsSchema.addColumn(new Column("f3", OdpsType.DOUBLE));
        odpsSchema.addColumn(new Column("f4", OdpsType.DATETIME));
        odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
        createOdpsTable(odpsSchema);

        try {
            config.setPartitionMode(SinkOdpsConfig.PartitionMode.USER_DEFINE);
            client.createConnector(TEST_PROJECT_NAME, tupleTopicName, ConnectorType.SINK_ODPS, columnFields, config);
            fail("testTableColSizeNotMatch should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }
    }

    @Test
    public void testSinkWithTableColSizeNotMatch2() throws Exception {
        List<String> columnFields = new ArrayList<String>();
        columnFields.add("f1");

        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("f1", FieldType.STRING));
        createTupleTopicBySchema(schema);

        TableSchema odpsSchema = new TableSchema();
        odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
        odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
        odpsSchema.addPartitionColumn(new Column("ct", OdpsType.STRING));
        odpsSchema.addColumn(new Column("f2", OdpsType.STRING));
        createOdpsTable(odpsSchema);

        int timeRange = 60;
        config.setPartitionMode(SinkOdpsConfig.PartitionMode.SYSTEM_TIME);
        config.setTimeRange(60);
        SinkOdpsConfig.PartitionConfig partitionConfig = new SinkOdpsConfig.PartitionConfig() {{
            addConfig("pt", "%Y%m%d");
            addConfig("ct", "%H%M");
        }};
        config.setPartitionConfig(partitionConfig);
        client.createConnector(TEST_PROJECT_NAME, tupleTopicName, ConnectorType.SINK_ODPS, columnFields, config);

        long currentTime = System.currentTimeMillis()/1000;
        currentTime = (currentTime - currentTime%(timeRange*60)) * 1000;
        // put records
        List<RecordEntry> recordEntries = new ArrayList<>();
        for (int i = 0; i < RECORD_COUNT; ++i) {
            recordEntries.add(genData(schema, -1, new HashSet<String>()));
        }
        PutRecordsResult putRecordsResult = client.putRecords(TEST_PROJECT_NAME, tupleTopicName, recordEntries);
        Assert.assertEquals(putRecordsResult.getFailedRecordCount(), 0);

        waitForAllShardSinked(tupleTopicName, MAX_SINK_TIMEOUT);

        PartitionSpec partitionSpec = new PartitionSpec();
        SimpleDateFormat pt = new SimpleDateFormat("yyyyMMdd");
        partitionSpec.set("pt", pt.format(new Date(currentTime)));
        SimpleDateFormat ct = new SimpleDateFormat("HHmm");
        partitionSpec.set("ct", ct.format(new Date(currentTime)));
        System.out.println(partitionSpec.toString());
        downloadAndCheckRecordCount(odpsSchema, partitionSpec, RECORD_COUNT);
    }

    @Test
    public void testSinkWithTableColTypeNotMatch() throws Exception {
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
        createTupleTopicBySchema(schema);

        TableSchema odpsSchema = new TableSchema();
        odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
        odpsSchema.addColumn(new Column("f2", OdpsType.BIGINT));
        odpsSchema.addColumn(new Column("f3", OdpsType.DOUBLE));
        odpsSchema.addColumn(new Column("f4", OdpsType.DATETIME));
        odpsSchema.addColumn(new Column("f5", OdpsType.DATETIME));
        odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
        createOdpsTable(odpsSchema);

        try {
            config.setPartitionMode(SinkOdpsConfig.PartitionMode.USER_DEFINE);
            client.createConnector(TEST_PROJECT_NAME, tupleTopicName, ConnectorType.SINK_ODPS, columnFields, config);
            fail("testTableColTypeNotMatch should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }
    }

    @Test
    public void testSinkWithDuplicateField() throws Exception {
        List<String> columnFields = new ArrayList<String>();

        columnFields.add("f1");
        columnFields.add("f1");
        columnFields.add("pt");

        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("f1", FieldType.STRING));
        schema.addField(new Field("f2", FieldType.STRING));
        schema.addField(new Field("pt", FieldType.STRING));
        createTupleTopicBySchema(schema);

        TableSchema odpsSchema = new TableSchema();
        odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
        odpsSchema.addColumn(new Column("f2", OdpsType.STRING));
        odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
        createOdpsTable(odpsSchema);

        try {
            config.setPartitionMode(SinkOdpsConfig.PartitionMode.USER_DEFINE);
            client.createConnector(TEST_PROJECT_NAME, tupleTopicName, ConnectorType.SINK_ODPS, columnFields, config);
            fail("testDuplicateField should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }
    }

    @Test
    public void testSinkWithDiffTopicWithSameConnector() throws Exception {
        List<String> columnFields = new ArrayList<String>();

        columnFields.add("f1");
        columnFields.add("pt");

        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("f1", FieldType.STRING));
        schema.addField(new Field("pt", FieldType.STRING));
        createTupleTopicBySchema(schema);

        TableSchema odpsSchema = new TableSchema();
        odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
        odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
        createOdpsTable(odpsSchema);

        config.setPartitionMode(SinkOdpsConfig.PartitionMode.USER_DEFINE);
        client.createConnector(TEST_PROJECT_NAME, tupleTopicName, ConnectorType.SINK_ODPS, columnFields, config);

        putRecords(schema);

        waitForAllShardSinked(tupleTopicName, MAX_SINK_TIMEOUT);

        // check odps data
        checkSinkedData(odpsSchema, RECORD_COUNT);

        tupleTopicName += "_tmp";
        createTupleTopicBySchema(schema);
        client.createConnector(TEST_PROJECT_NAME, tupleTopicName, ConnectorType.SINK_ODPS, columnFields, config);

        putRecords(schema);
        waitForAllShardSinked(tupleTopicName, MAX_SINK_TIMEOUT);

        // check odps data
        checkSinkedData(odpsSchema, RECORD_COUNT * 2);
    }

    @Test
    public void tesSinkWithShardSplit() throws Exception {
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
        createTupleTopicBySchema(schema);

        TableSchema odpsSchema = new TableSchema();
        odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
        odpsSchema.addColumn(new Column("f2", OdpsType.BIGINT));
        odpsSchema.addColumn(new Column("f3", OdpsType.DOUBLE));
        odpsSchema.addColumn(new Column("f4", OdpsType.DATETIME));
        odpsSchema.addColumn(new Column("f5", OdpsType.BOOLEAN));
        odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
        odpsSchema.addPartitionColumn(new Column("ct", OdpsType.STRING));
        createOdpsTable(odpsSchema);

        config.setPartitionMode(SinkOdpsConfig.PartitionMode.USER_DEFINE);
        client.createConnector(TEST_PROJECT_NAME, tupleTopicName, ConnectorType.SINK_ODPS, columnFields, config);
        putRecordsByShard(schema);
        waitForAllShardSinked(tupleTopicName, MAX_SINK_TIMEOUT);

        client.splitShard(TEST_PROJECT_NAME, tupleTopicName, "0");
        client.waitForShardReady(TEST_PROJECT_NAME, tupleTopicName);

        putRecordsByShard(schema);
        waitForAllShardSinked(tupleTopicName, MAX_SINK_TIMEOUT);

        GetConnectorResult getConnectorResult = client.getConnector(TEST_PROJECT_NAME, tupleTopicName, ConnectorType.SINK_ODPS);
        Assert.assertEquals(getConnectorResult.getColumnFields(), columnFields);
        ListShardResult listShardResult = client.listShard(TEST_PROJECT_NAME, tupleTopicName);
        for (ShardContext sc : getConnectorResult.getShardContexts()) {
            for (ShardEntry se : listShardResult.getShards()) {
                if (sc.getShardId().equals(se.getShardId())) {
                    if (se.getState() == ShardState.CLOSED) {
                        Assert.assertEquals(sc.getCurSequence() + 1, RECORD_COUNT);
                    } else if (se.getParentShardIds().contains("0")) {
                        Assert.assertEquals(sc.getCurSequence() + 1, RECORD_COUNT);
                    } else {
                        Assert.assertEquals(sc.getCurSequence() + 1, 2 * RECORD_COUNT);
                    }
                } else {
                    continue;
                }
            }
        }
    }

    @Test
    public void testSinkWithShardMerge() {
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
        createTupleTopicBySchema(schema);

        TableSchema odpsSchema = new TableSchema();
        odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
        odpsSchema.addColumn(new Column("f2", OdpsType.BIGINT));
        odpsSchema.addColumn(new Column("f3", OdpsType.DOUBLE));
        odpsSchema.addColumn(new Column("f4", OdpsType.DATETIME));
        odpsSchema.addColumn(new Column("f5", OdpsType.BOOLEAN));
        odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
        odpsSchema.addPartitionColumn(new Column("ct", OdpsType.STRING));
        createOdpsTable(odpsSchema);

        config.setPartitionMode(SinkOdpsConfig.PartitionMode.USER_DEFINE);
        client.createConnector(TEST_PROJECT_NAME, tupleTopicName, ConnectorType.SINK_ODPS, columnFields, config);

        putRecordsByShard(schema);
        waitForAllShardSinked(tupleTopicName, MAX_SINK_TIMEOUT);

        client.mergeShard(TEST_PROJECT_NAME, tupleTopicName, "0", "1");
        client.waitForShardReady(TEST_PROJECT_NAME, tupleTopicName);

        putRecordsByShard(schema);
        waitForAllShardSinked(tupleTopicName, MAX_SINK_TIMEOUT);
        sleepInMs(MAX_OPERATOR_INTERVAL);
        GetConnectorResult getConnectorResult = client.getConnector(TEST_PROJECT_NAME, tupleTopicName, ConnectorType.SINK_ODPS);
        Assert.assertEquals(getConnectorResult.getColumnFields(), columnFields);
        ListShardResult listShardResult = client.listShard(TEST_PROJECT_NAME, tupleTopicName);
        for (ShardContext sc : getConnectorResult.getShardContexts()) {
            for (ShardEntry se : listShardResult.getShards()) {
                if (sc.getShardId().equals(se.getShardId())) {
                    if (se.getState() == ShardState.CLOSED) {
                        Assert.assertEquals(sc.getCurSequence() + 1, RECORD_COUNT);
                    } else if (se.getParentShardIds().contains("0")) {
                        Assert.assertEquals(sc.getCurSequence() + 1, RECORD_COUNT);
                    } else {
                        Assert.assertEquals(sc.getCurSequence() + 1, 2 * RECORD_COUNT);
                    }
                } else {
                    continue;
                }
            }
        }

    }

    @Test
    public void testSinkWithMarkdownForSystemTime() throws Exception {
        List<String> columnFields = new ArrayList<String>();
        columnFields.add("f1");

        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("f1", FieldType.STRING));
        createTupleTopicBySchema(schema);

        TableSchema odpsSchema = new TableSchema();
        odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
        odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
        odpsSchema.addPartitionColumn(new Column("ct", OdpsType.STRING));
        createOdpsTable(odpsSchema);

        int timeRange = 60;
        config.setPartitionMode(SinkOdpsConfig.PartitionMode.SYSTEM_TIME);
        config.setTimeRange(timeRange);
        SinkOdpsConfig.PartitionConfig partitionConfig = new SinkOdpsConfig.PartitionConfig() {{
            addConfig("pt", "%Y%m%d");
            addConfig("ct", "%H%M");
        }};
        config.setPartitionConfig(partitionConfig);
        client.createConnector(TEST_PROJECT_NAME, tupleTopicName, ConnectorType.SINK_ODPS, columnFields, config);

        long doneTime = client.getConnectorDoneTime(TEST_PROJECT_NAME, tupleTopicName, ConnectorType.SINK_ODPS).getDoneTime();
        Assert.assertEquals(0, doneTime);

        long currentTime = System.currentTimeMillis()/1000;
        putRecords(schema);
        waitForAllShardSinked(tupleTopicName, MAX_SINK_TIMEOUT);

        doneTime = client.getConnectorDoneTime(TEST_PROJECT_NAME, tupleTopicName, ConnectorType.SINK_ODPS).getDoneTime();
        Assert.assertTrue(doneTime > currentTime - 5 && doneTime < currentTime + 5);

        currentTime = System.currentTimeMillis()/1000;
        putRecords(schema);
        waitForAllShardSinked(tupleTopicName, MAX_SINK_TIMEOUT);

        doneTime = client.getConnectorDoneTime(TEST_PROJECT_NAME, tupleTopicName, ConnectorType.SINK_ODPS).getDoneTime();
        Assert.assertTrue(doneTime > currentTime - 5 && doneTime < currentTime + 5);
    }

    @Test
    public void testSinkWithUpdateConfig() throws Exception {
        List<String> columnFields = new ArrayList<String>();
        columnFields.add("f1");

        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("f1", FieldType.STRING));
        createTupleTopicBySchema(schema);

        TableSchema odpsSchema = new TableSchema();
        odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
        odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
        odpsSchema.addPartitionColumn(new Column("ct", OdpsType.STRING));
        createOdpsTable(odpsSchema);

        int timeRange = 60;
        config.setPartitionMode(SinkOdpsConfig.PartitionMode.SYSTEM_TIME);
        config.setTimeRange(timeRange);
        SinkOdpsConfig.PartitionConfig partitionConfig = new SinkOdpsConfig.PartitionConfig() {{
            addConfig("pt", "%Y%m%d");
            addConfig("ct", "%H%M");
        }};
        config.setPartitionConfig(partitionConfig);
        client.createConnector(TEST_PROJECT_NAME, tupleTopicName, ConnectorType.SINK_ODPS, columnFields, config);

        GetConnectorResult ret = client.getConnector(TEST_PROJECT_NAME, tupleTopicName, ConnectorType.SINK_ODPS);
        SinkOdpsConfig oldConfig = (SinkOdpsConfig)ret.getConfig();
        Assert.assertTrue(!oldConfig.getEndpoint().equals("http://test"));
        Assert.assertEquals(oldConfig.getPartitionConfig().getConfigMap().get("pt"), "%Y%m%d");
        Assert.assertEquals(oldConfig.getPartitionConfig().getConfigMap().get("ct"), "%H%M");

        config.setEndpoint("http://test");
        config.setTunnelEndpoint("http://test_tunnel");
        config.setTimeRange(15);
        config.setAccessId("test");
        config.setAccessKey("ggg");
        SinkOdpsConfig.PartitionConfig partitionConfig2 = new SinkOdpsConfig.PartitionConfig() {{
            addConfig("pt", "%Y%m%d");
            addConfig("ct", "%H");
        }};
        config.setPartitionConfig(partitionConfig2);
        client.updateConnector(TEST_PROJECT_NAME, tupleTopicName, ConnectorType.SINK_ODPS, config);

        ret = client.getConnector(TEST_PROJECT_NAME, tupleTopicName, ConnectorType.SINK_ODPS);
        SinkOdpsConfig newConfig = (SinkOdpsConfig)ret.getConfig();
        Assert.assertEquals(newConfig.getEndpoint(), "http://test");
        Assert.assertEquals(newConfig.getTunnelEndpoint(), "http://test_tunnel");
        Assert.assertEquals(newConfig.getTimeRange(), 15);
        Assert.assertEquals(newConfig.getPartitionConfig().getConfigMap().get("pt"), "%Y%m%d");
        Assert.assertEquals(newConfig.getPartitionConfig().getConfigMap().get("ct"), "%H");
    }

    @Test
    public void testNormalWithUserDefineNullPartition() {
        RecordSchema schema = new RecordSchema() {{
            addField(new Field("f1", FieldType.STRING));
            addField(new Field("pt", FieldType.STRING));
            addField(new Field("ct", FieldType.STRING));
        }};

        createTupleTopicBySchema(schema);
        TableSchema odpsSchema = new TableSchema();
        odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
        odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
        odpsSchema.addPartitionColumn(new Column("ct", OdpsType.STRING));
        createOdpsTable(odpsSchema);

        List<String> columnFields = Arrays.asList("f1", "pt", "ct");
        config.setPartitionMode(SinkOdpsConfig.PartitionMode.USER_DEFINE);
        client.createConnector(TEST_PROJECT_NAME, tupleTopicName, connectorType, columnFields, config);

        Set<String> nullCol1 = new HashSet<>();
        nullCol1.add("pt");
        nullCol1.add("ct");
        putRecords(schema, nullCol1);
        Set<String> nullCol2 = new HashSet<>();
        nullCol2.add("pt");
        putRecords(schema, nullCol2);
        Set<String> nullCol3 = new HashSet<>();
        putRecords(schema, nullCol3);
        // wait for sink
        waitForAllShardSinked(tupleTopicName, MAX_SINK_TIMEOUT);

        // check data
        checkSinkedData(odpsSchema, RECORD_COUNT);

        long totalDiscard = 0;
        GetConnectorShardStatusResult status =
                client.getConnectorShardStatus(TEST_PROJECT_NAME, tupleTopicName, ConnectorType.SINK_ODPS);

        for (Map.Entry<String, ConnectorShardStatusEntry> sc : status.getStatusEntryMap().entrySet()) {
            totalDiscard += sc.getValue().getDiscardCount();
        }
        Assert.assertEquals(totalDiscard, RECORD_COUNT * 2);
    }

    @Test
    public void testNormalWithUserDefineInvalidPartition() {
        RecordSchema schema = new RecordSchema() {{
            addField(new Field("f1", FieldType.STRING));
            addField(new Field("pt", FieldType.STRING));
            addField(new Field("ct", FieldType.STRING));
        }};

        createTupleTopicBySchema(schema);
        TableSchema odpsSchema = new TableSchema();
        odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
        odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
        odpsSchema.addPartitionColumn(new Column("ct", OdpsType.STRING));
        createOdpsTable(odpsSchema);

        List<String> columnFields = Arrays.asList("f1", "pt", "ct");
        config.setPartitionMode(SinkOdpsConfig.PartitionMode.USER_DEFINE);
        client.createConnector(TEST_PROJECT_NAME, tupleTopicName, connectorType, columnFields, config);

        putInvalidRecords(schema);

        Set<String> nullCol = new HashSet<>();
        putRecords(schema, nullCol);

        Set<String> nullCol1 = new HashSet<>();
        nullCol1.add("pt");
        nullCol1.add("ct");
        putRecords(schema, nullCol1);
        // wait for sink
        waitForAllShardSinked(tupleTopicName, MAX_SINK_TIMEOUT);

        // check data
        checkSinkedData(odpsSchema, RECORD_COUNT);
        long totalDiscard = 0;
        GetConnectorShardStatusResult status =
                client.getConnectorShardStatus(TEST_PROJECT_NAME, tupleTopicName, ConnectorType.SINK_ODPS);

        for (Map.Entry<String, ConnectorShardStatusEntry> sc : status.getStatusEntryMap().entrySet()) {
            totalDiscard += sc.getValue().getDiscardCount();
        }
        Assert.assertEquals(totalDiscard, RECORD_COUNT * 2);
    }

    @Test
    public void testNormalWithRowKey() {
        RecordSchema schema = new RecordSchema() {{
            addField(new Field("f1", FieldType.STRING));
            addField(new Field("pt", FieldType.STRING));
            addField(new Field("ct", FieldType.STRING));
        }};

        createTupleTopicBySchema(schema);
        TableSchema odpsSchema = new TableSchema();
        odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
        odpsSchema.addColumn(new Column("__system_rowkey__", OdpsType.STRING));
        odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
        odpsSchema.addPartitionColumn(new Column("ct", OdpsType.STRING));
        createOdpsTable(odpsSchema);

        List<String> columnFields = Arrays.asList("f1", "pt", "ct");
        config.setPartitionMode(SinkOdpsConfig.PartitionMode.USER_DEFINE);
        client.createConnector(TEST_PROJECT_NAME, tupleTopicName, connectorType, columnFields, config);

        putRecords(schema);

        // wait for sink
        waitForAllShardSinked(tupleTopicName, MAX_SINK_TIMEOUT);

        // check data
        checkSinkedData(odpsSchema, RECORD_COUNT);
        long total = 0;
        GetConnectorShardStatusResult status =
                client.getConnectorShardStatus(TEST_PROJECT_NAME, tupleTopicName, ConnectorType.SINK_ODPS);

        for (Map.Entry<String, ConnectorShardStatusEntry> sc : status.getStatusEntryMap().entrySet()) {
            total += sc.getValue().getCurrSequence() + 1;
        }
        Assert.assertEquals(total, RECORD_COUNT);
    }

    @Test
    public void testNormalWithRowKeyInTopic() {
        RecordSchema schema = new RecordSchema() {{
            addField(new Field("f1", FieldType.STRING));
            addField(new Field("pt", FieldType.STRING));
            addField(new Field("ct", FieldType.STRING));
            addField(new Field("__system_rowkey__", FieldType.STRING));
        }};

        createTupleTopicBySchema(schema);
        TableSchema odpsSchema = new TableSchema();
        odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
        odpsSchema.addColumn(new Column("__system_rowkey__", OdpsType.STRING));
        odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
        odpsSchema.addPartitionColumn(new Column("ct", OdpsType.STRING));
        createOdpsTable(odpsSchema);

        List<String> columnFields = Arrays.asList("f1", "pt", "ct");
        config.setPartitionMode(SinkOdpsConfig.PartitionMode.USER_DEFINE);
        client.createConnector(TEST_PROJECT_NAME, tupleTopicName, connectorType, columnFields, config);

        putRecords(schema);

        // wait for sink
        waitForAllShardSinked(tupleTopicName, MAX_SINK_TIMEOUT);

        // check data
        hasRowKey = false;
        checkSinkedData(odpsSchema, RECORD_COUNT);
        long total = 0;
        GetConnectorShardStatusResult status =
                client.getConnectorShardStatus(TEST_PROJECT_NAME, tupleTopicName, ConnectorType.SINK_ODPS);

        for (Map.Entry<String, ConnectorShardStatusEntry> sc : status.getStatusEntryMap().entrySet()) {
            total += sc.getValue().getCurrSequence() + 1;
        }
        Assert.assertEquals(total, RECORD_COUNT);
    }

    @Test
    public void testUnretryableException() throws Exception {
        List<String> columnFields = new ArrayList<String>();
        columnFields.add("f1");

        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("f1", FieldType.STRING));
        createTupleTopicBySchema(schema);

        TableSchema odpsSchema = new TableSchema();
        odpsSchema.addColumn(new Column("f1", OdpsType.STRING));
        odpsSchema.addPartitionColumn(new Column("pt", OdpsType.STRING));
        odpsSchema.addPartitionColumn(new Column("ct", OdpsType.STRING));
        createOdpsTable(odpsSchema);

        config.setPartitionMode(SinkOdpsConfig.PartitionMode.SYSTEM_TIME);
        config.setTimeRange(60);
        SinkOdpsConfig.PartitionConfig partitionConfig = new SinkOdpsConfig.PartitionConfig() {{
            addConfig("pt", "%Y%m%d");
            addConfig("ct", "%H%M");
        }};
        config.setPartitionConfig(partitionConfig);
        client.createConnector(TEST_PROJECT_NAME, tupleTopicName, ConnectorType.SINK_ODPS, columnFields, config);
        sleepInMs(5*1000);
        dropOpdsTable(odpsTable);

        // put records
        List<RecordEntry> recordEntries = new ArrayList<>();
        for (int i = 0; i < RECORD_COUNT; ++i) {
            recordEntries.add(genData(schema, -1, new HashSet<String>()));
        }
        PutRecordsResult putRecordsResult = client.putRecords(TEST_PROJECT_NAME, tupleTopicName, recordEntries);
        Assert.assertEquals(putRecordsResult.getFailedRecordCount(), 0);

        sleepInMs(30*1000);
        GetConnectorShardStatusResult status =
                client.getConnectorShardStatus(TEST_PROJECT_NAME, tupleTopicName, ConnectorType.SINK_ODPS);

        for (Map.Entry<String, ConnectorShardStatusEntry> sc : status.getStatusEntryMap().entrySet()) {
            Assert.assertEquals(sc.getValue().getState(), ConnectorShardState.STOPPED);
            Assert.assertNotEquals(sc.getValue().getLastErrorMessage(), null);
            Assert.assertFalse(sc.getValue().getLastErrorMessage().equals(""));
        }
    }
}
