package com.aliyun.datahub.client.e2e;

import com.aliyun.datahub.client.e2e.common.Configure;
import com.aliyun.datahub.client.e2e.common.Constant;
import com.aliyun.datahub.client.model.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class SinkAdsTest extends SinkMysqlTest {
    private static final int MAX_WAIT_ADS_META_TIME = 60000;
    private static final int PARTITION_NUM = 10;
    private static final boolean IS_REAL_TIME = true;
    private String adsGroup;

    @Override
    void initSinkConfig() {
        adsGroup = Configure.getString(Constant.ADS_TABLE_GROUP);
        connectorType = ConnectorType.SINK_ADS;
        config = new SinkAdsConfig() {{
            setHost(Configure.getString(Constant.ADS_HOST));
            setPort(Configure.getInteger(Constant.ADS_PORT));
            setUser(Configure.getString(Constant.ADS_USER));
            setPassword(Configure.getString(Constant.ADS_PASSWORD));
            setDatabase(Configure.getString(Constant.ADS_DATABASE));
            setTable(tupleTopicName);
        }};
        enableDecimal = Configure.getBoolean(Constant.ADS_ENABLE_DECIMAL, false);
    }

    @Override
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

        sb.append(", PRIMARY KEY(").append(PK_COL).append("))");

        if (PARTITION_NUM > 0) {
            sb.append("PARTITION BY HASH KEY(pk) PARTITION NUM ").append(PARTITION_NUM);
        }
        sb.append(" TABLEGROUP ").append(adsGroup);
        if (IS_REAL_TIME) {
            sb.append(" options (updateType='realtime')");
        }

        doDbExecutor(sb.toString());
    }

    @Override
    void deleteTables() {
        String sql = String.format("SELECT TABLE_NAME AS name FROM information_schema.TABLES WHERE table_schema = '%s' AND table_group = '%s'",
                config.getDatabase(), adsGroup);
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

    @Override
    protected void waitForAllShardSinked(String topicName, long timeout) {
        super.waitForAllShardSinked(topicName, timeout);
        sleepInMs(MAX_WAIT_ADS_META_TIME);
    }


    @Override
    long convertTimestamp(ResultSet rs, String dbCol) throws SQLException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        try {
            Date dt = sdf.parse(rs.getString(dbCol));
            return dt.getTime() / 1000;
        } catch (ParseException e) {
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public void testSinkNormal() {
        super.testSinkNormal();
    }

    @Override
    public void testSinkWithIgnoreFalse() {
        super.testSinkWithIgnoreFalse();
    }

    @Override
    public void testUpdateConnector() {
        super.testUpdateConnector();
    }

    @Override
    public void testSinkWithNoPk() {
        // ads not test this
    }

    @Override
    public void testSinkWithFieldValueNull() {
        super.testSinkWithFieldValueNull();
    }

    @Override
    public void testSinkWithPkNull() {
        super.testSinkWithPkNull();
    }

    @Override
    public void testSinkWithNotAllFieldSinked() {
        super.testSinkWithNotAllFieldSinked();
    }

    @Override
    public void testSinkWithColumnOrderNotSame() {
        super.testSinkWithColumnOrderNotSame();
    }

    @Override
    public void testSinkWithColumnFieldSizeNotMatch() {
        super.testSinkWithColumnFieldSizeNotMatch();
    }

    @Override
    public void testSinkWithFieldTypeNotMatch() {
        super.testSinkWithFieldTypeNotMatch();
    }

    @Override
    public void testSinkWithFieldNameNotMatch() {
        super.testSinkWithFieldNameNotMatch();
    }

    @Override
    public void testSinkWithDbMoreField() {
        super.testSinkWithDbMoreField();
    }

    @Override
    public void testSinkWithAppendDatahubField() {
        super.testSinkWithAppendDatahubField();
    }
}
