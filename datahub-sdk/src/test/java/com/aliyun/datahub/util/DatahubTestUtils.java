package com.aliyun.datahub.util;

import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.DatahubConfiguration;
import com.aliyun.datahub.auth.Account;
import com.aliyun.datahub.auth.AliyunAccount;
import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.common.data.FieldType;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.exception.AbortedException;
import com.aliyun.datahub.model.ElasticSearchDesc;
import com.aliyun.datahub.model.RecordEntry;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
@Test
public class DatahubTestUtils {

    private static final Properties props = new Properties();
    private static AtomicLong counter = new AtomicLong();

    public final static Long MIN_TIMESTAMP_VALUE = -62135798400000000L;
    public final static Long MAX_TIMESTAMP_VALUE = 253402271999000000L;
    public final static int MAX_STRING_LENGTH = 1024 * 1024;

    static {
        try {
            loadConfig();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 从CLASSPATH加载test.properties
     *
     * @return
     * @throws IOException
     */
    public static Properties loadConfig() throws IOException {

        InputStream is = null;
        try {
            is = DatahubTestUtils.class.getClassLoader().getResourceAsStream(
                    "datahub_test.conf");
            props.load(is);
        } finally {
            if (is != null) {
                is.close();
            }
        }
        return props;
    }

    public static DatahubConfiguration getConf() {
        String accessId = props.getProperty("default.access.id");
        String accessKey = props.getProperty("default.access.key");
        String securityToken = props.getProperty("default.sts.token");
        Account account = new AliyunAccount(accessId, accessKey, securityToken);
        String endpoint = props.getProperty("default.endpoint");
        if (endpoint == null || endpoint.isEmpty()) {
            throw new AbortedException("default.endpoint not set!");
        }
        return new DatahubConfiguration(account, endpoint);
    }

    public static String getProjectName() {
        return props.getProperty("default.project");
    }

    public static String getRandomTopicName() {
        return "ut_topic_" + System.currentTimeMillis() + "_" + counter.addAndGet(1);
    }

    public static DatahubClient getDefaultClient() {
        DatahubConfiguration conf = getConf();
        return new DatahubClient(conf);
    }

    /**
     * create schema by spec
     *
     * @param spec format: type name, type name ...
     * @return
     */
    public static RecordSchema createSchema(String spec) {
        RecordSchema schema = new RecordSchema();
        String[] segs = spec.trim().split(",");
        for (String seg : segs) {
            String[] pair = seg.trim().split(" +");
            if (pair.length != 2) {
                throw new IllegalArgumentException("invalid spec");
            }
            String type = pair[0];
            String name = pair[1];
            schema.addField(new Field(name, FieldType.valueOf(type.toUpperCase())));
        }
        return schema;
    }

    public static String getRandomString(int size) {
        return RandomStringUtils.randomAscii(size);
    }

    public static String getRandomString() {
        return getRandomString(new Random().nextInt(32));
    }

    public static Long getRandomNumber() {
        return new Random().nextLong();
    }

    public static Long getRandomTimestamp() {
        return new RandomDataGenerator().nextLong(MIN_TIMESTAMP_VALUE, MAX_TIMESTAMP_VALUE);
    }

    public static Boolean getRandomBoolean() {
        return new Random().nextBoolean();
    }

    public static Double getRandomDecimal() {
        return new Random().nextDouble() + new Random().nextLong();
    }

    public static RecordEntry makeRecord(RecordSchema schema, boolean makeNullField) {
        RecordEntry entry = new RecordEntry(schema);
        for (int i = 0; i < schema.getFields().size(); ++i) {
            if (makeNullField && getRandomBoolean()) {
                continue;
            }
            if (schema.getField(i).getType() == FieldType.BIGINT) {
                entry.setBigint(i, getRandomNumber());
            } else if (schema.getField(i).getType() == FieldType.BOOLEAN) {
                entry.setBoolean(i, getRandomBoolean());
            } else if (schema.getField(i).getType() == FieldType.STRING) {
                entry.setString(i, getRandomString());
            } else if (schema.getField(i).getType() == FieldType.DOUBLE) {
                entry.setDouble(i, getRandomDecimal());
            } else if (schema.getField(i).getType() == FieldType.TIMESTAMP){
                entry.setTimeStamp(i, getRandomTimestamp());
            }
        }
        return entry;
    }

    public static RecordEntry makeRecord(RecordSchema schema) {
        return makeRecord(schema, false);
    }


    public static DatahubConfiguration getSecondConf() {
        String accessId = props.getProperty("second.access.id");
        String accessKey = props.getProperty("second.access.key");
        String securityToken = props.getProperty("second.sts.token");
        Account account = new AliyunAccount(accessId, accessKey, securityToken);
        String endpoint = props.getProperty("default.endpoint");
        if (endpoint == null || endpoint.isEmpty()) {
            throw new AbortedException("default.endpoint not set!");
        }
        return new DatahubConfiguration(account, endpoint);
    }

    public static DatahubConfiguration getSecondSubConf() {
        String accessId = props.getProperty("second.sub.access.id");
        String accessKey = props.getProperty("second.sub.access.key");
        String securityToken = props.getProperty("second.sub.sts.token");
        Account account = new AliyunAccount(accessId, accessKey, securityToken);
        String endpoint = props.getProperty("default.endpoint");
        if (endpoint == null || endpoint.isEmpty()) {
            throw new AbortedException("default.endpoint not set!");
        }
        return new DatahubConfiguration(account, endpoint);
    }

    public static String getSecondSubAccessId() {
        return props.getProperty("second.sub.access.id");
    }

    public static String getSecondSubAccesskey() {
        return props.getProperty("second.sub.access.key");
    }

    public static String getSecondAccessId() {
        return props.getProperty("second.access.id");
    }

    public static String getSecondAccesskey() {
        return props.getProperty("second.access.key");
    }

    public static String getEndpoint() {
        return props.getProperty("default.endpoint");
    }
    public static String getAccessId() {
        return props.getProperty("default.access.id");
    }

    public static String getAccessKey() {
        return props.getProperty("default.access.key");
    }

    public static String getRamdomProjectName() {
        return "ut_project_" + System.currentTimeMillis() + "_" + counter.addAndGet(1);
    }

    public static String getRamdomTopicName() {
        return "ut_topic_" + System.currentTimeMillis() + "_" + counter.addAndGet(1);
    }

    public static String getOdpsProjectName() {
        return props.getProperty("default.odps.project");
    }

    public static String getOdpsEndpoint() {
        return props.getProperty("default.odps.endpoint");
    }

    public static String getOdpsTunnelEndpoint() {
        return props.getProperty("default.odps.tunnel.endpoint");
    }

    public static String getSecondUser() {
        return props.getProperty("second.user");
    }

    public static String getDefaultUser() {
        return props.getProperty("default.user");
    }

    public static String getESEndpoint() {
        return props.getProperty("default.es.endpoint");
    }

    public static int getESPort() {
        return Integer.valueOf(props.getProperty("default.es.port"));
    }

    public static String getESUser() {
        return props.getProperty("default.es.user");
    }

    public static String getESPassword() {
        return props.getProperty("default.es.password");
    }

    public static String getOssEndpoint() {
        return props.getProperty("default.oss.endpoint");
    }

    public static String getOssBucket() {
        return props.getProperty("default.oss.bucket");
    }

    public static String getOssAccessId() {
        return props.getProperty("default.oss.access.id");
    }

    public static String getOssAccessKey() {
        return props.getProperty("default.oss.access.key");
    }

    public static String getMysqlHost() {
        return props.getProperty("default.mysql.host");
    }

    public static String getMysqlPort() {
        return props.getProperty("default.mysql.port");
    }

    public static String getMysqlDatabase() {
        return props.getProperty("default.mysql.database");
    }

    public static String getMysqlUser() {
        return props.getProperty("default.mysql.user");
    }

    public static String getMysqlPassword() {
        return props.getProperty("default.mysql.password");
    }

    public static String getADSHost() {
        return props.getProperty("default.ads.host");
    }

    public static String getADSPort() {
        return props.getProperty("default.ads.port");
    }

    public static String getADSUser() {
        return props.getProperty("default.ads.user");
    }

    public static String getADSPassword() {
        return props.getProperty("default.ads.password");
    }

    public static String getADSDatabase() {
        return props.getProperty("default.ads.database");
    }

    public static String getADSTableGroup() {
        return props.getProperty("default.ads.tablegroup");
    }

    public static String getOtsEndpoint() { return props.getProperty("default.ots.endpoint"); }

    public static String getOtsInstance() { return props.getProperty("default.ots.instance"); }

    public static String getOtsTable() { return props.getProperty("default.ots.table"); }

    public static String getOtsAccessId() { return props.getProperty("default.ots.access.id"); }

    public static String getOtsAccessKey() { return props.getProperty("default.ots.access.key"); }
 }
