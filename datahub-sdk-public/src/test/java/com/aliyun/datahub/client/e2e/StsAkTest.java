package com.aliyun.datahub.client.e2e;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.e2e.common.Configure;
import com.aliyun.datahub.client.e2e.common.Constant;
import com.aliyun.datahub.client.exception.NoPermissionException;
import com.aliyun.datahub.client.model.BlobRecordData;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.RecordType;
import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.http.MethodType;
import com.aliyuncs.http.ProtocolType;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.profile.IClientProfile;
import com.aliyuncs.sts.model.v20150401.AssumeRoleRequest;
import com.aliyuncs.sts.model.v20150401.AssumeRoleResponse;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.net.ssl.*;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.fail;

public class StsAkTest extends BaseTest {
    private static final String REGION = "cn-hangzhou";
    private static final String STS_API_VERSION = "2015-04-01";
    private static final String ROLE_ARN = "acs:ram::1225760110110404:role/teststsrole";
    private static final String DATAHUB_ENDPOINT = Configure.getString(Constant.DATAHUB_ENDPOINT);
    private static final String SUB_USER_AK = Configure.getString(Constant.DATAHUB_SUBUSER_ACCESS_ID);
    private static final String SUB_USER_SK = Configure.getString(Constant.DATAHUB_SUBUSER_ACCESS_KEY);
    private static final String PUT_RECORD_POLICY = "{\n" +
            "    \"Version\": \"1\", \n" +
            "    \"Statement\": [\n" +
            "        {\n" +
            "            \"Action\": [\n" +
            "                \"dhs:PutRecords\",\n" +
            "                \"dhs:*Topic\",\n" +
            "                \"dhs:*Project\"\n" +
            "            ], \n" +
            "            \"Resource\": [\n" +
            "                \"acs:dhs:*:*:*\"\n" +
            "            ], \n" +
            "            \"Effect\": \"Allow\"\n" +
            "        }\n" +
            "    ]\n" +
            "}";

    private static String blobTopicName = getTestTopicName(RecordType.BLOB);

    private static DatahubClient subClient;

    @BeforeClass
    public static void setUp() {
        BaseTest.setUpBeforeClass();
        // create project and topic with parent account
        // And delete with sts sub account.

        AssumeRoleResponse.Credentials credentials = StsAkTest.getToken(PUT_RECORD_POLICY);
        if (credentials == null) {
            throw new RuntimeException("Get sts token fail");
        }

        subClient = DatahubClientBuilder.newBuilder().setDatahubConfig(
                new DatahubConfig(
                        DATAHUB_ENDPOINT,
                        new AliyunAccount(credentials.getAccessKeyId(), credentials.getAccessKeySecret(), credentials.getSecurityToken()),
                        true)).build();

        try {
            client.createProject(TEST_PROJECT_NAME, "comment");
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            client.createTopic(TEST_PROJECT_NAME, blobTopicName, 3, 1, RecordType.BLOB, "test blob topic");
        } catch (Exception e) {
            //
        }
    }

    @AfterClass
    public static void tearDown() {
        try {
            subClient.deleteTopic(TEST_PROJECT_NAME, blobTopicName);
        } catch (Exception e) {
            //
        }
    }

    static {
        disableSslVerification();
    }

    private static void disableSslVerification() {
        try {
            // Create a trust manager that does not validate certificate chains
            TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager() {
                @Override
                public void checkClientTrusted(java.security.cert.X509Certificate[] x509Certificates, String s) throws CertificateException {
                }

                @Override
                public void checkServerTrusted(java.security.cert.X509Certificate[] x509Certificates, String s) throws CertificateException {
                }

                public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                    return null;
                }
            }
            };

            // Install the all-trusting trust manager
            SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, trustAllCerts, new java.security.SecureRandom());
            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());

            // Create all-trusting host name verifier
            HostnameVerifier allHostsValid = new HostnameVerifier() {
                public boolean verify(String hostname, SSLSession session) {
                    return true;
                }
            };

            // Install the all-trusting host verifier
            HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testPutRecords() throws InterruptedException {
        Thread.sleep(5000); // wait for shard ready
        RecordEntry entry = new RecordEntry() {{
            addAttribute("partition", "ds=2016");
            setRecordData(new BlobRecordData("test-data".getBytes()));
        }};

        List<RecordEntry> recordEntries = new ArrayList<>();
        recordEntries.add(entry);

        subClient.putRecordsByShard(TEST_PROJECT_NAME, blobTopicName, "0", recordEntries);
    }

    @Test
    public void testSplitShard() throws InterruptedException {
        Thread.sleep(5000);
        try {
            subClient.splitShard(TEST_PROJECT_NAME, blobTopicName, "0");
            fail("split shard should throw NoPermissionException");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof NoPermissionException);
        }
    }

    private static AssumeRoleResponse.Credentials getToken(String policy) {
        try {
            DefaultProfile.addEndpoint("Sts", REGION, "Sts", "sts-openapi-daily.alibaba.net");
            IClientProfile profile = DefaultProfile.getProfile(REGION, SUB_USER_AK, SUB_USER_SK);
            DefaultAcsClient acsClient = new DefaultAcsClient(profile);
            final AssumeRoleRequest request = new AssumeRoleRequest();
            request.setVersion(STS_API_VERSION);
            request.setMethod(MethodType.POST);
            request.setProtocol(ProtocolType.HTTPS);
            request.setRoleArn(ROLE_ARN);
            request.setRoleSessionName("datahub");
            request.setPolicy(policy);
            final AssumeRoleResponse response;
            response = acsClient.getAcsResponse(request);
            return response.getCredentials();
        } catch (ClientException e) {
            e.printStackTrace();
        }
        return null;
    }

}