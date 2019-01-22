package com.aliyun.datahub;

import com.aliyun.datahub.auth.AliyunAccount;
import com.aliyun.datahub.common.data.RecordType;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.exception.NoPermissionException;
import com.aliyun.datahub.model.BlobRecordEntry;
import com.aliyun.datahub.util.DatahubTestUtils;
import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.http.MethodType;
import com.aliyuncs.http.ProtocolType;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.profile.IClientProfile;
import com.aliyuncs.sts.model.v20150401.AssumeRoleRequest;
import com.aliyuncs.sts.model.v20150401.AssumeRoleResponse;
import org.testng.annotations.*;

import javax.net.ssl.*;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wz on 17/6/1.
 */
@Test
public class StsTest {
    private static String region = "cn-hangzhou";
    private static String STS_API_VERSION = "2015-04-01";
    private static String ROLE_ARN = "acs:ram::1225760110110404:role/teststsrole";
    private DatahubClient client;
    private String projectName;
    private String topicName;
    private String accessKeyId;
    private String accessKeySecret;

    private static String PUT_RECORD_POLICY = "{\n" +
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
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (KeyManagementException e) {
            e.printStackTrace();
        }
    }

    @BeforeClass
    public void setUp() {
        try {
            client = new DatahubClient(DatahubTestUtils.getConf());
            accessKeyId = DatahubTestUtils.getSecondSubAccessId();
            accessKeySecret = DatahubTestUtils.getSecondSubAccesskey();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @BeforeMethod
    public void beforeMethod() {
        projectName = DatahubTestUtils.getRamdomProjectName();
        topicName = DatahubTestUtils.getRamdomTopicName();
    }

    @AfterMethod
    public void afterMethod() {
        client.deleteTopic(projectName, topicName);
        client.deleteProject(projectName);
    }


    @Test
    public void testPutRecords() {
        AssumeRoleResponse.Credentials credentials = getToken(PUT_RECORD_POLICY);

        AliyunAccount account = new AliyunAccount(credentials.getAccessKeyId(),
                credentials.getAccessKeySecret(),
                credentials.getSecurityToken());
        client.setAccount(account);

        client.createProject(projectName, "sts test");
        client.createTopic(projectName, topicName, 1, 1, RecordType.BLOB, "sts test");
        client.getTopic(projectName, topicName);
        List<BlobRecordEntry> entries = new ArrayList<BlobRecordEntry>();
        BlobRecordEntry entry = new BlobRecordEntry();
        entry.setData("ststest".getBytes());
        entry.setShardId("0");
        entries.add(entry);
        client.putBlobRecords(projectName, topicName, entries);
    }

    @Test(expectedExceptions = NoPermissionException.class)
    public void testSplitShard() throws InterruptedException {
        AssumeRoleResponse.Credentials credentials = getToken(PUT_RECORD_POLICY);
        AliyunAccount account = new AliyunAccount(credentials.getAccessKeyId(),
                credentials.getAccessKeySecret(),
                credentials.getSecurityToken());
        client.setAccount(account);

        client.createProject(projectName, "sts test");
        client.createTopic(projectName, topicName, 1, 1, RecordType.BLOB, "");
        // sleep for operation limit
        Thread.sleep(6000);
        client.splitShard(projectName, topicName, "0");
    }

    private AssumeRoleResponse.Credentials getToken(String policy) {
        try {
            DefaultProfile.addEndpoint("Sts", region, "Sts", "sts-openapi-daily.alibaba.net");
            IClientProfile profile = DefaultProfile.getProfile(region, accessKeyId, accessKeySecret);
            DefaultAcsClient acsClient = new DefaultAcsClient(profile);
            // 创建一个 AssumeRoleRequest 并设置请求参数
            final AssumeRoleRequest request = new AssumeRoleRequest();

            request.setVersion(STS_API_VERSION);
            request.setMethod(MethodType.POST);
            request.setProtocol(ProtocolType.HTTPS);
            request.setRoleArn(ROLE_ARN);
            request.setRoleSessionName("datahub");
            request.setPolicy(policy);
            // 发起请求，并得到response
            final AssumeRoleResponse response;
            response = acsClient.getAcsResponse(request);
            return response.getCredentials();
        } catch (ClientException e) {
            e.printStackTrace();
        }
        return null;
    }

}