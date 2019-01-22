package com.aliyun.datahub;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.common.data.FieldType;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.common.data.RecordType;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.exception.DatahubServiceException;
import com.aliyun.datahub.exception.ResourceNotFoundException;
import com.aliyun.datahub.model.GetMeteringInfoRequest;
import com.aliyun.datahub.model.GetMeteringInfoResult;
import com.aliyun.datahub.model.ListShardResult;
import com.aliyun.datahub.model.ShardEntry;
import com.aliyun.datahub.util.DatahubTestUtils;

@Test
public class MeteringInfoTest {
	private String projectName = null;
	private String topicName = null;
	private DatahubClient client = null;
	private DatahubClient clientInternal = null;
	private int shardCount = 3;
	private int lifeCycle = 7;

	@BeforeClass
	public void setUp() {
		try {
			client = new DatahubClient(DatahubTestUtils.getConf());
			clientInternal = new DatahubClient(DatahubTestUtils.getConf());
			projectName = DatahubTestUtils.getProjectName();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@AfterClass
	public void tearDown() {
		try {
			// do nothing
		} catch (DatahubClientException e) {
			e.printStackTrace();
		}
	}

	@BeforeMethod
	public void SetUp() {
		try {
			RecordType type = RecordType.TUPLE;
			RecordSchema schema = new RecordSchema();
			schema.addField(new Field("test", FieldType.BIGINT));
			String comment = "test";
			topicName = DatahubTestUtils.getRamdomTopicName();
			System.out.println("Topic Name is " + topicName);
			client.createTopic(projectName, topicName, shardCount, lifeCycle, type, schema, comment);
			client.waitForShardReady(projectName, topicName);
		} catch (DatahubClientException e) {
			e.printStackTrace();
		}
	}

	@AfterMethod
	public void TearDown() {
		try {
			client.deleteTopic(projectName, topicName);
		} catch (DatahubClientException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testGetMeteringInfo() {
		ListShardResult listShardResult = client.listShard(projectName, topicName);
		Assert.assertEquals(listShardResult.getShards().size(), 3);
		boolean bExit = false;
		boolean bSuc = true;
		while (!bExit) {
			try {
				for (ShardEntry s : listShardResult.getShards()) {
					GetMeteringInfoResult result = clientInternal
							.getMeteringInfo(new GetMeteringInfoRequest(projectName, topicName, s.getShardId()));
					Assert.assertTrue(result.getActiveTime() > 0);
				}
				bSuc = true;
				bExit = true;
			} catch (ResourceNotFoundException e) {
				bExit = false;
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
					bExit = true;
					bSuc = false;
				}
			} catch (DatahubServiceException e2) {
				e2.printStackTrace();
				bExit = true;
				bSuc = false;
			}
		}
		Assert.assertTrue(bSuc);
	}
}
