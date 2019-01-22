package com.aliyun.datahub.example;

import java.util.List;

import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.DatahubConfiguration;
import com.aliyun.datahub.auth.AliyunAccount;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.exception.OffsetResetedException;
import com.aliyun.datahub.exception.OffsetSessionChangedException;
import com.aliyun.datahub.exception.SubscriptionOfflineException;
import com.aliyun.datahub.model.GetCursorResult;
import com.aliyun.datahub.model.GetRecordsResult;
import com.aliyun.datahub.model.GetTopicResult;
import com.aliyun.datahub.model.OffsetContext;
import com.aliyun.datahub.model.RecordEntry;
import com.aliyun.datahub.model.GetCursorRequest.CursorType;

public class SingleSubscriptionExample {
	private String accessId = "**you access id**";
	private String accessKey = "**you access key**";
	private String endpoint = "**datahub server endpoint**";
	private String projectName = "**you project name**";
	private String topicName = "**you topic name**";
	private String subId = "**you subscription id**";
	private String shardId = "**shard id**";
	private DatahubConfiguration conf;
	private DatahubClient client;

	public SingleSubscriptionExample() {
		this.conf = new DatahubConfiguration(new AliyunAccount(accessId, accessKey), endpoint);
		this.client = new DatahubClient(conf);
	}

	public void Start() {
		try {
			boolean bExit = false;
			GetTopicResult topicResult = client.getTopic(projectName, topicName);
			// 首先初始化offset上下文
			OffsetContext offsetCtx = client.initOffsetContext(projectName, topicName, subId, shardId);
			String cursor = null; // 开始消费的cursor
			if (!offsetCtx.hasOffset()) {
				// 之前没有存储过点位，先获取初始点位，比如这里获取当前该shard最早的数据
				GetCursorResult cursorResult = client.getCursor(projectName, topicName, shardId, CursorType.OLDEST);
				cursor = cursorResult.getCursor();
			} else {
				// 否则，获取当前已消费点位的下一个cursor
				cursor = client.getNextOffsetCursor(offsetCtx).getCursor();
			}

			System.out.println("Start consume records, begin offset context:" + offsetCtx.toObjectNode().toString()
					+ ", cursor:" + cursor);

			long recordNum = 0L;
			while (!bExit) {
				try {
					GetRecordsResult recordResult = client.getRecords(projectName, topicName, shardId, cursor, 10,
							topicResult.getRecordSchema());
					List<RecordEntry> records = recordResult.getRecords();
					if (records.size() == 0) {
						// 将最后一次消费点位上报
						client.commitOffset(offsetCtx);
						System.out.println("commit offset suc! offset context: " + offsetCtx.toObjectNode().toString());
						// 可以先休眠一会，再继续消费新记录
						Thread.sleep(1000);
						System.out.println("sleep 1s and continue consume records! shard id:" + shardId);
					} else {
						for (RecordEntry record : records) {
							// 处理记录逻辑
							// System.out.println("Consume shard:" + shardId + " thread process record:"
							//		+ record.toJsonNode().toString());

							// 上报点位，该示例是每处理100条记录上报一次点位
							offsetCtx.setOffset(record.getOffset());
							recordNum++;
							if (recordNum % 100 == 0) {
								client.commitOffset(offsetCtx);
								System.out.println("commit offset suc! offset context: " + offsetCtx.toObjectNode().toString());
							}
						}
						cursor = recordResult.getNextCursor();
					}
				} catch (SubscriptionOfflineException e) {
					// 订阅下线，退出
					bExit = true;
					e.printStackTrace();
				} catch (OffsetResetedException e) {
					// 点位被重置，更新offset上下文
					client.updateOffsetContext(offsetCtx);
					cursor = client.getNextOffsetCursor(offsetCtx).getCursor();
					System.out.println("Restart consume shard:" + shardId + ", reset offset:"
							+ offsetCtx.toObjectNode().toString() + ", cursor:" + cursor);
				} catch (OffsetSessionChangedException e) {
					// 其他consumer同时消费了该订阅下的相同shard，退出
					bExit = true;
					e.printStackTrace();
				} catch (Exception e) {
					bExit = true;
					e.printStackTrace();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		SingleSubscriptionExample example = new SingleSubscriptionExample();
		try {
			example.Start();
		} catch (DatahubClientException e) {
			e.printStackTrace();
		}
	}
}
