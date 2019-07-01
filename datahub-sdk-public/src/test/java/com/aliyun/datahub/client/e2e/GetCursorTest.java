package com.aliyun.datahub.client.e2e;

import com.aliyun.datahub.client.exception.InvalidParameterException;
import com.aliyun.datahub.client.exception.ResourceNotFoundException;
import com.aliyun.datahub.client.exception.SeekOutOfRangeException;
import com.aliyun.datahub.client.model.BlobRecordData;
import com.aliyun.datahub.client.model.CursorType;
import com.aliyun.datahub.client.model.GetCursorResult;
import com.aliyun.datahub.client.model.RecordEntry;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.fail;

public class GetCursorTest extends BaseTest {

    @BeforeClass
    public static void setUpBeforeClass() {
        bCreateTopic = true;
        BaseTest.setUpBeforeClass();
    }

    @Test
    public void testGetCursorNormarl() {
        GetCursorResult getCursorResult = client.getCursor(TEST_PROJECT_NAME, blobTopicName, "0", CursorType.OLDEST);
        Assert.assertEquals(0, getCursorResult.getSequence());
        Assert.assertEquals("30000000000000000000000000000000", getCursorResult.getCursor());

        // put 10 records
        long timestamp = System.currentTimeMillis();
        List<RecordEntry> recordEntries = new ArrayList<>(10);
        for (int i = 0; i < 10; ++i) {
            RecordEntry entry = new RecordEntry() {{
                addAttribute("partition", "ds=2016");
                setRecordData(new BlobRecordData("test-data".getBytes()));
            }};
            recordEntries.add(entry);
        }
        client.putRecordsByShard(TEST_PROJECT_NAME, blobTopicName, "0", recordEntries);

        getCursorResult = client.getCursor(TEST_PROJECT_NAME, blobTopicName, "0", CursorType.OLDEST);
        Assert.assertEquals(0, getCursorResult.getSequence());

        getCursorResult = client.getCursor(TEST_PROJECT_NAME, blobTopicName, "0", CursorType.SEQUENCE, 9);
        Assert.assertEquals(9, getCursorResult.getSequence());

        getCursorResult = client.getCursor(TEST_PROJECT_NAME, blobTopicName, "0", CursorType.SYSTEM_TIME, timestamp - 1000);
        Assert.assertEquals(0, getCursorResult.getSequence());

        try {
            getCursorResult = client.getCursor(TEST_PROJECT_NAME, blobTopicName, "0", CursorType.SYSTEM_TIME, timestamp + 10000);
            fail("GetCursor with future time should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof SeekOutOfRangeException);
        }
    }

    @Test
    public void testGetCursorWithInvalidSequence() {
        try {
            client.getCursor(TEST_PROJECT_NAME, blobTopicName, "0", CursorType.SEQUENCE, 1);
            fail("GetCusor with invalid sequence should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof SeekOutOfRangeException);
        }
    }

    @Test
    public void testGetCursorWithInvalidParam() {
        try {
            client.getCursor(TEST_PROJECT_NAME, blobTopicName, "0", CursorType.SEQUENCE, -1);
            fail("GetCusor with invalid sequence should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidParameterException);
        }
    }

    @Test
    public void testGetCursorWithNotExitShard() {
        try {
            client.getCursor(TEST_PROJECT_NAME, blobTopicName, "4", CursorType.OLDEST);
            fail("GetCursor with not exist shard should throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ResourceNotFoundException);
        }
    }
}
