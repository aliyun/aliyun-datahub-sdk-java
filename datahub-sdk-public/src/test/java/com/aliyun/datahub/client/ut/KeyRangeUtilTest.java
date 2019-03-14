package com.aliyun.datahub.client.ut;

import com.aliyun.datahub.client.util.KeyRangeUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.fail;

public class KeyRangeUtilTest {

    @Test
    public void testNormal() throws Exception {
        String beginKey = "00000000000000000000000000000000";
        String endKey = "22222222222222222222222222222222";
        Assert.assertEquals(KeyRangeUtils.trivialSplit(beginKey, endKey), "11111111111111111111111111111111");
        Assert.assertEquals(KeyRangeUtils.trivialSplit(endKey, beginKey), "11111111111111111111111111111111");
        beginKey = endKey;
        Assert.assertEquals(KeyRangeUtils.trivialSplit(beginKey, endKey), "22222222222222222222222222222222");

    }

    @Test
    public void testWithInvalidKey() {
        String invalidKey = "000000000000000000000000000000001";
        try {
            KeyRangeUtils.trivialSplit(invalidKey, invalidKey);
            fail("keyRange with invalid key should throw exception");
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Invalid Hash Key Range.");
        }
        invalidKey = "gfgaada";
        try {
            KeyRangeUtils.trivialSplit(invalidKey, invalidKey);
            fail("keyRange with invalid key should throw exception");
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Invalid Hash Key Range.");
        }
        invalidKey = "0000000000000000000000000000000";
        try {
            KeyRangeUtils.trivialSplit(invalidKey, invalidKey);
            fail("keyRange with invalid key should throw exception");
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Invalid Hash Key Range.");
        }
    }
}
