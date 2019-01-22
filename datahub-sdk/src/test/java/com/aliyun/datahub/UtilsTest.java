package com.aliyun.datahub;

import com.aliyun.datahub.common.util.KeyRangeUtils;

import com.aliyun.datahub.exception.DatahubClientException;
import org.testng.Assert;
import org.testng.annotations.*;

import java.math.BigInteger;
@Test
public class UtilsTest {
    @BeforeClass
    public void setUp() {}

    @AfterClass
    public void tearDown() {}

    @BeforeMethod
    public void SetUp() {}

    @AfterMethod
    public void TearDown() {}

    @Test
    public void testKeyRangeUtils() {
        String beginKey = "00000000000000000000000000000000";
        String endKey = "22222222222222222222222222222222";
        String maxKey = "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF";
        Assert.assertEquals(KeyRangeUtils.trivialSplit(beginKey, endKey), "11111111111111111111111111111111");
        Assert.assertEquals(KeyRangeUtils.trivialSplit(endKey, beginKey), "11111111111111111111111111111111");
        beginKey = endKey;
        Assert.assertEquals(KeyRangeUtils.trivialSplit(beginKey, endKey), "22222222222222222222222222222222");

        String invalidKey = "000000000000000000000000000000001";
        try {
            KeyRangeUtils.trivialSplit(invalidKey, invalidKey);
            Assert.assertTrue(false);
        } catch (DatahubClientException e) {
            Assert.assertEquals(e.getMessage(), "Invalid Hash Key Range.");
        }
        invalidKey = "gfgaada";
        try {
            KeyRangeUtils.trivialSplit(invalidKey, invalidKey);
            Assert.assertTrue(false);
        } catch (DatahubClientException e) {
            Assert.assertEquals(e.getMessage(), "Invalid Hash Key Range.");
        }
        invalidKey = "0000000000000000000000000000000";
        try {
            KeyRangeUtils.trivialSplit(invalidKey, invalidKey);
            Assert.assertTrue(false);
        } catch (DatahubClientException e) {
            Assert.assertEquals(e.getMessage(), "Invalid Hash Key Range.");
        }

        BigInteger maxValue = KeyRangeUtils.hashKeyToBigInt(maxKey);
        maxValue = maxValue.add(new BigInteger("1", 10));

        try {
            KeyRangeUtils.bigIntToHash(maxValue);
            Assert.assertTrue(false);
        } catch (DatahubClientException e) {
            Assert.assertEquals(e.getMessage(), "Invalid value.");
        }

        Assert.assertEquals(KeyRangeUtils.bigIntToHash(KeyRangeUtils.hashKeyToBigInt(beginKey)), beginKey);

        BigInteger normal = new BigInteger("5", 10);
        Assert.assertEquals(KeyRangeUtils.bigIntToHash(normal), "00000000000000000000000000000005");
    }
}
