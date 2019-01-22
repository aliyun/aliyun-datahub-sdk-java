package com.aliyun.datahub.wrapper;

import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.util.DatahubTestUtils;
import com.aliyun.datahub.wrapper.Project;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Created by wz on 16/6/6.
 */
@Test
public class ProjectWrapperTest {
    // TODO please fill some cases

    @Test
    void TestBuildWrapper() {
        try {
            DatahubClient client = DatahubTestUtils.getDefaultClient();
            Project project = Project.Builder.build(DatahubTestUtils.getProjectName(), client);
            Assert.assertEquals(DatahubTestUtils.getProjectName(), project.getProjectName());

            project = Project.Builder.build(DatahubTestUtils.getProjectName(), DatahubTestUtils.getConf());
            Assert.assertEquals(DatahubTestUtils.getProjectName(), project.getProjectName());
        } catch (Exception e) {
            Assert.assertTrue(false);
        }
    }

    @Test
    void testCreateTopic() {
        try {
            DatahubClient client = DatahubTestUtils.getDefaultClient();
            Project project = Project.Builder.build(DatahubTestUtils.getProjectName(), client);
            RecordSchema schema = DatahubTestUtils.createSchema("  bigint   a , string b, boolean c, timestamp d, double e, string f");
        } catch (Exception e) {
            Assert.assertTrue(false);
        }

    }
}