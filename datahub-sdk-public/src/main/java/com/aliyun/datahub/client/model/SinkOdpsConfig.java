package com.aliyun.datahub.client.model;

import java.util.LinkedHashMap;
import java.util.Map;

public class SinkOdpsConfig extends SinkConfig {
    /**
     * How to divide data into different MaxCompute partition.
     */
    public enum PartitionMode {
        /**
         * Partitioned by user defined columns
         */
        USER_DEFINE,
        /**
         * First should have a column named 'event_time TIMESTAMP', and then partitioned by this column value.
         */
        SYSTEM_TIME,
        /**
         * Partitioned by time that a record written into DataHub
         */
        EVENT_TIME
    }

    /**
     * MaxCompute project name.
     */
    private String project;

    /**
     * MaxCompute table name
     */
    private String table;

    /**
     * The endpoint of the MaxCompute service
     */
    private String endpoint;

    /**
     * The endpoint of the Tunnel service which is bound with MaxCompute<br>
     * If not specified, DataHub will get tunnel endpoint from MaxCompute project config.
     */
    private String tunnelEndpoint;

    /**
     * The accessId to visit MaxCompute. The accessId should have permission to visit the corresponding MaxCompute table.
     */
    private String accessId;

    /**
     * The accessKey to visit MaxCompute.
     */
    private String accessKey;

    /**
     * Specify how to partition data info corresponding MaxCompute partition. {@link PartitionMode}
     */
    private PartitionMode partitionMode;

    /**
     * How long time data that a MaxCompute partition to store. Used in SYSTEM_TIME and EVENT_TIME mode.
     * <br>
     * Unit: Minute
     */
    private int timeRange;

    /**
     * Specify how to compute MaxCompute partition with the specified column value.
     */
    private PartitionConfig partitionConfig;

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getTunnelEndpoint() {
        return tunnelEndpoint;
    }

    public void setTunnelEndpoint(String tunnelEndpoint) {
        this.tunnelEndpoint = tunnelEndpoint;
    }

    public String getAccessId() {
        return accessId;
    }

    public void setAccessId(String accessId) {
        this.accessId = accessId;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public PartitionMode getPartitionMode() {
        return partitionMode;
    }

    public void setPartitionMode(PartitionMode partitionMode) {
        this.partitionMode = partitionMode;
    }

    public int getTimeRange() {
        return timeRange;
    }

    public void setTimeRange(int timeRange) {
        this.timeRange = timeRange;
    }

    public PartitionConfig getPartitionConfig() {
        return partitionConfig;
    }

    public void setPartitionConfig(PartitionConfig partitionConfig) {
        this.partitionConfig = partitionConfig;
    }

    public static class PartitionConfig {
        private Map<String, String> configMap = new LinkedHashMap<>();

        public void addConfig(String key, String value) {
            configMap.put(key, value);
        }

        public Map<String, String> getConfigMap() {
            return configMap;
        }
    }
}