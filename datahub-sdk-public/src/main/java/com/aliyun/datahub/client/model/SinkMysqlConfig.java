package com.aliyun.datahub.client.model;

public class SinkMysqlConfig extends SinkConfig {
    public enum InsertMode {
        /**
         * Insert ignore if the database has the same key record
         */
        IGNORE,

        /**
         * Insert overwrite if the database has the same key record
         */
        OVERWRITE,
    }

    /**
     * The host of the database.
     */
    private String host;

    /**
     * The port of the database.
     */
    private int port;

    /**
     * Database name used to synchronize data.
     */
    private String database;

    /**
     * Table name used to synchronize data.
     */
    private String table;

    /**
     * User name used to synchronize data.
     */
    private String user;

    /**
     * User password used to synchronize data.
     */
    private String password;

    /**
     * Ignore write or not. Two categories: 'insert overwrite' and 'insert ignore'
     */

    private InsertMode insertMode = InsertMode.IGNORE;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public InsertMode getInsertMode() {
        return insertMode;
    }

    public void setInsertMode(InsertMode insertMode) {
        this.insertMode = insertMode;
    }
}
