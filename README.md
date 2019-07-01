# DataHub SDK for Java developers

    DataHub is a publish-subscribe(pub-sub) system developed for stream data processing.
    DataHub enables you to build applications based on stream data analysis.
    DataHub can be also used as the data entrance of StreamCompute and MaxCompute.

### Requirements

* Java 8+

### Clone and build

    git clone ...
    cd datahub-sdk-java
    mvn clean install -DskipTests

### Maven

    <dependency>
        <groupId>com.aliyun.datahub</groupId>
        <artifactId>aliyun-sdk-datahub</artifactId>
        <version>2.13.0-public</version>
    </dependency>

### Endpoint

    http://dh-cn-hangzhou.aliyuncs.com
    http://dh-cn-hangzhou-internal.aliyuncs.com

### Concepts

| name | detail |
| --- | --- |
| Project | Project is the basic organizational unit of DataHub, which contains a number of Topic. |
| Topic | Topic is the smallest unit of pub-sub, it can be used of processing the same type of data according to users' own business |
| Shard | Shard is the concurrent transfer channel of Topic, each shard has a corresponding ID |
| Lifecycle | It represents the number of days that data in topic to be recycled |
| Record | The basic unit of data pub-sub, each record has a unique id in shard |
| Cursor | The specified position used for record subcribe, can be obtained by timestamp |


### DataHub Usage

| variable | detail |
| ---- | ---- |
| DATAHUB_ENDPOINT | DataHub service endpoint |
| PROJECT_NAME | Project name |
| TOPIC_NAME | Topic name |
| SHARD_ID | Shard id |
| ACESS_ID | Your access id |
| ACCESS_KEY | Your access key |

##### 1. Create Project

    DataHub is now on public testing, please contact us for creating projects.

##### 2. Initialize

    DatahubClient client = DatahubClientBuilder.newBuilder()
        .setDatahubConfig(
            new DatahubConfig(DATAHUB_ENDPOINT, new AliyunAccount(ACESS_ID, ACCESS_KEY))
    ).build();

##### 3. CreateTopic

    int shardCount = 3;
    int lifecycle = 7;
    RecordType type = RecordType.TUPLE;
    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("field1", FieldType.BIGINT));
    schema.addField(new Field("field2", FieldType.String));
    String comment = "The first topic";
    client.createTopic(PROJECT_NAME, TOPIC_NAME, shardCount, lifecycle, type, schema, comment);

##### 4. PutRecords
    // construct record schema
    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("field1", FieldType.STRING));
    schema.addField(new Field("field2", FieldType.BIGINT));

    /**
     * or get record schema from server
     * RecordSchema schema = client.getTopic("projectName", "topicName").getRecordSchema();
     */

    List<RecordEntry> recordEntries = new ArrayList<>();
    for (int cnt = 0; cnt < 10; ++cnt) {
        RecordEntry entry = new RecordEntry();
        entry.addAttribute("key1", "value1");
        entry.addAttribute("key2", "value2");
        
        TupleRecordData data = new TupleRecordData(schema);
        data.setField("field1", "testValue");
        data.setField("field2", 1);
        
        entry.setRecordData(data);
        recordEntries.add(entry);
    }

    // put tuple records by shard
    client.putRecordsByShard(PROJECT_NAME, TOPIC_NAME, SHARD_ID, recordEntries);

##### 5. GetRecords
    // get records
    GetCursorResult getCursorResult = client.getCursor(PROJECT_NAME, TOPIC_NAME, SHARD_ID, CursorType.SEQUENCE, 0);
    GetRecordsResult getRecordsResult = client.getRecords(PROJECT_NAME, TOPIC_NAME, SHARD_ID, schema, getCursorResult.getCursor(), 100);
    for (RecordEntry entry : getRecordsResult.getRecords()) {
        TupleRecordData data = (TupleRecordData) entry.getRecordData();
        System.out.println("field1:" + data.getField(0) + ", field2:" + data.getField("field2"));
    }

### License

licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html)
