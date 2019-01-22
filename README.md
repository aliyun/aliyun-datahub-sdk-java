# DataHub SDK for Java developers

    DataHub is a publish-subscribe(pub-sub) system developed for stream data processing.
    DataHub enables you to build applications based on stream data analysis.
    DataHub can be also used as the data entrance of StreamCompute and MaxCompute.

### Requirements

* Java 6+

### Clone and build

    git clone ...
    cd datahub-sdk-java
    mvn clean install -DskipTests

### Maven

    <dependency>
        <groupId>com.aliyun.datahub</groupId>
        <artifactId>aliyun-sdk-datahub</artifactId>
        <version>2.9.4-public</version>
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

##### 1. Create Project

    DataHub is now on public testing, please contact us for creating projects.

##### 2. Initialize

    Account account = new AliyunAccount("your access id", "your access key");
    DatahubConfiguration conf = new DatahubConfiguration(account, "datahub endpoint");
    DatahubClient client = new DatahubClient(conf);

##### 3. CreateTopic

    int shardCount = 3;
    int lifecycle = 7;
    RecordType type = RecordType.TUPLE;
    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("field1", FieldType.BIGINT));
    schema.addField(new Field("field2", FieldType.String));
    String comment = "The first topic";
    client.createTopic(projectName, topicName, shardCount, lifecycle, type, schema, comment);

##### 4. PutRecords
    // construct the records to be upload
    RecordSchema schema = client.getTopic("projectName", "topicName").getRecordSchema();
    List<RecordEntry> recordEntries = new ArrayList<~>();
    RecordEntry entry = new RecordEntry(schema);
    entry.setBigint(0, 123456);
    entry.setString(1, "string");
    entry.setShardId("shardId");

    entry.putAttribute("attr_id", "2902381718"); //Attribute is the optional value for the record
    recordEntries.add(entry);

    // put records
    client.putRecords("projectName", "topicName", recordEntries);

##### 5. GetRecords
    // get records
    GetCursorResult cursor = client.getCursor("projectName", "topicName", "shardId", 1455869335000 /*ms*/);
    GetRecordsResult r = client.getRecords("projectName", "topicName", "shardId", cursor.getCursor(), 10);

### License

licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html)
