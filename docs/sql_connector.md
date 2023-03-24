<style>
    img {
        display: block;
        box-shadow: 0 0 10px #555;
        border-radius: 6px;
        margin-left: auto;
        margin-right: auto;
        margin-top: 10px;
        margin-bottom: 10px;
        -webkit-box-shadow: 0 0 10px #555;
    }
</style>
## Flink 自定义 SQL Connector
### 1. 启程
flink 本身提供了丰富的 sql connector, 一般不需要用户自己定义。但是在某些特殊的情况下需要手动实现针对实际场景的 sql connector。
最近在实践中遇到了两个比较极端的场景, 无法通过简单的 sql connector 实现: 
- 业务1: 逻辑上将数据分写入到一个 flink sql table, 物理上分发到多个表。
- 业务2：数据采集系统中，数据上报到某一张表，但是每次上报的字段不同。

flink sql 是建立在 streaming api 之上的。实现 sql connector 本质上是在钩子中编写一些 streaming 逻辑。
有哪些钩子呢？可以先看下图

<img src="http://nightlies.apache.org/flink/flink-docs-release-1.17/fig/table_connectors.svg" width="80%"></img>

  
1. DynamicTableSinkFactory
2. DynamicTableSink
3. RichSinkFunction

sql connector 定义在 `WITH ( connector = 'xxx')` 中，这需要 Java SPI 机制在进行注册发现。
在 resources 目录下建立 `META-INF/service/org.apache.flink.table.factories.Factory`
实现 DynamicTableSinkFactory 后将 class 路径填写到上面的 Factory 文件。

### 2. 实现
这里以一个'json-print'` sql connector 为例来说明 sql connector 的编写方式。
`json-print` : 逻辑 sink 表中的数据以 json 结构打印输出。重点突出钩子逻辑，简化输出逻辑。生产中的输出可能是各种存储介质，这里简单使用标准输出代替。

#### 2.1 DynamicTableFactory
1. 首先定义一个 `WithOption` 类来定义 with 中的参数
```java
public class WithOptions {
    public static final String IDENTIFIER = "json-print";
    public static final ConfigOption<Integer> BATCH_SIZE = ConfigOptions.key("batch-size").intType().noDefaultValue();
}
```

2. 定义 `JsonPrintDynamicTableFactory` 实现 `DynamicTableSinkFactory`
  
- 定义可选和必选参数
- 定义 identifier (connector = 'xxx' 名字)
- 传递 options, schema 给到 `JsonPrintDynamicTableSink` 类

> options 获取传入 sql connector 的参数
> schema 可以获取表结构信息，如字段名、字段类型、主键等
```java
public class JsonPrintDynamicTableFactory implements DynamicTableSinkFactory {
    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ReadableConfig options = helper.getOptions();
        ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
        // options and schema
        return new JsonPrintDynamicTableSink(options, schema);
    }

    // connector name
    @Override
    public String factoryIdentifier() {
        return WithOptions.IDENTIFIER;
    }

    // connector required args
    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        HashSet<ConfigOption<?>> options = new HashSet<>();
        options.add(WithOptions.BATCH_SIZE);
        return options;
    }

    // connector optional args
    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<ConfigOption<?>>();
    }
}
```

#### 2.2 DynamicTableSink
- 设置支持的 change log mode
- 提供 RuntimeProvider (这里创建 SinkFunction)
```java
public class JsonPrintDynamicTableSink implements DynamicTableSink, Serializable {
    ReadableConfig options;
    ResolvedSchema schema;

    public JsonPrintDynamicTableSink(ReadableConfig options, ResolvedSchema schema) {
        this.options = options;
        this.schema = schema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        // support change log mode
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        // args
        var batchSize = options.get(WithOptions.BATCH_SIZE);

        JsonPrintConfig cfg = new JsonPrintConfig(batchSize);
        // schema is very important
        // we can get columns info and primary keys info from schema
        List<Column> columns = schema.getColumns();
        ArrayList<Tuple2<String, String>> cols = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            var col = columns.get(i);
            cols.add(new Tuple2<>(col.getName(), col.getDataType().getConversionClass().getSimpleName()));
        }
        return SinkFunctionProvider.of(new JsonPrintSinkFunction(cfg, cols));
    }

    @Override
    public DynamicTableSink copy() {
        return new JsonPrintDynamicTableSink(options, schema);
    }

    @Override
    public String asSummaryString() {
        return "JsonPrint Table Sink";
    }
}
```

### 2.3 RichSinkFunction
简单实现一个 SinkFunction。应注意的是传出的泛型类应该是 flink 的 `RowData`

```java
@Slf4j
public class JsonPrintSinkFunction extends RichSinkFunction<RowData> implements CheckpointedFunction {
    private final JsonPrintConfig config;
    private final ArrayList<Tuple2<String, String>> columns;
    private final List<RowData> buffer  = new LinkedList<>();

    public JsonPrintSinkFunction(JsonPrintConfig config, ArrayList<Tuple2<String, String>> columns) {
        this.config = config;
        this.columns = columns;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 在这里建立数据库连接...
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void invoke(RowData row, Context context) throws Exception {
        super.invoke(row, context);
        if (row == null) {
            return;
        }
        buffer.add(row);
        if (buffer.size() >= config.getBatchSize()) {
            doSink();
        }
    }

    private void doSink() {
        if (buffer.size() <= 0) {
            return;
        }
        for (var row : buffer) {
            HashMap<String, Object> map = new HashMap<>();
            for (int i = 0; i < columns.size(); i++) {
                var col = columns.get(i);
                var feName = col.f0;
                var feType = col.f1;
                switch (feType) {
                    case "String":
                        map.put(feName, row.getString(i).toString());
                        break;
                    case "Integer":
                        map.put(feName, row.getInt(i));
                        break;
                }
            }
            System.out.printf("%s => %s\n", row.getRowKind().toString() ,JSON.toJSONString(map));
        }
        buffer.clear();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        doSink();
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

    }
}
```

### 3. 实现源码
[json-print sql connector 实现](https://github.com/owenqing/flink_sea/tree/main/src/main/java/org/owenqing/sea/connector/jsonprint)
