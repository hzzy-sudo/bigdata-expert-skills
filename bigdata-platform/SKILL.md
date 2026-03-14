---
name: bigdata-platform
description: 大数据平台研发专家技能集。用于大数据组件源码分析、二次开发、内核原理研究、性能优化。包括Flink、Spark、Hudi、Iceberg、ClickHouse、StarRocks等组件的深度定制开发。适用于需要阅读源码理解原理、二次开发定制功能、组件性能优化、问题深度定位的场景。
---

# 大数据平台研发专家

## 1. Flink源码分析

### 1.1 核心源码结构

```
flink-streaming-java/
├── src/main/java/org/apache/flink/streaming/
│   ├── api/
│   │   ├── context/
│   │   │   └── StreamExecutionEnvironment.java    # 运行环境
│   │   ├── graph/
│   │   │   ├── StreamGraph.java                  # 流图
│   │   │   ├── StreamNode.java                   # 节点
│   │   │   └── StreamEdge.java                   # 边
│   │   └── operators/
│   │       ├── KeyedProcessOperator.java         # Keyed处理算子
│   │       └── WindowOperator.java               # 窗口算子
│   └── runtime/
│       ├── jobmanager/
│       │   └── JobMaster.java                    # Job管理器
│       ├── taskmanager/
│       │   └── TaskExecutor.java                 # Task执行器
│       └── checkpoint/
│           ├── CheckpointCoordinator.java         # Checkpoint协调器
│           └── CheckpointStorage.java            # Checkpoint存储
```

### 1.2 JobGraph生成流程

```java
// 核心流程分析
// 1. StreamGraphGenerator.generate()
//    - 遍历所有Transformation
//    - 创建StreamNode和StreamEdge
//    - 构成StreamGraph

// 2. JobGraphGenerator.createJobGraph()
//    - 将StreamGraph转换为JobGraph
//    - 进行算子链化(Chainable)
//    - 设置并行度和资源

// 关键方法追踪：
// StreamGraphGenerator.generate() 
//   -> transform()

// 调试技巧：添加断点
// StreamGraphGenerator.java:142
// 查看transformation的输入输出
```

### 1.3 Checkpoint机制

```java
// Checkpoint触发流程
// 1. JobManager发送CheckpointTriggerMessage
// 2. Source算子收到后，保存状态并发送Barrier
// 3. Barrier向下游流动
// 4. 每个算子收到所有输入的Barrier后，进行Checkpoint
// 5. 所有算子完成，JobManager确认Checkpoint完成

// 关键类：
// - CheckpointCoordinator: 协调整个checkpoint过程
// - StateBackend: 状态后端管理
// - SnapshotStrategy: 快照策略

// 配置参数：
env.enableCheckpointing(60000);  // 60秒
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
env.getCheckpointConfig().setCheckpointTimeout(600000);
env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
```

### 1.4 状态管理

```java
// 状态类型
// 1. KeyedState
ValueState<T> keyedState = getRuntimeContext().getState(
    new ValueStateDescriptor<>("name", String.class));

ListState<T> listState = getRuntimeContext().getListState(
    new ListStateDescriptor<>("list", String.class));

MapState<K, V> mapState = getRuntimeContext().getMapState(
    new MapStateDescriptor<>("map", String.class, String.class));

// 2. OperatorState
ListState<T> operatorState = getRuntimeContext().getListState(
    new ListStateDescriptor<>("opList", String.class));

// 3. BroadcastState
MapState<B, V> broadcastState = getRuntimeContext().getBroadcastState(
    new MapStateDescriptor<>("broadcast", String.class, String.class));

// 状态后端选择
// HashMapStateBackend: 状态小，内存充足
// EmbeddedRocksDBStateBackend: 状态大，内存不足
env.setStateBackend(new HashMapStateBackend());
env.setStateBackend(new EmbeddedRocksDBStateBackend());
```

## 2. Spark源码分析

### 2.1 核心源码结构

```
spark/
├── core/
│   └── src/main/scala/org/apache/spark/
│       ├── SparkContext.scala              # 入口
│       ├── rdd/
│       │   ├── RDD.scala                  # RDD抽象
│       │   └── ShuffledRDD.scala           # Shuffle RDD
│       ├── scheduler/
│       │   ├── DAGScheduler.scala          # DAG调度
│       │   └── TaskScheduler.scala           # Task调度
│       └── storage/
│           └── BlockManager.scala           # 存储管理
└── sql/
    └── core/
        └── src/main/scala/org/apache/spark/sql/
            ├── SparkSession.scala           # SQL入口
            ├── execution/
            │   ├── SparkPlan.scala          # 执行计划
            │   └── exchange/
            │       └── ShuffleExchangeExec.scala  # Shuffle
```

### 2.2 DAG生成与Stage划分

```scala
// DAGScheduler.submitJob()
// 1. 创建Stage
// 2. 提交TaskSet到TaskScheduler

// Stage划分规则：
// 1. 窄依赖不切分(OneToOneDependency, PruneDependency)
// 2. 宽依赖切分(ShuffleDependency)

// 源码追踪：
// DAGScheduler.scala:handleJobSubmitted()
//   -> createResultStage()
//   -> createShuffleMapStage()

// 关键方法：
// getShuffleDependency() // 检测Shuffle依赖
// isStageMaterialized()   // 检查Stage是否完成
```

### 2.3 Shuffle原理

```scala
// ShuffleWrite (Map端)
// ShuffleWriter.write()
//   - SortShuffleWriter: 排序后写入
//   - UnsafeShuffleWriter: 低内存写入
//   - BypassMergeSortShuffleWriter: 不排序

// ShuffleRead (Reduce端)
// ShuffleBlockFetcherIterator.fetchNext()
//   - 拉取多个map端数据
//   - 合并排序

// 参数调优：
spark.shuffle.file.buffer = 1mb        // Map端缓冲
spark.reducer.maxSizeInFlight = 256mb  // Reduce端缓冲
spark.shuffle.io.numConnectionsPerPeer = 1
```

### 2.4 内存管理

```
Spark内存分布:
+------------------+------------------+
|    Execution     |     Storage      |
|    (执行内存)      |     (存储内存)    |
|  Shuffle/排序    |   Cache/广播      |
+------------------+------------------+
|         User Memory              |
|   UDF/数据结构/元数据            |
+------------------+------------------+
|        Reserved (300MB)          |
+----------------------------------+

// 动态分配
spark.memory.fraction = 0.6
spark.memory.storageFraction = 0.5

// Old Gen GC
- XX:OldGenRatio = 0.75
- XX:+UseG1GC
```

## 3. 二次开发

### 3.1 自定义Source

```java
// Flink Source
public class MySource implements SourceFunction<String> {
    private volatile boolean isRunning = true;
    
    @Override
    public void run(SourceContext<String> ctx) {
        while (isRunning) {
            String data = fetchData();
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(data);
            }
            Thread.sleep(1000);
        }
    }
    
    @Override
    public void cancel() {
        isRunning = false;
    }
    
    private String fetchData() {
        // 从外部系统获取数据
        return "data";
    }
}

// 使用
DataStream<String> stream = env.addSource(new MySource());
```

### 3.2 自定义Sink

```java
// Flink Sink
public class MySink implements SinkFunction<String> {
    @Override
    public void invoke(String value, Context context) {
        // 写入外部系统
        connection.write(value);
    }
}

// RichSinkFunction (带生命周期)
public class MyRichSink extends RichSinkFunction<String> {
    private Connection connection;
    
    @Override
    public void open(Configuration parameters) {
        connection = createConnection();
    }
    
    @Override
    public void invoke(String value, Context context) {
        connection.write(value);
    }
    
    @Override
    public void close() {
        if (connection != null) {
            connection.close();
        }
    }
}
```

### 3.3 自定义UDF

```java
// Spark UDF
// Java
spark.udf().register("myUDF", 
    (String s1, String s2) -> s1 + "_" + s2, 
    DataTypes.StringType);

// Scala
val myUDF = udf((s1: String, s2: String) => s1 + "_" + s2)
spark.udf.register("myUDF", myUDF)

// 使用
spark.sql("SELECT myUDF(col1, col2) FROM table")
```

### 3.4 自定义Connector

```java
// Flink Kafka Connector配置
KafkaSource<String> source = KafkaSource.<String>builder()
    .setBootstrapServers("localhost:9092")
    .setGroupId("my-group")
    .setTopics("topic1", "topic2")
    .setStartingOffsets(OffsetsInitializer.committedOffsets(
        OffsetResetStrategy.EARLIEST))
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .setProperties(properties)
    .build();

// Kafka Sink
KafkaSink<String> sink = KafkaSink.<String>builder()
    .setBootstrapServers("localhost:9092")
    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
        .setTopic("output-topic")
        .setValueSerializationSchema(new SimpleStringSchema())
        .build())
    .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
    .build();
```

## 4. Hudi二次开发

### 4.1 表格式选择

```java
// Copy-on-Write (COW)
// 优点：读快，写慢
// 适用：读多写少场景

// Merge-on-Read (MOR)
// 优点：写快，读慢(需要 compaction)
// 适用：写多读少场景

// 创建COW表
 HoodieTableType.COPY_ON_WRITE

// 创建MOR表
 HoodieTableType.MERGE_ON_READ
```

### 4.2 自定义Payload

```java
// 自定义合并策略
public class MyPayload extends HoodieRecordPayload<HoodieData> {
    @Override
    public Option<HoodieData> combineAndGetUpdateValue(
            HoodieData existing,
            HoodieData incoming,
            Schema schema) {
        // 自定义合并逻辑
        if (incoming.getTimestamp() > existing.getTimestamp()) {
            return Option.of(incoming);
        }
        return Option.of(existing);
    }
}
```

### 4.3 索引优化

```java
// 布隆索引(默认)
 HoodieIndex index = BloomIndex.createIndex(config);

// 哈希索引(分区较少)
 HoodieIndex index = HashIndex.createIndex(config);

// HBase索引(海量数据)
 HoodieIndex index = HBaseIndex.createIndex(config);
```

## 5. 性能优化

### 5.1 网络优化

```java
// Flink网络配置
taskmanager.network.memory.fraction = 0.15
taskmanager.network.memory.min = 256mb
taskmanager.network.memory.max = 1gb
taskmanager.network.memory.buffer-per-channel = 2mb
taskmanager.network.memory.exclusive-buffers-per-slot = 4

// Spark网络优化
spark.network.timeout = 300s
spark.executor.heartbeatInterval = 30s
```

### 5.2 内存优化

```java
// Flink内存
taskmanager.memory.process.size = 8g
taskmanager.memory.flink.managed.size = 4g
taskmanager.memory.task.heap.size = 2g
taskmanager.memory.managed.size = 2g

// Spark内存
spark.executor.memory = 8g
spark.driver.memory = 4g
spark.memory.fraction = 0.6
spark.memory.storageFraction = 0.5
```

### 5.3 Checkpoint优化

```java
// 增量Checkpoint
state.backend.incremental = true

// 调整间隔
env.enableCheckpointing(300000);  // 5分钟
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(60000);

// 异步Checkpoint
env.getCheckpointConfig().enableUnalignedCheckpoints();
```

## 6. 边界处理

### 6.1 状态膨胀

```方案>
1. 定期清理状态
2. 设置TTL
3. 使用 RocksDB 自动压缩
</方案>

### 6.2 数据倾斜

```java
// 加盐打散
val salt = (Math.random() * n).toInt
df.withColumn("salt", lit(salt))
  .withColumn("key", concat($"key", $"salt"))
  .groupBy("key")
  .agg(...)
```

### 6.3 背压处理

```bash
# 查看背压
flink list -r RUNNING
curl http://jobmanager:8081/jobs/<jobid>

# 增加并行度
env.setParallelism(8)

# 调整缓冲区
taskmanager.network.memory.buffer-per-channel = 4mb
```
