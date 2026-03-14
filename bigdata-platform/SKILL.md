---
name: bigdata-platform
description: 大数据平台研发专家技能集。用于大数据组件源码分析，二次开发，内核原理研究，性能优化。包括Flink、Spark，Hudi、Iceberg、ClickHouse、StarRocks等组件的深度定制开发，源码阅读，问题定位，性能调优。此skill包含完整的发现问题-定位问题-解决问题-复盘反思闭环。
---

# 大数据平台研发专家

## 核心能力图谱

```
发现问题 ──→ 定位问题 ──→ 解决问题 ──→ 复盘反思
    │              │              │            │
    ▼              ▼              ▼            ▼
  监控告警      源码阅读       内核修改      经验沉淀
  性能分析      执行计划       参数调优      知识库
  问题反馈      日志分析       代码优化      文档记录
```

---

## 1. Flink源码分析

### 1.1 发现问题

**问题信号**：
- Checkpoint失败：超时/状态不完整
- 状态丢失：重启后数据丢失
- 背压持续：处理延迟增加
- 内存泄漏：内存持续增长

### 1.2 定位问题

```java
// ==================== 1. JobGraph生成流程 ====================
// 入口：StreamGraphGenerator.generate()

// 关键方法追踪：
// 1) transform() - 转换算子
// 2) createStateDescriptor() - 创建状态描述
// 3) processElement() - 处理元素

// 调试技巧：
// 添加断点
// StreamGraphGenerator.java:142
// StreamGraph.java:89
// StreamNode.java:112

// 查看生成的StreamGraph
StreamGraph streamGraph = env.getStreamGraph();
streamGraph.getJobGraph();


// ==================== 2. Checkpoint机制 ====================
// 入口：CheckpointCoordinator.triggerCheckpoint()

// 关键方法：
// 1) triggerCheckpoint() - 触发checkpoint
// 2) snapshotState() - 快照状态
// 3) restoreState() - 恢复状态

// 问题定位：
// CheckpointCoordinator.java:345 - checkpoint触发
// StateBackend.java:123 - 状态存储
// RocksDBStateBackend.java:89 - RocksDB操作


// ==================== 3. 任务调度 ====================
// 入口：ExecutionGraph.build()

// 关键方法：
// 1) schedule() - 调度任务
// 2) deployToSlot() - 部署到slot
// 3) cancel() - 取消任务


// ==================== 4. 内存管理 ====================
// 入口：TaskManagerServices.create()

// 关键方法：
// 1) createMemoryManager() - 内存管理
// 2) createNetworkBuffers() - 网络缓冲
// 3) createStateBackends() - 状态后端


// ==================== 5. 背压机制 ====================
// 入口：InputChannel.requestBuffer()

// 关键方法：
// 1) isAvailable() - 检查可用
// 2) notifyBufferAvailable() - 通知可用
// 3) backlog() - 积压长度
```

### 1.3 解决问题

```java
// ==================== 1. 自定义Source ====================
// 实现并行Source
public class MyParallelSource implements ParallelSourceFunction<String> {
    private volatile boolean isRunning = true;
    private int index;
    
    public MyParallelSource(int index) {
        this.index = index;
    }
    
    @Override
    public void run(SourceContext<String> ctx) {
        while (isRunning) {
            String data = fetchData(index);
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
    
    private String fetchData(int index) {
        return "data_" + index + "_" + System.currentTimeMillis();
    }
}

// 使用
DataStream<String> stream = env.addSource(new MyParallelSource(0)).setParallelism(4);


// ==================== 2. 自定义Sink ====================
// 实现Exactly-Once Sink
public class MyExactlyOnceSink implements SinkFunction<String>, CheckpointedFunction {
    private transient ValueState<String> state;
    
    @Override
    public void initializeState(FunctionInitializationContext context) {
        state = context.getKeyedStateStore().getState(
            new ValueStateDescriptor<>("sinkState", String.class));
    }
    
    @Override
    public void snapshotState(FunctionSnapshotContext context) {
        // 状态快照
    }
    
    @Override
    public void invoke(String value, Context context) {
        // 写入外部系统
        writeToExternal(value);
        state.update(value);
    }
}


// ==================== 3. 自定义函数 ====================
// Rich函数（带生命周期）
public class MyRichMapFunction extends RichMapFunction<String, String> {
    
    @Override
    public void open(Configuration parameters) {
        // 初始化
        // 建立连接
        // 加载配置
    }
    
    @Override
    public String map(String value) {
        // 转换逻辑
        return value.toUpperCase();
    }
    
    @Override
    public void close() {
        // 清理资源
        // 关闭连接
    }
}


// ==================== 4. 状态编程 ====================
// Keyed State
ValueState<String> valueState = getRuntimeContext().getState(
    new ValueStateDescriptor<>("valueState", String.class));

// List State
ListState<String> listState = getRuntimeContext().getListState(
    new ListStateDescriptor<>("listState", String.class));

// Map State
MapState<String, Integer> mapState = getRuntimeContext().getMapState(
    new MapStateDescriptor<>("mapState", String.class, Integer.class));

// Reducing State
ReducingState<Count> reducingState = getRuntimeContext().getReducingState(
    new ReducingStateDescriptor<>("count", new CountReducer(), Count.class));


// ==================== 5. 窗口编程 ====================
// 滚动窗口
data.keyBy("key")
    .window(TumblingEventTimeWindows.of(Time.minutes(5)))
    .sum("amount");

// 滑动窗口
data.keyBy("key")
    .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(5)))
    .sum("amount");

// 会话窗口
data.keyBy("key")
    .window(EventTimeSessionWindows.withGap(Time.minutes(10)))
    .sum("amount");
```

### 1.4 复盘反思

```sql
CREATE TABLE flink_source_analysis (
    analysis_id BIGINT PRIMARY KEY,
    component STRING,          -- FLINK/SPARK/HUDI
    issue_type STRING,       -- CHECKPOINT/BACKPRESSURE/MEMORY/STATE
    symptom STRING,           -- 症状
    root_cause STRING,       -- 根因
    analysis_path STRING,    -- 源码路径
    analysis_method STRING,  -- 分析方法
    solution STRING,          -- 解决方案
    prevention STRING,       -- 预防措施
    duration_minutes INT,
    author STRING,
    create_time TIMESTAMP
);

-- 记录
INSERT INTO flink_source_analysis 
VALUES 
('FLINK', 'CHECKPOINT', 'Checkpoint超时10分钟', '状态过大(50G)', 'CheckpointCoordinator.java:345', '增加超时+增量Checkpoint', '监控状态大小', 60, '平台组', NOW());
```

---

## 2. Spark源码分析

### 2.1 发现问题

**问题信号**：
- DAG划分不合理：Stage数量过多
- Shuffle数据量大：网络传输慢
- 内存溢出：OOM错误
- 任务倾斜：部分Task执行时间长

### 2.2 定位问题

```scala
// ==================== 1. DAG生成流程 ====================
// 入口：DAGScheduler.submitJob()

// 关键方法：
// 1) submitStage() - 提交Stage
// 2) getShuffleDependencies() - 获取Shuffle依赖
// 3) createShuffleMapStage() - 创建ShuffleMapStage

// 调试
spark.sparkContext.setJobGroup("job1", "description")


// ==================== 2. Stage划分 ====================
// 入口：DAGScheduler.createStage()

// 窄依赖：不切分
// OneToOneDependency
// PruneDependency

// 宽依赖：切分
// ShuffleDependency

// 调试
val unresolvedRelations = df.queryExecution.analyzed
println(unresolvedRelations.treeString)


// ==================== 3. Task执行 ====================
// 入口：TaskSetManager.resourceOffer()

// 关键方法：
// 1) offerSlots() - 分配Slot
// 2) launchTask() - 启动Task
// 3) run() - 执行Task


// ==================== 4. Shuffle原理 ====================
// ShuffleWrite (Map端)
// SortShuffleWriter - 排序写入
// UnsafeShuffleWriter - 低内存写入

// ShuffleRead (Reduce端)
// BlockStoreShuffleReader - 读取数据
// ShuffleBlockFetcherIterator - 获取Block
```

### 2.3 解决问题

```scala
// ==================== 1. 自定义Partitioner ====================
class MyPartitioner(numPartitions: Int) extends Partitioner {
    override def numPartitions: Int = numPartitions
    
    override def getPartition(key: Any): Int = {
        key match {
            case null => 0
            case _ => key.hashCode() % numPartitions
        }
    }
}

val partitioned = data.partitionBy(new MyPartitioner(100))


// ==================== 2. 自定义UDF ====================
// 注册UDF
spark.udf.register("myUDF", (s: String) => {
    s.toUpperCase
}, StringType)

// 使用
spark.sql("SELECT myUDF(name) FROM table")


// ==================== 3. 内存优化 ====================
// 配置
spark.executor.memory = 8g
spark.driver.memory = 4g
spark.memory.fraction = 0.6
spark.memory.storageFraction = 0.5

// 代码优化
// 1. 避免shuffle
df.repartition(100).join(df2.repartition(100), "key")

// 2. 广播小表
val smallDf = df.filter("key IN (1,2,3)")
df.join(broadcast(smallDf), "key")

// 3. 减少中间数据
df.select("col1", "col2").filter("col1 > 0")


// ==================== 4. 数据倾斜 ====================
// 加盐
val salt = (Math.random() * 10).toInt
val salted = df.withColumn("salt", lit(salt))
    .withColumn("key", concat($"key", $"salt"))

val result = salted.groupBy("key")
    .agg(sum("value") as "value")
    .groupBy("key")
    .agg(sum("value") as "value")
```

### 2.4 复盘反思

```sql
CREATE TABLE spark_source_analysis (
    analysis_id BIGINT PRIMARY KEY,
    issue_type STRING,       -- DAG/STAGE/SHUFFLE/MEMORY
    symptom STRING,           -- 症状
    root_cause STRING,       -- 根因
    source_path STRING,      -- 源码路径
    analysis_method STRING,  -- 分析方法
    solution STRING,          -- 解决方案
    effect STRING,          -- 效果
    author STRING,
    create_time TIMESTAMP
);

INSERT INTO spark_source_analysis 
VALUES 
('DAG', 'Stage过多(30+)', 'DAG划分不合理', 'DAGScheduler.scala:234', '查看执行计划', '减少shuffle', 'Stage减至10个', '平台组', NOW());
```

---

## 3. Hudi二次开发

### 3.1 发现问题

**问题信号**：
- 写入性能差：吞吐量低
- 压缩频繁：IO高
- 查询慢：延迟高
- 表类型选择错误

### 3.2 定位问题

```java
// ==================== 1. 表类型分析 ====================
// 判断表类型
HoodieTableType tableType = table.getTableType();
if (tableType == HoodieTableType.COPY_ON_WRITE) {
    // COW表
} else if (tableType == HoodieTableType.MERGE_ON_READ) {
    // MOR表
}

// ==================== 2. 写入分析 ====================
// HoodieWriteClient
// 1) initTable() - 初始化表
// 2) startCommit() - 开始提交
// 3) insert() - 插入数据
// 4) upsert() - 更新数据
// 5) compact() - 压缩

// ==================== 3. 索引分析 ====================
// HoodieIndex
// BloomIndex - 布隆索引(默认)
// HBaseIndex - HBase索引
// SimpleIndex - 简单索引

// ==================== 4. 压缩策略 ====================
// CompactionStrategy
// DayBasedCompactionStrategy - 按日期
// NumOrCompactionStrategy - 按文件数
```

### 3.3 解决问题

```java
// ==================== 1. 选择表类型 ====================
// COW: 读多写少
// 优点：读快，写慢
// 适用：CDC、分析场景

// MOR: 写多读少
// 优点：写快，读慢(需要compaction)
// 适用：实时写入、IoT


// ==================== 2. 自定义Payload ====================
public class MyPayload extends HoodieRecordPayload<MyData> {
    @Override
    public Option<MyData> combineAndGetUpdateValue(
            MyData existing, MyData incoming, Schema schema) {
        // 自定义合并逻辑
        if (incoming.getTimestamp() > existing.getTimestamp()) {
            return Option.of(incoming);
        }
        return Option.of(existing);
    }
    
    @Override
    public Option<MyData> getInsertValue(Schema schema) {
        return Option.of(this.getData());
    }
}


// ==================== 3. 自定义索引 ====================
public class MyIndex extends HoodieIndex<String, String> {
    @Override
    public Option<String> getLocation(String key, String partitionPath) {
        // 查询索引
        return lookupInCache(key, partitionPath);
    }
    
    @Override
    public void updateLocation(String key, String partitionPath, String fileId) {
        // 更新索引
        updateCache(key, partitionPath, fileId);
    }
}


// ==================== 4. 压缩策略 ====================
// 自定义压缩策略
public class MyCompactionStrategy extends CompactionStrategy {
    @Override
    public List<String> filterInstants(
            HoodieTable table, List<String> instants, int minInstantsToKeep) {
        // 过滤需要压缩的instant
        return instants.stream()
            .filter(i -> shouldCompact(i))
            .collect(Collectors.toList());
    }
}
```

### 3.4 复盘反思

```sql
CREATE TABLE hudi_development (
    dev_id BIGINT PRIMARY KEY,
    issue_type STRING,         -- TABLE_TYPE/INDEX/COMPACTION/PERFORMANCE
    symptom STRING,           -- 症状
    solution STRING,          -- 解决方案
    effect STRING,           -- 效果
    author STRING,
    create_time TIMESTAMP
);

INSERT INTO hudi_development 
VALUES 
('INDEX', '索引查询慢', 'Bloom索引误判率高', '切换到HBase索引', '查询延迟从100ms降至10ms', '平台组', NOW());
```

---

## 4. Iceberg二次开发

### 4.1 发现问题

**问题信号**：
- 快照过多：查询慢
- 写入失败：异常
- Manifest膨胀：读取慢
- Time Travel失效

### 4.2 定位问题

```java
// ==================== 1. 表结构分析 ====================
Table table = CatalogUtil.buildTable();
// Schema
Schema schema = table.schema();
// PartitionSpec
PartitionSpec spec = table.specs().get(specId);
// SortOrder
SortOrder order = table.sortOrders().get(orderId);

// ==================== 2. 快照分析 ====================
Iterable<Snapshot> snapshots = table.snapshots();
for (Snapshot snapshot : snapshots) {
    long snapshotId = snapshot.snapshotId();
    long timestamp = snapshot.timestampMillis();
    // Manifest文件
    List<ManifestFile> manifests = snapshot.manifestFiles();
}

// ==================== 3. Manifest分析 ====================
ManifestFile manifest = manifestFile;
List<ManifestEntry> entries = FileHelpers.readManifestEntries(manifest);
for (ManifestEntry entry : entries) {
    DataFile dataFile = entry.dataFile();
    // 文件信息
}
```

### 4.3 解决问题

```java
// ==================== 1. 分区管理 ====================
// 添加分区
Table table = ...
table.updateSpec()
    .addField(Expressions.year("timestamp"))
    .commit();

// ==================== 2. 排序优化 ====================
// 添加排序
table.updateSortOrder()
    .asc("ts", NullOrder.NULLS_LAST)
    .commit();

// ==================== 3. 快照过期 ====================
//  expireSnapshots
table.expireSnapshots()
    .expireOlderThan(expireTime)
    .commit();

// ==================== 4. 数据写入 ====================
// Append
table.newAppend()
    .appendFile(dataFile)
    .commit();

// Overwrite
table.newOverwrite()
    .addFile(newFile)
    .deleteFile(deleteFile)
    .commit();

// Row-level Delete
table.newDelete()
    .deleteByFilter(Expressions.equal("id", "123"))
    .commit();
```

### 4.4 复盘反思

```sql
CREATE TABLE iceberg_development (
    dev_id BIGINT PRIMARY KEY,
    issue_type STRING,      -- SNAPSHOT/MANIFEST/PERFORMANCE
    symptom STRING,        -- 症状
    solution STRING,       -- 解决方案
    effect STRING,         -- 效果
    author STRING,
    create_time TIMESTAMP
);

INSERT INTO iceberg_development 
VALUES 
('SNAPSHOT', '快照过多(1000+)', '定期清理过期快照', '保留30天', '查询性能提升50%', '平台组', NOW());
```

---

## 5. ClickHouse二次开发

### 5.1 发现问题

**问题信号**：
- 查询超时：延迟高
- 写入阻塞：响应慢
- 磁盘满：存储问题
- Merge卡住：后台任务慢

### 5.2 定位问题

```sql
-- ==================== 1. 查询分析 ====================
EXPLAIN PLAN SELECT ...

-- 2. 索引分析
SYSTEM TABLE REGIONS;

-- 3. Merge分析
SELECT 
    database,
    table,
    sum(rows),
    sum(bytes)
FROM system.merges
WHERE is_stale = 1
GROUP BY database, table;

-- 4. Part分析
SELECT 
    table,
    count(),
    sum(rows),
    max(modification_time)
FROM system.parts
WHERE database = 'default'
GROUP BY table;

-- 5. 写入分析
SYSTEM FLUSH LOGS;
SELECT * FROM system.query_log WHERE type != 'QueryStart';
```

### 5.3 解决问题

```sql
-- ==================== 1. 物化视图 ====================
-- 预聚合
CREATE MATERIALIZED VIEW mv_agg
ENGINE = SummingMergeTree()
ORDER BY (date, user_id)
AS SELECT 
    date,
    user_id,
    sum(amount) as amount,
    count() as cnt
FROM orders
GROUP BY date, user_id;

-- ==================== 2. 索引优化 ====================
ALTER TABLE orders ADD INDEX idx_user user TYPE bloom_filter GRANULARITY 1;
ALTER TABLE orders ADD INDEX idx_time toUInt32(time) TYPE minmax GRANULARITY 4;

-- ==================== 3. 分区裁剪 ====================
-- 按日期分区
ALTER TABLE orders MODIFY PARTITION BY toYYYYMM(date);

-- ==================== 4. 写入优化 ====================
-- 批量写入
INSERT INTO orders VALUES (1, '2024-01-01', 100), (2, '2024-01-01', 200);

-- ==================== 5. 分布式表 ====================
CREATE TABLE orders_dist AS orders
ENGINE = Distributed('cluster', 'default', 'orders', rand());
```

### 5.4 复盘反思

```sql
CREATE TABLE clickhouse_optimization (
    opt_id BIGINT PRIMARY KEY,
    issue_type STRING,     -- QUERY/INSERT/STORAGE/MERGE
    symptom STRING,       -- 症状
    solution STRING,      -- 解决方案
    effect STRING,        -- 效果
    author STRING,
    create_time TIMESTAMP
);

INSERT INTO clickhouse_optimization 
VALUES 
('QUERY', '查询超时10s', '添加物化视图', '查询降至100ms', '平台组', NOW());
```

---

## 6. StarRocks二次开发

### 6.1 发现问题

**问题信号**：
- 查询超时
- 导入失败
- BE节点挂
- 物化视图未刷新

### 6.2 定位问题

```sql
-- ==================== 1. 查询分析 ====================
SHOW FRONTEND CONFIG LIKE "%enable_profile%";
SET enable_profile = true;
-- 执行查询后
SHOW PROC "/statements/profile";

-- ==================== 2. 导入分析 ====================
SHOW EXPORT FROM db;

-- ==================== 3. BE状态 ====================
SHOW BACKENDS;

-- ==================== 4. 物化视图 ====================
SHOW MATERIALIZED VIEW;
```

### 6.3 解决问题

```sql
-- ==================== 1. 物化视图 ====================
-- 自动刷新
CREATE MATERIALIZED VIEW mv_agg
DISTRIBUTED BY HASH(user_id) BUCKETS 10
REFRESH ASYNC AS
SELECT 
    dt,
    user_id,
    SUM(amount) as amount,
    COUNT(*) as cnt
FROM orders
GROUP BY dt, user_id;

-- ==================== 2. 智能物化视图 ====================
CREATE MATERIALIZED VIEW mv_auto
PROPERTIES (
    "refresh_mode" = "auto",
    "refresh_interval" = "3600"
)
AS SELECT ...


-- ==================== 3. Join优化 ====================
SELECT /*+ BROADCAST(orders) */ *
FROM orders
JOIN users ON orders.user_id = users.id;

-- ==================== 4. Bitmap索引 ====================
CREATE INDEX idx_user ON orders (user_id) USING BITMAP;

-- ==================== 5. Bloom Filter ====================
CREATE INDEX idx_order ON orders (order_id) USING BLOOMFILTER;
```

### 6.4 复盘反思

```sql
CREATE TABLE starrocks_optimization (
    opt_id BIGINT PRIMARY KEY,
    issue_type STRING,     -- QUERY/IMPORT/MV/BE
    symptom STRING,        -- 症状
    solution STRING,       -- 解决方案
    effect STRING,         -- 效果
    author STRING,
    create_time TIMESTAMP
);

INSERT INTO starrocks_optimization 
VALUES 
('QUERY', '查询超时30s', '创建物化视图', '查询降至200ms', '平台组', NOW());
```

---

## 7. 性能优化

### 7.1 发现问题

**监控指标**：
- CPU使用率 > 80%
- 内存使用率 > 90%
- 磁盘IOPS高
- 网络带宽满

### 7.2 定位问题

```bash
# CPU分析
top -H -p <pid>

# 内存分析
jmap -heap <pid>
jstat -gcutil <pid> 1000

# IO分析
iostat -x 1

# 网络分析
netstat -an | grep <port>
```

### 7.3 解决问题

```properties
# Flink优化
taskmanager.memory.process.size: 8g
taskmanager.memory.flink.managed.size: 4g
taskmanager.network.memory.fraction: 0.15
execution.checkpointing.interval: 5min

# Spark优化
spark.executor.memory: 8g
spark.memory.fraction: 0.6
spark.sql.shuffle.partitions: 200
spark.serializer: org.apache.spark.serializer.KryoSerializer

# Hudi优化
hoodie.upsert.shuffle.parallelism: 200
hoodie.insert.shuffle.parallelism: 200
hoodie.datasource.merge.before: true
```

### 7.4 复盘反思

```sql
CREATE TABLE platform_performance_log (
    log_id BIGINT PRIMARY KEY,
    component STRING,
    issue_type STRING,      -- CPU/MEMORY/IO/NETWORK
    symptom STRING,          -- 症状
    root_cause STRING,      -- 根因
    solution STRING,         -- 解决方案
    effect STRING,          -- 效果
    duration_minutes INT,
    author STRING,
    create_time TIMESTAMP
);

INSERT INTO platform_performance_log 
VALUES 
('SPARK', 'OOM', 'Executor内存溢出', '数据量大，内存不足', '增加executor内存至8G', '任务成功完成', 30, '平台组', NOW());
```

---

## 8. 常见问题速查

### 8.1 Flink

| 问题 | 现象 | 方案 |
|------|------|------|
| Checkpoint慢 | 延迟增加 | 增量Checkpoint |
| 状态膨胀 | 内存增长 | 设置TTL |
| 背压 | 处理变慢 | 增加并行度 |
| 频繁重启 | 异常退出 | 调整重启策略 |

### 8.2 Spark

| 问题 | 现象 | 方案 |
|------|------|------|
| OOM | 失败 | 增加内存 |
| 倾斜 | 部分慢 | 加盐 |
| Shuffle | 卡住 | 调整partitions |

### 8.3 Hudi

| 问题 | 现象 | 方案 |
|------|------|------|
| 写入慢 | 延迟高 | 选择COW |
| 压缩频繁 | IO高 | 调整策略 |

### 8.4 ClickHouse

| 问题 | 现象 | 方案 |
|------|------|------|
| 查询慢 | 超时 | 物化视图 |
| 写入堵 | 延迟高 | 批量写入 |

### 8.5 StarRocks

| 问题 | 现象 | 方案 |
|------|------|------|
| 查询慢 | 超时 | 物化视图 |
| 导入慢 | 延迟高 | 调整并行度 |
