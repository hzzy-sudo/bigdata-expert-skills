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
- Checkpoint失败
- 状态丢失
- 背压持续
- 内存泄漏

### 1.2 定位问题

```java
// 1. 追踪JobGraph生成
// 入口：StreamGraphGenerator.generate()

// 2. 追踪StreamGraph -> JobGraph
// 入口：JobGraphGenerator.createJobGraph()

// 3. 追踪Task执行
// 入口：StreamTask.execute()

// 4. 追踪Checkpoint
// 入口：CheckpointCoordinator.triggerCheckpoint()

// 调试技巧
// 添加断点在关键方法
// StreamGraphGenerator.java:142
// JobGraphGenerator.java:234
// CheckpointCoordinator.java:456
```

### 1.3 解决问题

```java
// 1. 自定义Source
public class MySource implements SourceFunction<String> {
    private volatile boolean isRunning = true;
    
    @Override
    public void run(SourceContext<String> ctx) {
        while (isRunning) {
            String data = fetchData();
            ctx.collect(data);
            Thread.sleep(1000);
        }
    }
    
    @Override
    public void cancel() {
        isRunning = false;
    }
}

// 2. 自定义Sink
public class MySink implements SinkFunction<String> {
    @Override
    public void invoke(String value, Context context) {
        connection.write(value);
    }
}

// 3. 自定义函数
public class MyMapFunction implements MapFunction<String, String> {
    @Override
    public String map(String value) {
        return process(value);
    }
}
```

### 1.4 复盘反思

```sql
CREATE TABLE flink_source_analysis (
    analysis_id BIGINT PRIMARY KEY,
    component STRING,          -- FLINK/SPARK/HUDI
    issue_type STRING,       -- CHECKPOINT/BACKPRESSURE/MEMORY
    symptom STRING,           -- 症状
    root_cause STRING,       -- 根因
    analysis_path STRING,    -- 源码路径
    solution STRING,          -- 解决方案
    prevention STRING,       -- 预防措施
    author STRING,
    create_time TIMESTAMP
);
```

---

## 2. Spark源码分析

### 2.1 发现问题

**问题信号**：
- DAG划分不合理
- Stage数量过多
- Shuffle数据量大
- 内存溢出

### 2.2 定位问题

```scala
// 1. 追踪DAG生成
// 入口：DAGScheduler.submitJob()

// 2. 追踪Stage划分
// 入口：DAGScheduler.createStage()

// 3. 追踪Task执行
// 入口：TaskSetManager.resourceOffer()

// 4. 追踪Shuffle
// 入口：ShuffleManager.getWriter()

// 调试技巧
spark.sparkContext.setJobGroup("job1", "description")
```

### 2.3 解决问题

```scala
// 1. 自定义Partitioner
class MyPartitioner extends Partitioner {
    override def numPartitions: Int = 100
    override def getPartition(key: Any): Int = {
        key.hashCode() % numPartitions
    }
}

// 2. 自定义UDF
val myUDF = udf((s: String) => s.toUpperCase)

// 3. 优化内存
spark.memory.fraction = 0.6
spark.memory.storageFraction = 0.5
```

### 2.4 复盘反思

```sql
CREATE TABLE spark_source_analysis (
    analysis_id BIGINT PRIMARY KEY,
    issue_type STRING,
    root_cause STRING,
    source_path STRING,
    solution STRING,
    author STRING,
    create_time TIMESTAMP
);
```

---

## 3. Hudi二次开发

### 3.1 发现问题

**问题信号**：
- 表格式选择错误
- 写入性能差
- 压缩频率高

### 3.2 定位问题

```java
// 1. 分析表类型
 HoodieTableType tableType = table.getTableType();

// 2. 分析WriteConfig
WriteConfig config = table.getConfig();

// 3. 分析索引效率
HoodieIndex index = table.getIndex();

// 4. 分析压缩策略
HoodieCompactionStrategy strategy = table.getCompactionStrategy();
```

### 3.3 解决问题

```java
// 1. 选择表类型
// COW: 读多写少
// MOR: 写多读少

// 2. 自定义Payload
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
}

// 3. 自定义索引
public class MyIndex extends HoodieIndex<String, String> {
    // 实现索引逻辑
}
```

### 3.4 复盘反思

```sql
CREATE TABLE hudi_development (
    dev_id BIGINT PRIMARY KEY,
    issue_type STRING,     -- TABLE_TYPE/COMPACTION/INDEX
    solution STRING,
    author STRING,
    create_time TIMESTAMP
);
```

---

## 4. Iceberg二次开发

### 4.1 发现问题

**问题信号**：
- 写入失败
- 快照过多
- 读取慢

### 4.2 定位问题

```java
// 1. 分析Table
Table table = CatalogUtil.buildTable();

// 2. 分析Snapshot
Iterable<Snapshot> snapshots = table.snapshots();

// 3. 分析Manifest
List<ManifestFile> manifests = snapshot.manifestFiles();
```

### 4.3 解决问题

```java
// 1. 自定义Partition
PartitionSpec spec = PartitionSpec.builderFor(table.schema())
    .bucket("id", 16)
    .identity("category")
    .build();

// 2. 自定义SortOrder
SortOrder order = SortOrder.builderFor(table.schema())
    .asc("ts")
    .build();

// 3. 优化写入
Table table = ...
table.newAppend()
    .appendFile(dataFile)
    .commit();
```

### 4.4 复盘反思

```sql
CREATE TABLE iceberg_development (
    dev_id BIGINT PRIMARY KEY,
    issue_type STRING,
    solution STRING,
    author STRING,
    create_time TIMESTAMP
);
```

---

## 5. ClickHouse二次开发

### 5.1 发现问题

**问题信号**：
- 查询慢
- 写入阻塞
- 磁盘空间满

### 5.2 定位问题

```sql
-- 1. 分析查询
EXPLAIN PLAN SELECT ...

-- 2. 分析索引
SYSTEM TABLE REGIONS;

-- 3. 分析Merge
SELECT 
    database,
    table,
    sum(rows),
    sum(bytes)
FROM system.merges
WHERE is_stale = 1
GROUP BY database, table;

-- 4. 分析Part
SELECT 
    table,
    count(),
    sum(rows),
    max(modification_time)
FROM system.parts
WHERE database = 'default'
GROUP BY table;
```

### 5.3 解决问题

```sql
-- 1. 物化视图优化
CREATE MATERIALIZED VIEW mv_agg
ENGINE = SummingMergeTree()
ORDER BY (date, user_id)
AS SELECT 
    date,
    user_id,
    sum(amount) as amount
FROM orders
GROUP BY date, user_id;

-- 2. 索引优化
ALTER TABLE orders ADD INDEX idx_user user TYPE bloom_filter GRANULARITY 1;

-- 3. 分区裁剪
SELECT * FROM orders
WHERE date >= '2024-01-01' AND date < '2024-02-01';
```

### 5.4 复盘反思

```sql
CREATE TABLE clickhouse_optimization (
    opt_id BIGINT PRIMARY KEY,
    issue_type STRING,     -- QUERY/INSERT/STORAGE
    solution STRING,
    effect STRING,
    author STRING,
    create_time TIMESTAMP
);
```

---

## 6. StarRocks二次开发

### 6.1 发现问题

**问题信号**：
- 查询超时
- 导入失败
- BE节点挂

### 6.2 定位问题

```sql
-- 1. 分析查询
SHOW FRONTEND CONFIG LIKE "%enable_profile%";
SET enable_profile = true;
-- 执行查询后
SHOW PROC "/statements/profile";

-- 2. 分析导入
SHOW EXPORT FROM db;

-- 3. 分析BE
SHOW BACKENDS;
```

### 6.3 解决问题

```sql
-- 1. 物化视图
CREATE MATERIALIZED VIEW mv_agg
AS SELECT 
    dt,
    user_id,
    SUM(amount) as amount,
    COUNT(*) as cnt
FROM orders
GROUP BY dt, user_id;

-- 2. 智能物化视图
CREATE MATERIALIZED VIEW mv_auto
DISTRIBUTED BY HASH(user_id) BUCKETS 10
PROPERTIES (
    "refresh_mode" = "auto"
)
AS SELECT ...
FROM orders;

-- 3. Join优化
SELECT /*+ BROADCAST(orders) */ *
FROM orders
JOIN users ON orders.user_id = users.id;
```

### 6.4 复盘反思

```sql
CREATE TABLE starrocks_optimization (
    opt_id BIGINT PRIMARY KEY,
    issue_type STRING,     -- QUERY/IMPORT/BE
    solution STRING,
    effect STRING,
    author STRING,
    create_time TIMESTAMP
);
```

---

## 7. 性能优化

### 7.1 发现问题

**问题信号**：
- CPU高
- 内存高
- IO高
- 网络高

### 7.2 定位问题

```bash
# 1. CPU分析
top -H -p <pid>

# 2. 内存分析
jmap -heap <pid>
jstat -gcutil <pid> 1000

# 3. IO分析
iostat -x 1

# 4. 网络分析
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
```

### 7.4 复盘反思

```sql
CREATE TABLE platform_performance_log (
    log_id BIGINT PRIMARY KEY,
    component STRING,
    issue_type STRING,      -- CPU/MEMORY/IO/NETWORK
    symptom STRING,
    root_cause STRING,
    solution STRING,
    effect STRING,
    author STRING,
    create_time TIMESTAMP
);
```

---

## 8. 常见问题速查

### 8.1 Flink

| 问题 | 现象 | 方案 |
|------|------|------|
| Checkpoint慢 | 延迟增加 | 增量Checkpoint |
| 状态膨胀 | 内存增长 | 设置TTL |
| 背压 | 处理变慢 | 增加并行度 |

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
