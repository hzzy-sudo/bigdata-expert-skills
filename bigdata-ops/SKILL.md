---
name: bigdata-ops
description: 大数据运维专家技能集。用于大数据集群部署、运维、故障排查、监控告警等工作。包括Hadoop、Kafka、Flink、Spark等组件的安装配置、性能调优、问题发现、定位与解决、复盘反思。此skill包含完整的发现问题-定位问题-解决问题-复盘反思闭环。
---

# 大数据运维专家

## 核心能力图谱

```
发现问题 ──→ 定位问题 ──→ 解决问题 ──→ 复盘反思
    │              │              │            │
    ▼              ▼              ▼            ▼
  监控告警      日志分析       参数调优      经验沉淀
  业务反馈      线程分析       资源调整      文档记录
  指标异常     dump分析        版本回滚      知识库
```

---

## 1. Hadoop集群部署与运维

### 1.1 发现问题

**监控指标**：
- NameNode内存使用率 > 80%
- DataNode磁盘使用率 > 85%
- YARN队列资源使用率 > 90%
- 任务失败率 > 1%

**业务反馈**：
- 作业提交失败
- 任务运行缓慢

**系统告警**：
- 磁盘空间不足
- JVM OOM
- RPC超时

### 1.2 定位问题

```bash
# 1. 检查集群健康状态
hdfs dfsadmin -report
yarn rmadmin -getServiceState rm1

# 2. 检查NameNode状态
hdfs fsck / -files -blocks -locations
hdfs namenode -safemode get

# 3. 检查YARN队列
yarn queue -status default
yarn application -list -states RUNNING

# 4. 检查日志
tail -f /opt/hadoop/logs/hadoop-root-namenode-*.log
tail -f /opt/hadoop/logs/yarn-root-resourcemanager-*.log

# 5. JVM分析
jstack <namenode_pid>
jmap -heap <namenode_pid>
jstat -gcutil <namenode_pid> 1000

# 6. 线程分析
ps -T -p <pid>
top -H -p <pid>
```

### 1.3 解决问题

```bash
# ==================== 问题1：NameNode OOM ====================
# 原因：元数据内存不足
# 解决：
# 1. 调整JVM堆内存
export HADOOP_NAMENODE_OPTS="-Xms4g -Xmx4g -XX:+UseG1GC"

# 2. 优化元数据
hdfs dfsadmin -saveNamespace  # 合并fsimage

# 3. 限制连接数
dfs.namenode.handler.count=100


# ==================== 问题2：DataNode磁盘满 ====================
# 解决：
# 1. 扩容
hdfs dfsadmin -report

# 2. 清理数据
hdfs dfs -rm -r -skipTrash /tmp/*

# 3. 添加新磁盘
hdfs dfsadmin -refreshNodes


# ==================== 问题3：YARN资源争抢 ====================
# 解决：
# 1. 配置队列
yarn-scheduler.xml:
<property>
  <name>yarn.scheduler.capacity.root.default.capacity</name>
  <value>40</value>
</property>

# 2. 限制单用户资源
yarn.scheduler.capacity.root.default.user-limit-factor=1


# ==================== 问题4：HDFS小文件多 ====================
# 解决：
# 1. 定期合并
hdfs balancer -threshold 10

# 2. 使用Archive
hdfs archive -archiveName myarchive.har -p /user/input /user/archive
```

### 1.4 复盘反思

```sql
-- 创建运维问题记录表
CREATE TABLE ops_issue_log (
    issue_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    component STRING,           -- HDFS/YARN/HIVE
    issue_type STRING,         -- OOM/PERFORMANCE/DISK/NETWORK
    symptom STRING,             -- 症状
    root_cause STRING,         -- 根因
    solution STRING,           -- 解决方案
    prevention STRING,        -- 预防措施
    find_time TIMESTAMP,
    resolve_time TIMESTAMP,
    owner STRING
);

-- 记录问题
INSERT INTO ops_issue_log (component, issue_type, symptom, root_cause, solution, prevention, find_time, resolve_time, owner)
VALUES 
(
    'HDFS',
    'OOM',
    'NameNode内存使用率95%',
    '元数据过大，未定期清理',
    '增加堆内存，合并fsimage',
    '设置定期合并任务',
    '2024-01-01 10:00:00',
    '2024-01-01 12:00:00',
    '运维组'
);
```

---

## 2. Kafka集群部署与运维

### 2.1 发现问题

**监控指标**：
- 消费延迟 > 10000条
- 磁盘使用率 > 80%
- 副本Lag > 1000
- 消息积压率 > 50%

**告警**：
- Broker down
- Partition leader不可用
- ISR收缩

### 2.2 定位问题

```bash
# 1. 检查消费延迟
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group test-group \
  --describe

# 2. 检查Broker状态
kafka-broker-api-versions.sh --bootstrap-server localhost:9092

# 3. 检查Topic状态
kafka-topics.sh --describe \
  --topic test-topic \
  --bootstrap-server localhost:9092

# 4. 检查ISR
kafka-topics.sh --describe \
  --topic test-topic \
  --bootstrap-server localhost:9092 | grep ISR

# 5. 检查日志
tail -f /opt/kafka/logs/server.log

# 6. 检查磁盘IO
iostat -x 1
```

### 2.3 解决问题

```bash
# ==================== 问题1：消息积压 ====================
# 原因：消费者处理慢/消费者少
# 解决：
# 1. 增加消费者
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group test-group \
  --reset-offsets \
  --to-earliest \
  --topic test-topic \
  --execute

# 2. 调整fetch参数
fetch.min.bytes=1
fetch.max.wait.ms=500
max.poll.records=500

# 3. 增加分区
kafka-topics.sh --alter \
  --topic test-topic \
  --partitions 12 \
  --bootstrap-server localhost:9092


# ==================== 问题2：磁盘满 ====================
# 解决：
# 1. 清理日志
kafka-log-dirs.sh \
  --bootstrap-server localhost:9092 \
  --topic-list test-topic \
  --delete-configured-dirs

# 2. 调整retention
kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --alter \
  --topic test-topic \
  --add-config retention.ms=604800000


# ==================== 问题3：ISR收缩 ====================
# 原因：Broker落后/网络抖动
# 解决：
# 1. 检查Broker状态
# 2. 调整参数
replica.lag.time.max.ms=30000
replica.socket.timeout.ms=30000


# ==================== 问题4：Partition Leader不平衡 ====================
# 解决：
kafka-leader-election.sh \
  --election-type PREFERRED \
  --topic test-topic \
  --partition 0,1,2,3 \
  --bootstrap-server localhost:9092
```

### 2.4 复盘反思

```sql
CREATE TABLE kafka_issue_log (
    issue_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    issue_type STRING,
    symptom STRING,
    root_cause STRING,
    solution STRING,
    prevention STRING,
    find_time TIMESTAMP,
    resolve_time TIMESTAMP
);

INSERT INTO kafka_issue_log VALUES 
('MESSAGES_LAG', '消费延迟10万条', '消费者处理慢', '增加消费者数', '监控消费速率', NOW(), NOW());
```

---

## 3. Flink集群部署与运维

### 3.1 发现问题

**监控指标**：
- Checkpoint时间 > 5分钟
- 反压Task > 0
- Task运行时间 > 预期2倍
- 状态大小 > 10GB

**告警**：
- Job失败
- Checkpoint失败
- Task重启

### 3.2 定位问题

```bash
# 1. 查看Job状态
flink list -r

# 2. 查看反压
curl http://jobmanager:8081/jobs/<jobid>/vertices/<vertexid>/backpressure

# 3. 查看Checkpoin
curl http://jobmanager:8081/jobs/<jobid>/checkpoints

# 4. 查看TaskManager
curl http://jobmanager:8081/taskmanagers

# 5. 查看日志
tail -f /opt/flink/log/flink-*-taskmanager-*.log

# 6. 分析GC
jstat -gcutil <taskmanager_pid> 1000
```

### 3.3 解决问题

```bash
# ==================== 问题1：Checkpoint超时 ====================
# 解决：
# 1. 调整参数
execution.checkpointing.interval=10min
execution.checkpointing.timeout=15min
execution.checkpointing.min-pause=5min

# 2. 增量Checkpoint
state.backend.incremental=true

# 3. 调整状态后端
state.backend=rocksdb


# ==================== 问题2：反压 ====================
# 解决：
# 1. 增加并行度
env.setParallelism(8)

# 2. 调整缓冲区
taskmanager.network.memory.fraction=0.15
taskmanager.network.buffer-per-channel=2mb

# 3. 减少数据倾斜
# 使用keyBy加盐


# ==================== 问题3：状态膨胀 ====================
# 解决：
# 1. 设置TTL
state.ttl=2d
state.cleanup.min-pause=1h

# 2. 定期清理
# 使用RocksDB自动压缩


# ==================== 问题4：Task频繁重启 ====================
# 解决：
# 1. 增加RestartStrategy
env.setRestartStrategy(
    RestartStrategies.exponentialDelayRestart(
        1000,  // initialDelay
        2.0,   // multiplier
        600000  // maxDelay
    )
)

# 2. 调整TM资源
taskmanager.memory.process.size=8g
taskmanager.heap.size=6g
```

### 3.4 复盘反思

```sql
CREATE TABLE flink_issue_log (
    issue_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    job_name STRING,
    issue_type STRING,      -- CHECKPOINT/BACKPRESSURE/RESTART/OOM
    symptom STRING,
    root_cause STRING,
    solution STRING,
    prevention STRING,
    find_time TIMESTAMP,
    resolve_time TIMESTAMP
);

INSERT INTO flink_issue_log VALUES 
('realtime_order', 'CHECKPOINT', 'Checkpoint超时10分钟', '状态过大', '开启增量Checkpoint+TTL', '监控状态大小', NOW(), NOW());
```

---

## 4. Spark集群部署与运维

### 4.1 发现问题

**监控指标**：
- 任务执行时间 > 预期2倍
- Stage数量 > 20
- Shuffle数据量 > 100GB
- Executor OOM

**告警**：
- 任务失败
- Driver OOM
- Shuffle fetch失败

### 4.2 定位问题

```bash
# 1. 查看UI
spark.sparkContext.uiWebUrl

# 2. 查看Stage
spark.sparkContext.statusTracker().getJobIds()

# 3. 查看Executor
spark.executor.memoryUsed()
spark.executor.memory()

# 4. 分析Shuffle
spark.sql.shuffle.partitions

# 5. 查看日志
tail -f /opt/spark/logs/spark-*-org.apache.spark.*
```

### 4.3 解决问题

```bash
# ==================== 问题1：OOM ====================
# 解决：
# 1. 调整内存
spark.executor.memory=8g
spark.driver.memory=4g

# 2. 调整内存比例
spark.memory.fraction=0.6
spark.memory.storageFraction=0.5

# 3. 优化代码
# 避免collect()大表
# 使用filter提前过滤


# ==================== 问题2：数据倾斜 ====================
# 解决：
# 1. 加盐打散
df.withColumn("salt", (rand * 10).cast(IntegerType))

# 2. 广播小表
spark.sql.autoBroadcastJoinThreshold=10485760


# ==================== 问题3：Shuffle文件多 ====================
# 解决：
# 1. 减少分区
spark.sql.shuffle.partitions=200

# 2. 合并小文件
spark.sql.files.maxPartitionBytes=128MB


# ==================== 问题4：GC频繁 ====================
# 解决：
# 1. 调整GC
spark.executor.extraJavaOptions=-XX:+UseG1GC

# 2. 增加内存
spark.memory.fraction=0.5
```

### 4.4 复盘反思

```sql
CREATE TABLE spark_issue_log (
    issue_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    app_name STRING,
    issue_type STRING,      -- OOM/SHUFFLE/SLOW/GC
    symptom STRING,
    root_cause STRING,
    solution STRING,
    prevention STRING,
    find_time TIMESTAMP,
    resolve_time TIMESTAMP
);

INSERT INTO spark_issue_log VALUES 
('etl_job', 'OOM', 'Executor OOM', '数据量大，内存不足', '增加executor内存', '监控内存使用', NOW(), NOW());
```

---

## 5. 监控告警体系

### 5.1 监控指标

| 组件 | 指标 | 阈值 | 告警级别 |
|------|------|------|----------|
| HDFS | DataNode存活率 | <100% | P0 |
| YARN | 队列使用率 | >90% | P1 |
| Kafka | 消费Lag | >10000 | P1 |
| Flink | Checkpoint时间 | >10min | P1 |
| Spark | 任务失败率 | >1% | P0 |

### 5.2 告警配置

```yaml
# Prometheus告警规则
groups:
  - name: hadoop_alerts
    rules:
      - alert: DataNodeDown
        expr: up{job="datanode"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "DataNode宕机"
          
      - alert: HighDiskUsage
        expr: (1 - node_filesystem_avail_bytes / node_filesystem_size_bytes) > 0.85
        for: 5m
        labels:
          severity: warning

  - name: kafka_alerts
    rules:
      - alert: MessageLag
        expr: kafka_consumer_group_lag > 10000
        for: 5m
        labels:
          severity: warning
```

---

## 6. 常见问题速查表

### 6.1 Hadoop

| 问题 | 现象 | 原因 | 方案 |
|------|------|------|------|
| NameNode OOM | 无法写入 | 元数据过大 | 增加内存，合并fsimage |
| DataNode磁盘满 | 写入失败 | 数据增长快 | 清理数据，扩容 |
| YARN资源争抢 | 任务排队 | 队列配置不合理 | 调整队列容量 |

### 6.2 Kafka

| 问题 | 现象 | 原因 | 方案 |
|------|------|------|------|
| 消费延迟 | Lag增加 | 消费者少/处理慢 | 增加消费者，优化代码 |
| 磁盘满 | 写入失败 | retention未设置 | 调整retention |
| ISR收缩 | 副本同步慢 | Broker落后 | 检查网络，调整参数 |

### 6.3 Flink

| 问题 | 现象 | 原因 | 方案 |
|------|------|------|------|
| Checkpoint慢 | 延迟增加 | 状态过大 | 增量Checkpoint |
| 反压 | 处理变慢 | 下游瓶颈 | 增加并行度 |
| 状态膨胀 | 内存增长 | 未清理 | 设置TTL |

### 6.4 Spark

| 问题 | 现象 | 原因 | 方案 |
|------|------|------|------|
| OOM | 任务失败 | 内存不足 | 增加内存，优化代码 |
| 数据倾斜 | 部分Task慢 | Key不均 | 加盐打散 |
| Shuffle慢 | Stage卡住 | 并行度低 | 调整partitions |
