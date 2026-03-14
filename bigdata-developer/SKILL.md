---
name: bigdata-developer
description: 大数据研发专家技能集。用于数仓建设、ETL开发、数据治理、指标体系建设。包括离线数仓、实时数仓、数仓建模、维度设计、Flink SQL/Hive SQL/Spark SQL性能调优、问题排查与复盘。适用于需要构建企业级数仓，开发ETL任务、进行数据治理、建立指标体系的场景。此skill包含完整的发现问题-定位问题-解决问题-复盘反思闭环。
---

# 大数据研发专家

## 核心能力图谱

```
发现问题 ──→ 定位问题 ──→ 解决问题 ──→ 复盘反思
    │              │              │            │
    ▼              ▼              ▼            ▼
  监控告警      日志分析       参数调优      经验沉淀
  业务反馈      SQL分析        代码优化      文档记录
  数据异常      执行计划       资源调整      知识库
```

---

## 1. 数仓分层架构设计

### 1.1 发现问题

**监控指标**：
- 任务失败率 > 1%
- 任务延迟 > 30分钟
- 数据量环比变化 > 20%

**业务反馈**：
- 业务方投诉数据不准
- 报表数据与业务系统不一致

**数据异常**：
- 关键指标环比波动 > 30%
- null值比例突然增加

### 1.2 定位问题

```sql
-- 1. 检查任务执行日志
SHOW BATCH;

-- 2. 查看历史任务状态
SELECT * FROM information_schema.datalake_task_history 
WHERE task_name = 'dwd_order' 
  AND dt = '${dt}'
  AND execution_time > '2024-01-01';

-- 3. 对比数据量变化
SELECT 
    dt,
    COUNT(*) as cnt,
    LAG(COUNT(*), 1) OVER (ORDER BY dt) as yesterday_cnt,
    (COUNT(*) - LAG(COUNT(*), 1) OVER (ORDER BY dt)) * 100.0 / LAG(COUNT(*), 1) OVER (ORDER BY dt) as change_pct
FROM dwd_order
WHERE dt >= DATE_SUB('${dt}', 7)
GROUP BY dt;

-- 4. 检查数据分布
SELECT 
    status,
    COUNT(*) as cnt,
    COUNT(DISTINCT user_id) as user_cnt
FROM dwd_order
WHERE dt = '${dt}'
GROUP BY status;
```

### 1.3 解决问题

```sql
-- 1. ODS层：保持原貌，不做处理
INSERT OVERWRITE TABLE ods_order PARTITION(dt='${dt}')
SELECT 
    *,
    CURRENT_TIMESTAMP() as etl_time,
    'ods' as layer
FROM source_order
WHERE dt = '${dt}';

-- 2. DWD层：清洗规则
INSERT OVERWRITE TABLE dwd_order PARTITION(dt='${dt}')
SELECT 
    -- 主键
    COALESCE(CAST(order_id AS BIGINT), 0) as order_id,
    
    -- 外键（维度关联）
    COALESCE(CAST(user_id AS INT), -1) as user_key,
    COALESCE(CAST(product_id AS INT), -1) as product_key,
    
    -- 度量值清洗
    COALESCE(TRY_CAST(amount AS DECIMAL(15,2)), 0) as order_amount,
    COALESCE(TRY_CAST(pay_amount AS DECIMAL(15,2)), 0) as pay_amount,
    
    -- 时间标准化
    COALESCE(
        TO_TIMESTAMP(create_time, 'yyyy-MM-dd HH:mm:ss'),
        TO_TIMESTAMP(CONCAT(dt, ' 00:00:00'))
    ) as order_time,
    
    -- 状态标准化
    CASE 
        WHEN status IN ('PAID', 'PAY', 'p') THEN 'PAID'
        WHEN status IN ('SHIPPED', 'SHIP', 's') THEN 'SHIPPED'
        WHEN status IN ('CONFIRMED', 'CONFIRM', 'c') THEN 'CONFIRMED'
        WHEN status IN ('CANCELLED', 'CANCEL', 'x') THEN 'CANCELLED'
        ELSE 'UNKNOWN'
    END as order_status,
    
    -- 脱敏处理
    CASE 
        WHEN phone IS NOT NULL AND LENGTH(phone) = 11 
        THEN CONCAT(LEFT(phone, 3), '****', RIGHT(phone, 4))
        ELSE NULL
    END as phone_masked,
    
    -- ETL审计
    CURRENT_TIMESTAMP() as etl_time,
    'dwd' as layer,
    'dwd_order' as task_name
FROM ods_order
WHERE dt = '${dt}'
  AND order_id IS NOT NULL
  AND LENGTH(order_id) > 0;
```

### 1.4 复盘反思

```sql
-- 创建问题记录表
CREATE TABLE dwd_issue_log (
    issue_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    task_name STRING,
    issue_type STRING,           -- DATA_QUALITY/PERFORMANCE/LOGIC
    issue_desc STRING,
    root_cause STRING,
    solution STRING,
    impact_scope STRING,
    find_time TIMESTAMP,
    resolve_time TIMESTAMP,
    owner STRING
);

-- 记录问题
INSERT INTO dwd_issue_log (task_name, issue_type, issue_desc, root_cause, solution, find_time, resolve_time, owner)
VALUES 
(
    'dwd_order',
    'DATA_QUALITY',
    '订单金额为负数',
    '源系统数据未做校验',
    '增加金额字段校验，负数置为0',
    '2024-01-01 10:00:00',
    '2024-01-01 14:00:00',
    '数据组'
);
```

---

## 2. 维度建模

### 2.1 发现问题

**问题信号**：
- 维度表膨胀快
- 同一实体多个版本
- 业务方不理解维度含义

### 2.2 定位问题

```sql
-- 1. 检查维度表膨胀
SELECT 
    table_name,
    COUNT(*) as record_count,
    MAX(update_time) as last_update
FROM information_schema.tables
WHERE table_schema = 'dwd'
  AND table_name LIKE 'dim_%'
GROUP BY table_name
HAVING COUNT(*) > 1000000;

-- 2. 检查SCD变化情况
SELECT 
    user_id,
    COUNT(*) as versions,
    MIN(start_date) as first_version,
    MAX(start_date) as latest_version
FROM dim_user_scd
GROUP BY user_id
HAVING COUNT(*) > 5;

-- 3. 检查历史分区残留
SHOW PARTITIONS dim_user PARTITIONS;
```

### 2.3 解决问题

```sql
-- 1. SCD Type2 实现
-- 初始化
INSERT INTO dim_user_scd
SELECT 
    user_key,
    user_id,
    user_name,
    phone,
    email,
    '1900-01-01' as start_date,
    '2999-12-31' as end_date,
    TRUE as is_current,
    1 as version,
    CURRENT_TIMESTAMP() as created_at
FROM (
    SELECT 
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at) as rn,
        *
    FROM source_user
) t
WHERE t.rn = 1;

-- 2. 增量更新（处理SCD变化）
-- 步骤1：关闭旧版本
UPDATE dim_user_scd
SET end_date = DATE_SUB(CURRENT_DATE, 1),
    is_current = FALSE,
    updated_at = CURRENT_TIMESTAMP()
WHERE user_id IN (
    SELECT user_id FROM source_user WHERE update_time > CURRENT_DATE
)
AND is_current = TRUE;

-- 步骤2：插入新版本
INSERT INTO dim_user_scd
SELECT 
    (SELECT MAX(user_key) FROM dim_user_scd) + ROW_NUMBER() OVER() as user_key,
    user_id,
    user_name,
    phone,
    email,
    CURRENT_DATE as start_date,
    '2999-12-31' as end_date,
    TRUE as is_current,
    version + 1 as version,
    CURRENT_TIMESTAMP() as created_at
FROM source_user 
WHERE update_time > CURRENT_DATE;

-- 3. 定期清理历史版本（保留最近N个）
DELETE FROM dim_user_scd
WHERE user_id IN (
    SELECT user_id 
    FROM (
        SELECT 
            user_id,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY version DESC) as rn
        FROM dim_user_scd
    ) t
    WHERE t.rn > 10
);
```

### 2.4 复盘反思

```sql
-- 创建维度变更记录
CREATE TABLE dim_change_log (
    change_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    dim_name STRING,
    change_type STRING,        -- ADD_COLUMN/SCD_CHANGE/VALUE_CHANGE
    before_value STRING,
    after_value STRING,
    change_reason STRING,
    approved_by STRING,
    change_time TIMESTAMP
);

-- 记录维度变更
INSERT INTO dim_change_log (dim_name, change_type, before_value, after_value, change_reason, approved_by, change_time)
VALUES 
(
    'dim_user',
    'SCD_CHANGE',
    'version=1',
    'version=2',
    '用户手机号变更',
    '数据组负责人',
    CURRENT_TIMESTAMP()
);
```

---

## 3. Hive SQL性能调优

### 3.1 发现问题

**问题信号**：
- SQL执行时间 > 10分钟
- 任务卡在reduce
- 内存溢出(OOM)

**监控告警**：
- 执行时间 > 5分钟
- 数据量 > 1亿
- shuffle数据量 > 100GB

### 3.2 定位问题

```sql
-- 1. 查看执行计划
EXPLAIN EXTENDED 
SELECT a.*, b.name 
FROM large_table a 
JOIN dim_table b ON a.id = b.id 
WHERE a.dt = '${dt}';

-- 2. 查看Stage详情
SET hive.explain.user=TRUE;
-- 观察Map和Reduce数量

-- 3. 分析数据分布
SELECT 
    key,
    COUNT(*) as cnt,
    AVG(value) as avg_value
FROM large_table
WHERE dt = '${dt}'
GROUP BY key
ORDER BY cnt DESC
LIMIT 20;

-- 4. 检查小文件
!ls -l /warehouse/table/path/ | head -20;

-- 5. 检查Join倾斜
SELECT 
    key,
    COUNT(*) as cnt
FROM (
    SELECT explode(ARRAY(1,2,3)) as key
) t
GROUP BY key
HAVING COUNT(*) > 10000000;
```

### 3.3 解决问题

```sql
-- ==================== 优化1：分区裁剪 ====================
-- 优化前
SELECT * FROM table WHERE YEAR(dt) = 2024;

-- 优化后
SELECT * FROM table WHERE dt >= '2024-01-01' AND dt < '2025-01-01';


-- ==================== 优化2：小表广播 ====================
-- 优化前
SELECT a.*, b.name 
FROM fact a 
JOIN large_dim b ON a.key = b.key;  -- 大表JOIN大表

-- 优化后
SELECT /*+ BROADCAST(d) */ a.*, d.name
FROM fact a
JOIN (
    SELECT key, name FROM large_dim
) d ON a.key = d.key;


-- ==================== 优化3：列裁剪 ====================
-- 优化前
SELECT * FROM table;

-- 优化后
SELECT key, value, dt FROM table;


-- ==================== 优化4：避免Shuffle ====================
-- 优化前：GROUP BY触发Shuffle
SELECT key, COUNT(*) FROM table GROUP BY key;

-- 优化后：利用预聚合
INSERT OVERWRITE TABLE agg_table PARTITION(dt='${dt}')
SELECT key, COUNT(*) FROM table WHERE dt = '${dt}' GROUP BY key;


-- ==================== 优化5：小文件合并 ====================
-- 方案1：设置参数
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
SET hive.merge.size.per.task=256000000;
SET hive.merge.smallfiles.avgsize=16000000;

-- 方案2：定期合并
ALTER TABLE table PARTITION(dt='${dt}') CONCATENATE;


-- ==================== 优化6：分桶 ====================
-- 创建分桶表
CREATE TABLE bucket_table (
    id INT,
    name STRING
) CLUSTERED BY (id) INTO 32 BUCKETS;

-- 分桶JOIN
SELECT /*+ MAPJOIN(b) */ a.*, b.name
FROM bucket_table a
JOIN bucket_table b ON a.id = b.id;


-- ==================== 优化7：向量化执行 ====================
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;


-- ==================== 优化8： CBO优化 ====================
SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.fetch.column.stats=true;
ANALYZE TABLE table COMPUTE STATISTICS;
ANALYZE TABLE table COMPUTE STATISTICS FOR COLUMNS key,value;
```

### 3.4 复盘反思

```sql
-- 创建SQL优化记录表
CREATE TABLE sql_optimization_log (
    log_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    sql_text STRING,
    problem_type STRING,        -- SHUFFLE/SKEW/IO/MEMORY
    before_cost STRING,        -- 执行时间/资源
    after_cost STRING,
    optimization_method STRING,
    effect_pct STRING,         -- 提升百分比
    author STRING,
    create_time TIMESTAMP
);

-- 记录优化
INSERT INTO sql_optimization_log (sql_text, problem_type, before_cost, after_cost, optimization_method, effect_pct, author, create_time)
VALUES 
(
    'SELECT * FROM fact JOIN dim ON fact.key = dim.key',
    'SKEW',
    '30分钟',
    '3分钟',
    '分桶+广播',
    '90%',
    '数据组',
    CURRENT_TIMESTAMP()
);
```

---

## 4. Spark SQL性能调优

### 4.1 发现问题

**问题信号**：
- Stage数量过多
- Task分布不均
- 内存使用率100%
- GC频繁

### 4.2 定位问题

```scala
// 1. 查看执行计划
df.explain(true)

// 2. 查看Stage划分
spark.sparkContext.uiWebUrl

// 3. 查看Shuffle数据量
spark.sql.shuffle.partitions  // 默认200

// 4. 检查数据倾斜
val skewAnalysis = df.groupBy("key")
  .agg(count("*") as "cnt")
  .orderBy($"cnt".desc)
  .limit(10)

// 5. 检查内存
// Spark UI -> Executors -> Memory
```

### 4.3 解决问题

```scala
// ==================== 优化1：Spark配置 ====================
// 内存优化
spark.executor.memory = 8g
spark.driver.memory = 4g
spark.memory.fraction = 0.6
spark.memory.storageFraction = 0.5

// Shuffle优化
spark.sql.shuffle.partitions = 200  // 根据数据量调整
spark.shuffle.file.buffer = 1mb
spark.reducer.maxSizeInFlight = 256mb
spark.shuffle.service.enabled = true

// 并行度
spark.default.parallelism = 100

// 序列化
spark.serializer = org.apache.spark.serializer.KryoSerializer
spark.kryo.registrationRequired = false


// ==================== 优化2：代码优化 ====================
// 优化前：多次Shuffle
val result = df1.join(df2, "key")
  .join(df3, "key")
  .groupBy("key")
  .agg(sum("value"))

// 优化后：减少Shuffle
val result = df1.join(df2, "key")
  .groupBy("key")
  .agg(sum("value"))

// 优化前：笛卡尔积
df1.crossJoin(df2)

// 优化后：避免笛卡尔积
df1.join(df2, "key")


// ==================== 优化3：数据倾斜 ====================
// 加盐打散
val dfWithSalt = df.withColumn("salt", (rand * 10).cast(IntegerType))
val result = dfWithSalt
  .groupBy($"key" + $"salt")
  .agg(sum("value") as "value")
  .groupBy("key")
  .agg(sum("value") as "value")


// ==================== 优化4：Broadcast ====================
// 小表广播
val smallDf = largeDf.filter("key IN (1,2,3)")
val result = largeDf.join(broadcast(smallDf), "key")


// ==================== 优化5：缓存优化 ====================
// 缓存常用数据
df.persist(StorageLevel.MEMORY_AND_DISK)

// 及时释放
df.unpersist()


// ==================== 优化6：代码层面 ====================
// 避免NULL参与计算
df.filter("key IS NOT NULL")
  .groupBy("key")
  .agg(sum("value"))

// 使用强类型
import org.apache.spark.sql.functions._
df.select($"id", $"name", $"value" * 1.0)
```

### 4.4 复盘反思

```scala
// 创建优化记录
case class SparkOptimization(
    problem: String,
    solution: String,
    effect: String,
    author: String,
    timestamp: java.sql.Timestamp
)

val optimizations = Seq(
    SparkOptimization(
        "数据倾斜导致Stage卡死",
        "加盐打散+两阶段聚合",
        "30分钟 -> 2分钟",
        "数据组",
        new java.sql.Timestamp(System.currentTimeMillis())
    )
)
```

---

## 5. Flink SQL性能调优

### 5.1 发现问题

**问题信号**：
- Checkpoint时间 > 5分钟
- 背压持续
- 状态膨胀
- 延迟增加

### 5.2 定位问题

```sql
-- 1. 查看Flink UI
-- http://jobmanager:8081

-- 2. 查看背压
-- TaskManagers -> Task -> Back Pressure

-- 3. 查看Checkpoin详情
-- Checkpoints -> Latest Checkpoint -> Summary

-- 4. 查看状态大小
SELECT 
    job_name,
    state_size,
    last_checkpoint_size
FROM flink_job_table
WHERE job_name = 'your_job';
```

### 5.3 解决问题

```sql
-- ==================== 优化1：Checkpoin优化 ====================
-- 配置
INSERT INTO flink_env VALUES (
    'execution.checkpointing.interval', '5 min',
    'execution.checkpointing.mode', 'EXACTLY_ONCE',
    'execution.checkpointing.timeout', '10 min',
    'execution.checkpointing.min-pause', '1 min',
    'execution.checkpointing.max-concurrent-checkpoints', '1',
    'state.backend', 'rocksdb',
    'state.checkpoints.dir', 'hdfs:///flink/checkpoints',
    'state.savepoints.dir', 'hdfs:///flink/savepoints'
);

-- ==================== 优化2：状态优化 ====================
-- 状态TTL
INSERT INTO flink_env VALUES (
    'state.ttl', '2 d',
    'state.cleanup.min-pause', '1 h'
);

-- ==================== 优化3：背压处理 ====================
-- 调整缓冲区
INSERT INTO flink_env VALUES (
    'taskmanager.network.memory.fraction', '0.15',
    'taskmanager.network.memory.min', '256mb',
    'taskmanager.network.memory.max', '1gb',
    'taskmanager.network.buffer-per-channel', '2mb',
    'taskmanager.network.exclusive-buffers-per-slot', '4'
);

-- ==================== 优化4：并行度 ====================
-- 设置并行度
SELECT SET('parallelism.default', '8');

-- ==================== 优化5：SQL改写 ====================
-- 优化前：复杂JOIN
INSERT INTO sink
SELECT a.*, b.*
FROM table_a a
JOIN table_b b ON a.id = b.id
JOIN table_c c ON b.key = c.key;

-- 优化后：拆分+先聚合
INSERT INTO sink
SELECT a.*, b.*
FROM (
    SELECT * FROM table_a
) a
JOIN (
    SELECT b.*, c.value as c_value
    FROM table_b b
    JOIN table_c c ON b.key = c.key
) b ON a.id = b.id;


-- ==================== 优化6：维表Join ====================
-- 使用Async I/O
SELECT /*+ ASYNC('cache', '1000', '60000') */ 
    o.*, u.name
FROM orders o
JOIN users FOR SYSTEM_TIME AS OF o.proctime u
ON o.user_id = u.id;
```

### 5.4 复盘反思

```sql
-- 创建Flink优化记录
CREATE TABLE flink_optimization (
    opt_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    job_name STRING,
    problem_type STRING,      -- CHECKPOINT/BACKPRESSURE/STATE/PERFORMANCE
    symptom STRING,
    root_cause STRING,
    solution STRING,
    effect STRING,
    author STRING,
    create_time TIMESTAMP
);

-- 记录
INSERT INTO flink_optimization (job_name, problem_type, symptom, root_cause, solution, effect, author, create_time)
VALUES 
(
    'realtime_order',
    'CHECKPOINT',
    'Checkpoint超时',
    '状态过大',
    '开启增量Checkpoint+设置状态TTL',
    '10分钟->30秒',
    '实时组',
    CURRENT_TIMESTAMP()
);
```

---

## 6. ETL开发

### 6.1 发现问题

**问题信号**：
- 任务执行失败
- 数据不符合预期
- 产出延迟

### 6.2 定位问题

```sql
-- 1. 检查任务状态
SELECT * FROM task_instance 
WHERE task_name = 'dwd_order' 
  AND dt = '${dt}'
  AND run_time > '2024-01-01';

-- 2. 检查依赖任务
SELECT 
    a.task_name,
    a.status,
    a.start_time,
    a.end_time,
    b.task_name as depends_on
FROM task_instance a
JOIN task_dependency b ON a.task_id = b.task_id
WHERE a.dt = '${dt}';

-- 3. 检查数据完整性
SELECT 
    '源表' as layer,
    COUNT(*) as cnt
FROM ods_order WHERE dt = '${dt}'
UNION ALL
SELECT 
    '明细层' as layer,
    COUNT(*) as cnt
FROM dwd_order WHERE dt = '${dt}'
UNION ALL
SELECT 
    '汇总层' as layer,
    COUNT(*) as cnt
FROM dws_order_1d WHERE dt = '${dt}';
```

### 6.3 解决问题

```sql
-- 1. 失败重跑机制
-- 方案1：重新执行
INSERT OVERWRITE TABLE dwd_order PARTITION(dt='${dt}')
SELECT * FROM ods_order WHERE dt = '${dt}';

-- 方案2：修复后重新执行
-- 先修复问题数据
INSERT OVERWRITE TABLE ods_order_fix PARTITION(dt='${dt}')
SELECT 
    CASE 
        WHEN order_id = 'error_id' THEN 'fixed_value'
        ELSE order_id
    END as order_id,
    *
FROM ods_order 
WHERE dt = '${dt}';

-- 2. 数据修复
-- 修复金额异常
UPDATE dwd_order 
SET order_amount = 0 
WHERE dt = '${dt}' 
  AND order_amount < 0;

-- 3. 幂等处理
-- 每次全量覆盖
INSERT OVERWRITE TABLE dwd_order PARTITION(dt='${dt}')
SELECT * FROM ods_order WHERE dt = '${dt}';
```

### 6.4 复盘反思

```sql
-- 创建ETL问题记录
CREATE TABLE etl_issue (
    issue_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    task_name STRING,
    issue_time TIMESTAMP,
    issue_type STRING,       -- FAIL/DELAY/ERROR
    error_message STRING,
    root_cause STRING,
    fix_solution STRING,
    prevent_solution STRING,  -- 预防措施
    owner STRING,
    resolved_time TIMESTAMP
);

-- 记录问题
INSERT INTO etl_issue (task_name, issue_time, issue_type, error_message, root_cause, fix_solution, prevent_solution, owner, resolved_time)
VALUES 
(
    'dwd_order',
    '2024-01-01 08:00:00',
    'FAIL',
    'Java heap space',
    '数据量突增，内存不足',
    '增加executor内存，调整分区',
    '增加监控，提前预警',
    '数据组',
    '2024-01-01 10:00:00'
);
```

---

## 7. 数据质量

### 7.1 发现问题

**监控规则**：
- 完整性：null比例 > 5%
- 准确性：值域超范围
- 一致性：跨表不一致
- 及时性：延迟 > 阈值

### 7.2 定位问题

```sql
-- 1. 完整性检查
SELECT 
    column_name,
    COUNT(*) as total,
    SUM(CASE WHEN column IS NULL THEN 1 ELSE 0 END) as null_cnt,
    SUM(CASE WHEN column IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as null_pct
FROM dwd_order
WHERE dt = '${dt}'
GROUP BY column_name
HAVING SUM(CASE WHEN column IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > 5;

-- 2. 准确性检查
SELECT 
    column_name,
    COUNT(*) as invalid_cnt
FROM dwd_order
WHERE dt = '${dt}'
  AND (
    (amount < 0) OR
    (status NOT IN ('PENDING', 'PAID', 'SHIPPED', 'CONFIRMED', 'CANCELLED'))
)
GROUP BY column_name;

-- 3. 一致性检查
SELECT 
    'dwd' as layer,
    COUNT(*) as cnt
FROM dwd_order
WHERE dt = '${dt}'
UNION ALL
SELECT 
    'ads' as layer,
    COUNT(*) as cnt
FROM ads_order_report
WHERE dt = '${dt}';
```

### 7.3 解决问题

```sql
-- 1. 自动告警
INSERT INTO alert_log (task_name, check_type, check_value, threshold, alert_time)
SELECT 
    'dwd_order',
    'NULL_CHECK',
    null_pct,
    5,
    CURRENT_TIMESTAMP()
FROM (
    SELECT 
        column_name,
        SUM(CASE WHEN column IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as null_pct
    FROM dwd_order
    WHERE dt = '${dt}'
    GROUP BY column_name
) t
WHERE t.null_pct > 5;

-- 2. 数据修复
UPDATE dwd_order 
SET phone = 'UNKNOWN' 
WHERE dt = '${dt}' 
  AND phone IS NULL;

-- 3. 质量报告
INSERT INTO quality_report (dt, table_name, total_cnt, pass_cnt, pass_rate, alert_cnt)
SELECT 
    '${dt}',
    'dwd_order',
    COUNT(*) as total_cnt,
    SUM(CASE WHEN is_valid = TRUE THEN 1 ELSE 0 END) as pass_cnt,
    SUM(CASE WHEN is_valid = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as pass_rate,
    SUM(CASE WHEN is_valid = FALSE THEN 1 ELSE 0 END) as alert_cnt
FROM (
    SELECT 
        *,
        CASE 
            WHEN order_id IS NOT NULL 
             AND amount >= 0 
             AND status IN ('PENDING', 'PAID', 'SHIPPED', 'CONFIRMED', 'CANCELLED')
            THEN TRUE
            ELSE FALSE
        END as is_valid
    FROM dwd_order
    WHERE dt = '${dt}'
) t;
```

### 7.4 复盘反思

```sql
-- 创建质量规则库
CREATE TABLE quality_rule (
    rule_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    table_name STRING,
    column_name STRING,
    rule_type STRING,         -- NULL/RANGE/ENUM/CUSTOM
    rule_expr STRING,
    threshold STRING,
    severity STRING,          -- P0/P1/P2
    alert_channel STRING,
    owner STRING,
    create_time TIMESTAMP
);

-- 添加规则
INSERT INTO quality_rule (table_name, column_name, rule_type, rule_expr, threshold, severity, alert_channel, owner, create_time)
VALUES 
(
    'dwd_order',
    'order_id',
    'NULL',
    'order_id IS NOT NULL',
    '0',
    'P0',
    'DINGTALK',
    '数据组',
    CURRENT_TIMESTAMP()
);
```

---

## 8. 指标体系

### 8.1 发现问题

**问题信号**：
- 指标口径不一致
- 业务方质疑数据
- 指标重复定义

### 8.2 定位问题

```sql
-- 1. 检查同名指标
SELECT 
    indicator_name,
    COUNT(DISTINCT indicator_code) as definitions,
    GROUP_CONCAT(indicator_code) as codes
FROM dwd_dim_indicator
GROUP BY indicator_name
HAVING COUNT(DISTINCT indicator_code) > 1;

-- 2. 检查指标计算逻辑
SELECT 
    i.indicator_code,
    i.calculation_method,
    i.filter_conditions,
    i.owner_name
FROM dwd_dim_indicator i
WHERE i.indicator_name = 'GMV';

-- 3. 对比不同实现
SELECT 
    code,
    value,
    calc_method
FROM (
    SELECT 'code_1' as code, value FROM indicator_v1
    UNION ALL
    SELECT 'code_2' as code, value FROM indicator_v2
) t
PIVOT (MAX(value) FOR code IN ('code_1', 'code_2'));
```

### 8.3 解决问题

```sql
-- 1. 统一指标口径
UPDATE dwd_dim_indicator 
SET calculation_method = 'SUM(CASE WHEN status = ''PAID'' THEN amount ELSE 0 END)',
    filter_conditions = "status IN ('PAID')"
WHERE indicator_code = 'gmv';

-- 2. 废弃重复指标
UPDATE dwd_dim_indicator 
SET status = 'DEPRECATED',
    deprecated_by = 'gmv_standard'
WHERE indicator_code IN ('gmv_old', 'gmv_v1', 'gmv_v2');

-- 3. 新增标准指标
INSERT INTO dwd_dim_indicator (indicator_code, indicator_name, indicator_type, calculation_method, filter_conditions, owner_name, create_time)
VALUES 
(
    'gmv_standard',
    'GMV(标准版)',
    'DERIVED',
    'SUM(CASE WHEN order_status = ''PAID'' THEN order_amount ELSE 0 END)',
    "order_status = 'PAID' AND dt = CURRENT_DATE",
    '数据组',
    CURRENT_TIMESTAMP()
);
```

### 8.4 复盘反思

```sql
-- 创建指标变更历史
CREATE TABLE indicator_change_log (
    change_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    indicator_code STRING,
    change_type STRING,       -- CREATE/UPDATE/DEPRECATE
    before_value STRING,
    after_value STRING,
    change_reason STRING,
    approved_by STRING,
    change_time TIMESTAMP
);

-- 记录变更
INSERT INTO indicator_change_log (indicator_code, change_type, before_value, after_value, change_reason, approved_by, change_time)
VALUES 
(
    'gmv',
    'UPDATE',
    'SUM(order_amount)',
    'SUM(CASE WHEN status = ''PAID'' THEN amount ELSE 0 END)',
    '原口径包含未支付订单',
    '数据组负责人',
    CURRENT_TIMESTAMP()
);
```

---

## 9. 常见问题速查表

### 9.1 性能问题

| 问题 | 现象 | 原因 | 解决方案 |
|-----|------|------|----------|
| Shuffle慢 | Stage卡住 | 并行度低 | 增加partitions |
| 数据倾斜 | 部分Task慢 | Key分布不均 | 加盐打散 |
| OOM | 任务失败 | 内存不足 | 增加executor/减少并行度 |
| 小文件多 | IO高 | 分区小 | 合并小文件 |

### 9.2 数据问题

| 问题 | 现象 | 原因 | 解决方案 |
|-----|------|------|----------|
| 数据延迟 | 产出晚 | 任务慢 | 优化SQL/增加资源 |
| 数据不准 | 业务投诉 | 口径不一致 | 统一指标定义 |
| 数据缺失 | null多 | 源数据问题 | 修复源数据 |

### 9.3 任务问题

| 问题 | 现象 | 原因 | 解决方案 |
|-----|------|------|----------|
| 任务失败 | 红灯 | 代码/资源问题 | 查看日志修复 |
| 依赖阻塞 | 等待 | 上游未完成 | 检查上游任务 |
| 资源争抢 | 并行慢 | 资源不足 | 调整优先级 |
