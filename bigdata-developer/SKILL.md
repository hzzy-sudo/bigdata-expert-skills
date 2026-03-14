---
name: bigdata-developer
description: 大数据研发专家技能集。用于数仓建设、ETL开发，数据治理、指标体系建设。包括完整的数仓建模能力：需求分析、业务调研、数据源分析、业务总线矩阵设计、数仓架构设计、ODS/DWD/DWS/ADS四层建设，Flink SQL/Hive SQL/Spark SQL性能调优。适用于需要构建企业级数仓，开发ETL任务、进行数据治理，建立指标体系的场景。此skill包含完整的发现问题-定位问题-解决问题-复盘反思闭环。
---

# 大数据研发专家

## 核心能力图谱

```
需求分析 ──→ 业务调研 ──→ 数据源分析 ──→ 总线矩阵 ──→ 数仓架构 ──→ ODS/DWD/DWS/ADS建设
    │              │              │              │              │              │
    ▼              ▼              ▼              ▼              ▼              ▼
  需求访谈      业务流程图      源系统调研     维度设计       分层设计        ETL开发
  功能拆解      业务术语库      数据质量       事实设计       存储设计        任务调度
  指标定义      业务规则       数据地图       关系设计       治理规范        监控运维
```

---

## 第一阶段：需求分析

### 1.1 发现问题

**问题信号**：
- 业务方提出新需求
- 报表数据不满足
- 现有数仓无法支持

### 1.2 定位问题

```sql
-- 1. 收集需求
-- 访谈对象：业务负责人、产品经理、运营、数据分析师
-- 访谈内容：
-- [?] 业务目标是什么？
-- [?] 需要哪些数据？
-- [?] 数据更新频率要求？
-- [?] 数据精确度要求？
-- [?] 报表展示形式？

-- 2. 需求分类
SELECT 
    requirement_name,
    requirement_type,  -- REPORT/ANALYSIS/DW/API
    priority,          -- P0/P1/P2
    requestor,
    request_time
FROM dwd_requirement
WHERE status = 'PENDING';

-- 3. 需求评审
-- 评审维度：
-- 业务价值
-- 技术可行性
-- 数据可行性
-- 资源投入
```

### 1.3 解决问题

```sql
-- 创建需求表
CREATE TABLE dwd_requirement (
    req_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    req_code STRING,              -- REQ_2024_001
    req_name STRING,
    req_type STRING,            -- REPORT/ANALYSIS/DW/API
    business_domain STRING,
    priority STRING,            -- P0/P0_1/P1/P2
    description STRING,
    requestor STRING,
    owner STRING,
    status STRING,             -- PENDING/APPROVED/DEVELOPING/DONE
    target_date DATE,
    create_time TIMESTAMP,
    update_time TIMESTAMP
);

-- 创建需求评审表
CREATE TABLE dwd_requirement_review (
    review_id BIGINT PRIMARY KEY,
    req_id BIGINT,
    reviewer STRING,
    review_result STRING,      -- APPROVED/REJECTED/MODIFY
    comments STRING,
    review_time TIMESTAMP
);

-- 插入需求
INSERT INTO dwd_requirement (req_code, req_name, req_type, business_domain, priority, description, requestor, owner, status, target_date, create_time)
VALUES 
(
    'REQ_2024_001',
    '销售日报',
    'REPORT',
    '交易域',
    'P0',
    '每日销售数据报表，包含销售额、订单数、转化率等',
    '销售部-张三',
    '数据组-李四',
    'PENDING',
    '2024-02-01',
    CURRENT_TIMESTAMP()
);
```

### 1.4 复盘反思

```sql
-- 创建需求分析记录
CREATE TABLE req_analysis_log (
    log_id BIGINT PRIMARY KEY,
    req_id BIGINT,
    analysis_type STRING,       -- INTERVIEW/ANALYSIS/REVIEW
    content STRING,
    findings STRING,           -- 发现的点
    action_items STRING,       -- 行动项
    analyst STRING,
    analyze_time TIMESTAMP
);
```

---

## 第二阶段：业务调研

### 2.1 发现问题

**问题信号**：
- 不理解业务术语
- 不知道业务流程
- 数据边界不清晰

### 2.2 定位问题

```sql
-- 1. 绘制业务流程图
-- 方法：
-- 1) 访谈业务人员
-- 2) 观察业务流程
-- 3) 查看业务文档
-- 4) 分析业务日志

-- 2. 梳理业务实体
-- 实体关系：
-- 客户 -> 订单 -> 商品 -> 支付 -> 物流

-- 3. 确定业务边界
-- 业务范围：线上交易
-- 不包括：线下交易、采购

-- 4. 识别业务规则
-- 订单状态流转：创建 -> 支付 -> 发货 -> 确认
-- 优惠计算规则：...
-- 退货规则：...
```

### 2.3 解决问题

```sql
-- 创建业务流程表
CREATE TABLE dwd_business_process (
    process_id BIGINT PRIMARY KEY,
    process_code STRING,
    process_name STRING,
    domain STRING,
    start_event STRING,        -- 开始事件
    end_event STRING,         -- 结束事件
    description STRING,
    owner STRING,
    create_time TIMESTAMP
);

-- 创建业务实体表
CREATE TABLE dwd_business_entity (
    entity_id BIGINT PRIMARY KEY,
    entity_code STRING,
    entity_name STRING,
    entity_type STRING,        -- CORE/AUXILIARY
    domain STRING,
    attributes STRING,         -- JSON
    relationships STRING,     -- JSON: 关联实体
    owner STRING,
    create_time TIMESTAMP
);

-- 创建业务规则表
CREATE TABLE dwd_business_rule (
    rule_id BIGINT PRIMARY KEY,
    rule_code STRING,
    rule_name STRING,
    rule_type STRING,         -- CALC/FILTER/TRANSFORM
    domain STRING,
    expression STRING,        -- 规则表达式
    description STRING,
    examples STRING,
    owner STRING,
    create_time TIMESTAMP
);

-- 插入业务流程
INSERT INTO dwd_business_process (process_code, process_name, domain, start_event, end_event, description, owner)
VALUES 
('PROC_ORDER', '订单处理流程', '交易域', '创建订单', '完成订单', '客户下单到完成订单的全流程', '销售部');
```

### 2.4 复盘反思

```sql
-- 创建业务调研记录
CREATE TABLE business_research_log (
    log_id BIGINT PRIMARY KEY,
    research_type STRING,      -- INTERVIEW/DOC/OBSERVATION
    research_object STRING,   -- 被调研对象
    key_findings STRING,      -- 关键发现
    questions STRING,         -- 待确认问题
    next_steps STRING,        -- 下一步
    researcher STRING,
    research_time TIMESTAMP
);
```

---

## 第三阶段：数据源分析

### 3.1 发现问题

**问题信号**：
- 源系统数据结构不清
- 数据质量未知
- 数据口径不一致

### 3.2 定位问题

```sql
-- 1. 源系统盘点
-- 列出所有数据源
SELECT 
    source_system,
    source_type,             -- RDBMS/LOG/API/FILE
    data_volume,
    update_frequency,
    data_owner,
    contact_person
FROM dwd_data_source;

-- 2. 数据质量评估
-- 完整性
SELECT 
    '订单表' as table_name,
    'order_id' as column_name,
    COUNT(*) as total,
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) as null_count,
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as null_pct
FROM source_order;

-- 准确性
SELECT 
    '订单表' as table_name,
    'amount' as column_name,
    COUNT(*) as total,
    SUM(CASE WHEN amount < 0 THEN 1 ELSE 0 END) as invalid_count
FROM source_order;

-- 一致性
SELECT 
    'order_status' as column_name,
    DISTINCT_COUNT(status) as distinct_values,
    GROUP_CONCAT(status) as values
FROM source_order
GROUP BY 'order_status';

-- 3. 数据量评估
SELECT 
    table_name,
    ROW_COUNT as row_count,
    DATA_SIZE as data_size,
    LAST_UPDATE as last_update
FROM information_schema.tables
WHERE table_schema = 'source';
```

### 3.3 解决问题

```sql
-- 创建数据源表
CREATE TABLE dwd_data_source (
    source_id BIGINT PRIMARY KEY,
    source_code STRING,
    source_name STRING,
    source_type STRING,        -- MYSQL/ORACLE/POSTGRES/KAFKA/API/FILE
    host STRING,
    port INT,
    database_name STRING,
    owner STRING,
    contact STRING,
    description STRING,
    create_time TIMESTAMP
);

-- 创建源表清单
CREATE TABLE dwd_source_table (
    table_id BIGINT PRIMARY KEY,
    source_id BIGINT,
    table_name STRING,
    table_type STRING,        -- DIM/FACT/AUX
    row_count BIGINT,
    data_size STRING,
    update_frequency STRING,  -- REAL/TIMELY/DAILY/WEEKLY
    primary_key STRING,
    description STRING,
    create_time TIMESTAMP
);

-- 创建源字段清单
CREATE TABLE dwd_source_column (
    column_id BIGINT PRIMARY KEY,
    table_id BIGINT,
    column_name STRING,
    data_type STRING,
    is_primary_key BOOLEAN,
    is_nullable BOOLEAN,
    default_value STRING,
    description STRING,
    sample_values STRING,
    create_time TIMESTAMP
);

-- 创建数据质量报告
CREATE TABLE dwd_data_quality_report (
    report_id BIGINT PRIMARY KEY,
    source_id BIGINT,
    table_name STRING,
    column_name STRING,
    quality_type STRING,       -- COMPLETE/ACCURATE/CONSISTENT/TIMELY
    quality_score DECIMAL(5,2),
    issues STRING,            -- 问题描述
    suggestions STRING,      -- 改进建议
    report_time TIMESTAMP
);
```

### 3.4 复盘反思

```sql
-- 创建数据源分析总结
CREATE TABLE source_analysis_summary (
    summary_id BIGINT PRIMARY KEY,
    source_system STRING,
    summary_type STRING,      -- OVERVIEW/QUALITY/MODEL
    key_findings STRING,     -- 关键发现
    risks STRING,           -- 风险点
    recommendations STRING,  -- 建议
    analyst STRING,
    analysis_time TIMESTAMP
);
```

---

## 第四阶段：业务总线矩阵设计

### 4.1 发现问题

**问题信号**：
- 维度定义不统一
- 事实关系不清晰
- 指标口径冲突

### 4.2 定位问题

```sql
-- 1. 识别业务过程
-- 业务过程：用户注册、浏览商品、加入购物车、下单、支付、发货、确认收货

-- 2. 识别维度
-- 维度列表：
-- 时间维度、用户维度、商品维度、店铺维度、地区维度、渠道维度

-- 3. 识别度量
-- 度量列表：
-- 订单数、订单金额、商品数量、支付金额

-- 4. 设计总线矩阵
SELECT 
    business_process,
    dimension_name,
    fact_name,
    relationship_type   -- ONE_TO_ONE/ONE_TO_MANY/MANY_TO_MANY
FROM dwd_bus_matrix;
```

### 4.3 解决问题

```sql
-- 创建总线矩阵表
CREATE TABLE dwd_bus_matrix (
    matrix_id BIGINT PRIMARY KEY,
    business_process STRING,   -- 业务过程
    process_description STRING,
    subject_area STRING,      -- 主题域
    
    -- 维度
    dim_time STRING,         -- 时间维度
    dim_user STRING,        -- 用户维度
    dim_product STRING,     -- 商品维度
    dim_store STRING,        -- 店铺维度
    dim_region STRING,      -- 地区维度
    dim_channel STRING,      -- 渠道维度
    
    -- 事实
    fact_order_cnt STRING,   -- 订单数
    fact_order_amt STRING,   -- 订单金额
    fact_pay_amt STRING,    -- 支付金额
    fact_quantity STRING,   -- 商品数量
    
    create_time TIMESTAMP
);

-- 插入总线矩阵
INSERT INTO dwd_bus_matrix (business_process, process_description, subject_area, dim_time, dim_user, dim_product, dim_store, fact_order_cnt, fact_order_amt)
VALUES 
('下单', '客户提交订单', '交易域', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y'),
('支付', '客户完成支付', '交易域', 'Y', 'Y', 'Y', 'Y', 'N', 'Y'),
('发货', '商家发货', '交易域', 'Y', 'Y', 'Y', 'Y', 'N', 'Y');
```

### 4.4 复盘反思

```sql
-- 创建矩阵评审记录
CREATE TABLE bus_matrix_review (
    review_id BIGINT PRIMARY KEY,
    matrix_id BIGINT,
    reviewer STRING,
    review_type STRING,      -- INITIAL/UPDATE
    comments STRING,
    approvals STRING,
    review_time TIMESTAMP
);
```

---

## 第五阶段：数仓架构设计

### 5.1 发现问题

**问题信号**：
- 分层不清晰
- 主题不明确
- 存储混乱

### 5.2 定位问题

```sql
-- 1. 设计分层架构
-- ODS -> DWD -> DWS -> ADS

-- 2. 设计主题域
-- 交易域、用户域、商品域、流量域、财务域

-- 3. 设计数据流向
SELECT 
    source_table,
    target_table,
    layer,
    data_flow_type,          -- ETL/CDC/STREAM
    frequency,
    description
FROM dwd_data_flow;

-- 4. 评估数据量
SELECT 
    table_name,
    layer,
    daily增量,
    存储量,
    retention
FROM dwd_storage_estimate;
```

### 5.3 解决问题

```sql
-- 创建数仓架构表
CREATE TABLE dwd_dw_architecture (
    arch_id BIGINT PRIMARY KEY,
    layer STRING,             -- ODS/DWD/DWS/ADS
    layer_description STRING,
    table_prefix STRING,     -- ods_/dwd_/dws_/ads_
    storage_type STRING,      -- PARQUET/ORC/KUDU
    retention_days INT,
    partition_type STRING,   -- DAY/HOUR
    compression STRING,     -- SNAPPY/ZSTD
    description STRING,
    owner STRING,
    create_time TIMESTAMP
);

-- 创建主题域表
CREATE TABLE dwd_subject_area (
    area_id BIGINT PRIMARY KEY,
    area_code STRING,        -- TRADE/USER/PRODUCT/FINANCE
    area_name STRING,
    description STRING,
    tables STRING,           -- JSON: 包含的表
    owner STRING,
    create_time TIMESTAMP
);

-- 插入主题域
INSERT INTO dwd_subject_area (area_code, area_name, description, tables, owner)
VALUES 
('TRADE', '交易域', '订单、支付、售后等交易相关', '["fact_order","fact_payment","dim_order"]', '交易组'),
('USER', '用户域', '用户、会员等', '["dim_user","dim_user_profile"]', '用户组'),
('PRODUCT', '商品域', '商品、类目、品牌', '["dim_product","dim_category"]', '商品组'),
('FLOW', '流量域', 'PV、UV、点击', '["fact_page_view","fact_click"]', '流量组'),
('FINANCE', '财务域', '收入、成本、利润', '["fact_revenue","fact_cost"]', '财务组');
```

### 5.4 复盘反思

```sql
-- 创建架构评审记录
CREATE TABLE dw_architecture_review (
    review_id BIGINT PRIMARY KEY,
    review_type STRING,      -- INITIAL/UPDATE/OPTIMIZE
    changes STRING,         -- 变更内容
    reason STRING,         -- 变更原因
    impacts STRING,        -- 影响范围
    approver STRING,
    review_time TIMESTAMP
);
```

---

## 第六阶段：ODS层建设

### 6.1 发现问题

**问题信号**：
- 源数据接入失败
- 数据延迟
- 数据格式异常

### 6.2 定位问题

```sql
-- 1. 检查数据同步状态
SELECT 
    task_name,
    source_table,
    target_table,
    status,                -- SUCCESS/FAILED/RUNNING
    start_time,
    end_time,
    rows_synced,
    error_message
FROM dwd_etl_task_log
WHERE dt = '${dt}'
  AND layer = 'ODS';

-- 2. 检查数据量变化
SELECT 
    source_table,
    target_table,
    dt,
    source_count,
    target_count,
    diff_count,
    diff_pct
FROM ods_count_comparison
WHERE dt = '${dt}';
```

### 6.3 解决问题

```sql
-- ODS层建表模板
-- 1. 保持原貌，不做清洗
-- 2. 增加审计字段
-- 3. 分区存储

CREATE TABLE ods_order (
    -- 原始字段（保持类型一致）
    order_id STRING,
    user_id STRING,
    product_id STRING,
    amount STRING,
    status STRING,
    create_time STRING,
    raw_data STRING,         -- 原始JSON
    
    -- 审计字段
    etl_time TIMESTAMP,
    etl_batch STRING,
    source_system STRING,
    source_table STRING,
    dt STRING
) COMMENT '订单原始数据表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compression'='SNAPPY'
);

-- ODS层同步任务
INSERT OVERWRITE TABLE ods_order PARTITION(dt='${dt}')
SELECT 
    order_id,
    user_id,
    product_id,
    amount,
    status,
    create_time,
    raw_data,
    CURRENT_TIMESTAMP() as etl_time,
    '${batch_id}' as etl_batch,
    'POS' as source_system,
    'pos_order' as source_table,
    '${dt}' as dt
FROM pos_order
WHERE dt = '${dt}';
```

### 6.4 复盘反思

```sql
-- 创建ODS层问题记录
CREATE TABLE ods_issue_log (
    issue_id BIGINT PRIMARY KEY,
    source_table STRING,
    issue_type STRING,       -- SYNC/DELAY/QUALITY
    symptom STRING,
    root_cause STRING,
    solution STRING,
    prevention STRING,
    find_time TIMESTAMP,
    resolve_time TIMESTAMP
);
```

---

## 第七阶段：DWD层建设

### 7.1 发现问题

**问题信号**：
- 数据清洗不完整
- 格式不统一
- 脱敏不规范

### 7.2 定位问题

```sql
-- 1. 检查数据质量
SELECT 
    'null_check' as check_type,
    column_name,
    null_count,
    null_pct
FROM (
    SELECT 
        'order_id' as column_name,
        COUNT(*) as total,
        SUM(CASE WHEN order_id IS NULL OR order_id = '' THEN 1 ELSE 0 END) as null_count
    FROM dwd_order WHERE dt = '${dt}'
    UNION ALL
    SELECT 
        'amount' as column_name,
        COUNT(*) as total,
        SUM(CASE WHEN amount IS NULL OR amount = '' THEN 1 ELSE 0 END) as null_count
    FROM dwd_order WHERE dt = '${dt}'
) t
WHERE null_count > 0;

-- 2. 检查值域
SELECT 
    status,
    COUNT(*) as cnt
FROM dwd_order
WHERE dt = '${dt}'
GROUP BY status
HAVING status NOT IN ('CREATED', 'PAID', 'SHIPPED', 'CONFIRMED', 'CANCELLED');

-- 3. 检查主键唯一性
SELECT 
    order_id,
    COUNT(*) as cnt
FROM dwd_order
WHERE dt = '${dt}'
GROUP BY order_id
HAVING COUNT(*) > 1;
```

### 7.3 解决问题

```sql
-- DWD层建表模板
-- 1. 数据清洗：去噪、去重、格式转换
-- 2. 脱敏处理：敏感字段加密
-- 3. 规范化：统一编码、口径

CREATE TABLE dwd_order (
    -- 主键
    order_id BIGINT,
    
    -- 外键（关联维度）
    user_key INT,
    product_key INT,
    store_key INT,
    date_key INT,
    
    -- 度量值
    order_amount DECIMAL(15,2),
    pay_amount DECIMAL(15,2),
    discount_amount DECIMAL(15,2),
    freight_amount DECIMAL(15,2),
    quantity INT,
    
    -- 退化维度
    order_status STRING,
    payment_method STRING,
    shipping_method STRING,
    
    -- 时间
    order_time TIMESTAMP,
    pay_time TIMESTAMP,
    shipping_time TIMESTAMP,
    confirm_time TIMESTAMP,
    
    -- ETL审计
    etl_time TIMESTAMP,
    source_system STRING,
    dt STRING
) COMMENT '订单明细表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

-- DWD层清洗任务
INSERT OVERWRITE TABLE dwd_order PARTITION(dt='${dt}')
SELECT 
    -- 主键
    COALESCE(CAST(order_id AS BIGINT), 0) AS order_id,
    
    -- 外键
    COALESCE(CAST(user_id AS INT), -1) AS user_key,
    COALESCE(CAST(product_id AS INT), -1) AS product_key,
    COALESCE(CAST(store_id AS INT), -1) AS store_key,
    CAST(DATE_FORMAT(order_time, 'yyyyMMdd') AS INT) AS date_key,
    
    -- 度量值清洗
    COALESCE(TRY_CAST(amount AS DECIMAL(15,2)), 0) AS order_amount,
    COALESCE(TRY_CAST(pay_amount AS DECIMAL(15,2)), 0) AS pay_amount,
    COALESCE(TRY_CAST(discount_amount AS DECIMAL(15,2)), 0) AS discount_amount,
    COALESCE(TRY_CAST(freight_amount AS DECIMAL(15,2)), 0) AS freight_amount,
    COALESCE(CAST(quantity AS INT), 0) AS quantity,
    
    -- 状态标准化
    CASE 
        WHEN status IN ('CREATED', 'CREATE', 'c') THEN 'CREATED'
        WHEN status IN ('PAID', 'PAY', 'p') THEN 'PAID'
        WHEN status IN ('SHIPPED', 'SHIP', 's') THEN 'SHIPPED'
        WHEN status IN ('CONFIRMED', 'CONFIRM', 'c') THEN 'CONFIRMED'
        WHEN status IN ('CANCELLED', 'CANCEL', 'x') THEN 'CANCELLED'
        ELSE 'UNKNOWN'
    END AS order_status,
    
    -- 时间清洗
    COALESCE(
        TO_TIMESTAMP(create_time, 'yyyy-MM-dd HH:mm:ss'),
        TO_TIMESTAMP(CONCAT(dt, ' 00:00:00'))
    ) AS order_time,
    
    -- 脱敏
    CASE 
        WHEN phone IS NOT NULL AND LENGTH(phone) = 11 
        THEN CONCAT(LEFT(phone, 3), '****', RIGHT(phone, 4))
        ELSE NULL
    END AS phone,
    
    CURRENT_TIMESTAMP() AS etl_time,
    'POS' AS source_system,
    '${dt}' AS dt
FROM ods_order
WHERE dt = '${dt}'
  AND order_id IS NOT NULL
  AND order_id != '';
```

### 7.4 复盘反思

```sql
-- 创建DWD层问题记录
CREATE TABLE dwd_issue_log (
    issue_id BIGINT PRIMARY KEY,
    table_name STRING,
    issue_type STRING,       -- QUALITY/LOGIC/PERFORMANCE
    column_name STRING,
    issue_desc STRING,
    root_cause STRING,
    solution STRING,
    prevention STRING,
    find_time TIMESTAMP,
    resolve_time TIMESTAMP
);
```

---

## 第八阶段：DWS层建设

### 8.1 发现问题

**问题信号**：
- 汇总粒度不对
- 指标计算错误
- 复用性差

### 8.2 定位问题

```sql
-- 1. 检查汇总数据准确性
SELECT 
    dws.order_date,
    dws.order_count,
    dwd.order_count as dwd_count,
    (dws.order_count - dwd.order_count) as diff
FROM dws_order_1d dws
LEFT JOIN (
    SELECT 
        DATE_FORMAT(order_time, 'yyyy-MM-dd') as order_date,
        COUNT(*) as order_count
    FROM dwd_order
    WHERE dt = '${dt}'
      AND order_status != 'CANCELLED'
    GROUP BY DATE_FORMAT(order_time, 'yyyy-MM-dd')
) dwd ON dws.order_date = dwd.order_date
WHERE dws.dt = '${dt}'
  AND dws.order_count != dwd.order_count;

-- 2. 检查指标一致性
SELECT 
    indicator_name,
    dws_value,
    ads_value,
    diff
FROM (
    SELECT 'GMV' as indicator_name, gmv as dws_value FROM dws_order_1d WHERE dt = '${dt}'
) dws
JOIN (
    SELECT 'GMV' as indicator_name, gmv as ads_value FROM ads_order_report WHERE dt = '${dt}'
) ads ON dws.indicator_name = ads.indicator_name
WHERE dws.dws_value != ads.ads_value;
```

### 8.3 解决问题

```sql
-- DWS层建表模板
-- 1. 轻度汇总
-- 2. 主题宽表
-- 3. 维度退化

CREATE TABLE dws_order_1d (
    -- 粒度
    dt STRING,
    
    -- 退化维度
    user_group STRING,        -- 用户群
    product_category STRING, -- 商品类目
    region STRING,           -- 地区
    
    -- 原子指标
    order_count BIGINT,
    order_user_count BIGINT,
    order_amount DECIMAL(15,2),
    pay_count BIGINT,
    pay_amount DECIMAL(15,2),
    pay_user_count BIGINT,
    
    -- 派生指标
    conversion_rate DECIMAL(10,4),  -- 转化率
    avg_order_amount DECIMAL(15,2), -- 客单价
    
    -- 时间
    etl_time TIMESTAMP
) COMMENT '订单汇总日表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

-- DWS层汇总任务
INSERT OVERWRITE TABLE dws_order_1d PARTITION(dt='${dt}')
SELECT 
    '${dt}' as dt,
    
    -- 退化维度
    u.user_group,
    p.category_name as product_category,
    o.region,
    
    -- 原子指标
    COUNT(DISTINCT o.order_id) as order_count,
    COUNT(DISTINCT o.user_key) as order_user_count,
    SUM(o.order_amount) as order_amount,
    COUNT(DISTINCT CASE WHEN o.order_status = 'PAID' THEN o.order_id END) as pay_count,
    SUM(CASE WHEN o.order_status = 'PAID' THEN o.order_amount ELSE 0 END) as pay_amount,
    COUNT(DISTINCT CASE WHEN o.order_status = 'PAID' THEN o.user_key END) as pay_user_count,
    
    -- 派生指标
    ROUND(
        COUNT(DISTINCT CASE WHEN o.order_status = 'PAID' THEN o.user_key END) * 1.0 / 
        NULLIF(COUNT(DISTINCT o.user_key), 0), 4
    ) as conversion_rate,
    ROUND(
        SUM(o.order_amount) / NULLIF(COUNT(DISTINCT o.user_key), 0), 2
    ) as avg_order_amount,
    
    CURRENT_TIMESTAMP() as etl_time
FROM dwd_order o
LEFT JOIN dim_user u ON o.user_key = u.user_key AND u.is_current = TRUE
LEFT JOIN dim_product p ON o.product_key = p.product_key
WHERE o.dt = '${dt}'
  AND o.order_status != 'CANCELLED'
GROUP BY 
    '${dt}',
    u.user_group,
    p.category_name,
    o.region;
```

### 8.4 复盘反思

```sql
-- 创建DWS层问题记录
CREATE TABLE dws_issue_log (
    issue_id BIGINT PRIMARY KEY,
    table_name STRING,
    issue_type STRING,       -- ACCURACY/CONSISTENCY
    issue_desc STRING,
    root_cause STRING,
    solution STRING,
    find_time TIMESTAMP,
    resolve_time TIMESTAMP
);
```

---

## 第九阶段：ADS层建设

### 9.1 发现问题

**问题信号**：
- 报表加载慢
- 数据不一致
- 需求变更频繁

### 9.2 定位问题

```sql
-- 1. 检查报表性能
SELECT 
    report_name,
    load_time_sec,
    query_time_sec,
    render_time_sec
FROM ads_report_performance
WHERE dt = '${dt}'
ORDER BY load_time_sec DESC
LIMIT 10;

-- 2. 检查数据一致性
SELECT 
    report_name,
    indicator_name,
    report_value,
    dws_value,
    diff
FROM ads_dws_comparison
WHERE dt = '${dt}'
  AND diff > 0.01;
```

### 9.3 解决问题

```sql
-- ADS层建表模板
-- 1. 业务报表
-- 2. 专题分析
-- 3. 即席查询

CREATE TABLE ads_sales_report_daily (
    dt STRING,
    
    -- 维度
    region STRING,
    category STRING,
    channel STRING,
    
    -- 指标
    sales_amount DECIMAL(15,2),
    order_count INT,
    uv INT,
    conversion_rate DECIMAL(10,4),
    avg_order_amount DECIMAL(15,2),
    
    -- 时间
    etl_time TIMESTAMP
) COMMENT '销售日报'
STORED AS PARQUET;

-- ADS层报表任务
INSERT OVERWRITE TABLE ads_sales_report_daily PARTITION(dt='${dt}')
SELECT 
    dt,
    region,
    category,
    channel,
    sales_amount,
    order_count,
    uv,
    conversion_rate,
    avg_order_amount,
    CURRENT_TIMESTAMP()
FROM dws_trade_summary
WHERE dt = '${dt}';
```

### 9.4 复盘反思

```sql
-- 创建ADS层问题记录
CREATE TABLE ads_issue_log (
    issue_id BIGINT PRIMARY KEY,
    report_name STRING,
    issue_type STRING,       -- PERFORMANCE/ACCURACY
    issue_desc STRING,
    solution STRING,
    find_time TIMESTAMP,
    resolve_time TIMESTAMP
);
```

---

## 第十阶段：SQL性能调优

### 10.1 发现问题

**问题信号**：
- 执行时间 > 10分钟
- 内存溢出
- 任务卡死

### 10.2 定位问题

```sql
-- 1. 查看执行计划
EXPLAIN EXTENDED SELECT ...

-- 2. 分析数据分布
SELECT key, COUNT(*) as cnt
FROM table
WHERE dt = '${dt}'
GROUP BY key
ORDER BY cnt DESC
LIMIT 20;

-- 3. 检查资源使用
SHOW APPLICATION;
```

### 10.3 解决问题

```sql
-- Hive优化
SET hive.explain.user=TRUE;
SET hive.cbo.enable=TRUE;
SET hive.compute.query.using.stats=TRUE;

-- Spark优化
SET spark.sql.shuffle.partitions=200;
SET spark.sql.adaptive.enabled=TRUE;

-- Flink优化
SET table.exec.resource.default-parallelism=8;
SET table.exec.state.backend=rocksdb;
```

### 10.4 复盘反思

```sql
CREATE TABLE sql_optimization_log (
    log_id BIGINT PRIMARY KEY,
    sql_text STRING,
    problem_type STRING,
    before_cost STRING,
    after_cost STRING,
    optimization_method STRING,
    effect STRING,
    author STRING,
    create_time TIMESTAMP
);
```

---

## 第十一阶段：完整速查表

### 11.1 数仓建设速查

| 阶段 | 关键产出 | 常见问题 | 解决方案 |
|------|----------|----------|----------|
| 需求分析 | 需求清单、优先级 | 需求不清晰 | 访谈+原型确认 |
| 业务调研 | 流程图、规则库 | 术语不懂 | 业务字典+访谈 |
| 数据源分析 | 数据质量报告 | 质量差 | 清洗+监控 |
| 总线矩阵 | 维度+事实 | 关系不清 | ER图+评审 |
| 数仓架构 | 分层设计 | 分层乱 | 标准规范+评审 |
| ODS | 原始数据 | 同步失败 | 重跑+监控 |
| DWD | 清洗数据 | 清洗不完整 | 规则+校验 |
| DWS | 汇总数据 | 汇总错误 | 对数+校验 |
| ADS | 报表数据 | 报表慢 | 优化+预计算 |

### 11.2 SQL调优速查

| 问题 | 现象 | 方案 |
|------|------|------|
| 分区裁剪 | 全表扫描 | WHERE dt='xxx' |
| 小表广播 | 大表JOIN | /*+ BROADCAST(t) */ |
| 数据倾斜 | 部分Task慢 | 加盐打散 |
| Shuffle多 | Stage多 | 减少JOIN |
| 内存溢出 | OOM | 增加内存+减少并行度 |

### 11.3 问题记录速查

| 表名 | 用途 |
|------|------|
| dwd_requirement | 需求管理 |
| dwd_business_process | 业务流程 |
| dwd_data_source | 数据源 |
| dwd_bus_matrix | 总线矩阵 |
| dwd_dw_architecture | 数仓架构 |
| dwd_issue_log | 问题记录 |
| sql_optimization_log | SQL优化 |
