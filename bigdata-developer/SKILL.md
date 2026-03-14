---
name: bigdata-developer
description: 大数据研发专家技能集。用于数仓建设、ETL开发、数据治理、指标体系建设。包括离线数仓、实时数仓、数仓建模、维度设计、ETL优化、数据质量监控。适用于需要构建企业级数仓、开发ETL任务、进行数据治理、建立指标体系的场景。
---

# 大数据研发专家

## 1. 数仓分层架构设计

### 1.1 四层架构详解

```
┌─────────────────────────────────────────────────────────────────┐
│                        数仓四层架构                               │
├─────────────────────────────────────────────────────────────────┤
│  ODS (Operational Data Store)  操作数据层                        │
│  • 原始数据，原样接入                                            │
│  • 不做清洗，保留原貌                                           │
│  • 保留全量历史                                                 │
│  • 与源系统1:1映射                                             │
│  • 分区存储，按接入日期                                          │
├─────────────────────────────────────────────────────────────────┤
│  DWD (Data Warehouse Detail)  明细数据层                        │
│  • 数据清洗：去噪、去重、格式转换                                  │
│  • 脱敏处理：敏感字段加密                                         │
│  • 规范统一：命名、编码、口径                                     │
│  • 脏数据落地：便于问题追溯                                       │
│  • 保留90天明细                                                  │
├─────────────────────────────────────────────────────────────────┤
│  DWS (Data Warehouse Service)  汇总数据层                        │
│  • 主题宽表：按业务主题构建                                       │
│  • 轻度聚合：跨主题关联、轻度汇总                                  │
│  • 维度退化：低频维度退化到表中                                    │
│  • 公共指标：派生指标、复合指标                                    │
│  • 保留1年汇总数据                                               │
├─────────────────────────────────────────────────────────────────┤
│  ADS (Application Data Service)  应用数据层                       │
│  • 业务报表：定制化报表数据                                       │
│  • 专题分析：特定业务场景                                        │
│  • 即席查询：Ad-hoc分析                                          │
│  • 数据服务：API/DB接口                                          │
└─────────────────────────────────────────────────────────────────┘
```

### 1.2 分层命名规范

```sql
-- 数据库命名
CREATE DATABASE ods_ecommerce;      -- ODS层
CREATE DATABASE dwd_trade;          -- DWD层
CREATE DATABASE dws_user;          -- DWS层
CREATE DATABASE ads_report;        -- ADS层

-- 表命名规范
ods_ecommerce.ods_order_20240314    -- ODS: ods_{源系统}_{表名}
dwd_trade.dwd_order_detail          -- DWD: dwd_{业务域}_{主题}
dws_user.dws_user_trade_1d          -- DWS: dws_{业务域}_{主题}_{粒度}
ads_report.ads_daily_sales          -- ADS: ads_{用途}_{主题}
```

### 1.3 数据流向设计

```sql
-- ODS层：保持原貌
CREATE TABLE ods_order (
    order_id STRING,
    user_id STRING,
    amount STRING,        -- 保持string，不转换
    create_time STRING,  -- 保持原始格式
    raw_data STRING,     -- 原始json
    etl_time TIMESTAMP,
    source_system STRING
) PARTITIONED BY (dt STRING)
  TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- DWD层：清洗脱敏
CREATE TABLE dwd_order (
    order_id BIGINT,
    user_id INT,
    amount DECIMAL(15,2),           -- 格式转换
    order_time TIMESTAMP,            -- 时间转换
    phone STRING,                    -- 脱敏处理
    id_card STRING,                  -- 加密存储
    status STRING,
    etl_time TIMESTAMP,
    source_system STRING
) PARTITIONED BY (dt STRING);

-- DWS层：汇总宽表
CREATE TABLE dws_user_trade_1d (
    user_id INT,
    dt STRING,
    order_count INT,               -- 订单数
    order_amount DECIMAL(15,2),    -- 订单金额
    avg_amount DECIMAL(15,2),       -- 平均金额
    max_amount DECIMAL(15,2),      -- 最大金额
    first_order_time TIMESTAMP,     -- 首单时间
    last_order_time TIMESTAMP,      -- 最后订单时间
    order_days INT,                 -- 购买天数
    category_count INT             -- 购买类目数
);

-- ADS层：业务报表
CREATE TABLE ads_daily_sales (
    dt STRING,
    region STRING,
    category STRING,
    sales_amount DECIMAL(15,2),
    order_count INT,
    uv INT,
    conversion_rate DECIMAL(10,4)
);
```

## 2. 维度建模

### 2.1 事实表设计

```sql
-- 事务事实表：记录业务过程
CREATE TABLE fact_order (
    -- 主键
    order_id BIGINT,
    
    -- 外键
    user_key INT,
    product_key INT,
    store_key INT,
    date_key INT,
    category_key INT,
    
    -- 度量值
    order_amount DECIMAL(15,2),
    pay_amount DECIMAL(15,2),
    discount_amount DECIMAL(15,2),
    freight_amount DECIMAL(15,2),
    quantity INT,
    
    -- 退化维度
    order_status STRING,           -- 订单状态
    payment_method STRING,         -- 支付方式
    shipping_type STRING,          -- 配送方式
    
    -- 时间
    order_time TIMESTAMP,
    pay_time TIMESTAMP,
    deliver_time TIMESTAMP,
    confirm_time TIMESTAMP,
    
    -- ETL
    etl_time TIMESTAMP
) ENGINE=OLAP
DUPLICATE KEY(order_id)
PARTITIONED BY (dt)
DISTRIBUTED BY HASH(order_id) BUCKETS 10;

-- 周期快照事实表：定期状态
CREATE TABLE fact_daily_inventory (
    inventory_date DATE,
    product_key INT,
    warehouse_key INT,
    
    quantity_on_hand INT,          -- 库存
    quantity_allocated INT,        -- 已分配
    quantity_available INT,        -- 可用
    inventory_value DECIMAL(15,2)
) ENGINE=OLAP
DUPLICATE KEY(inventory_date, product_key)
PARTITIONED BY (dt)
DISTRIBUTED BY HASH(product_key) BUCKETS 10;

-- 累积快照事实表：过程记录
CREATE TABLE fact_order_wide (
    order_id BIGINT,
    user_key INT,
    
    -- 时间戳
    create_time TIMESTAMP,
    pay_time TIMESTAMP,
    deliver_time TIMESTAMP,
    confirm_time TIMESTAMP,
    finish_time TIMESTAMP,
    
    -- 状态
    order_status STRING,
    payment_status STRING,
    shipping_status STRING,
    
    -- 度量值
    order_amount DECIMAL(15,2)
) ENGINE=OLAP
DUPLICATE KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 10;
```

### 2.2 维度表设计

```sql
-- 缓慢变化维 Type2
CREATE TABLE dim_user (
    user_key INT PRIMARY KEY,
    user_id INT,
    user_name STRING,
    phone STRING,
    email STRING,
    
    -- SCD字段
    start_date DATE,
    end_date DATE,
    is_current BOOLEAN,
    version INT,
    
    -- 审计
    created_at TIMESTAMP,
    updated_at TIMESTAMP
) ENGINE=OLAP
DUPLICATE KEY(user_key)
DISTRIBUTED BY HASH(user_key) BUCKETS 1;

-- 维度表：日期
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    date_value DATE,
    year INT,
    quarter INT,
    quarter_name STRING,
    month INT,
    month_name STRING,
    month_name_cn STRING,
    week_of_year INT,
    day_of_week INT,
    day_name STRING,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    holiday_name STRING,
    fiscal_year INT,
    fiscal_quarter INT
) ENGINE=OLAP
DUPLICATE KEY(date_key)
DISTRIBUTED BY HASH(date_key) BUCKETS 1;

-- 维度表：商品(含层级)
CREATE TABLE dim_product (
    product_key INT PRIMARY KEY,
    product_id INT,
    product_name STRING,
    
    -- 多级类目
    category_id INT,
    category_name STRING,
    category_level1 INT,
    category_level1_name STRING,
    category_level2 INT,
    category_level2_name STRING,
    category_level3 INT,
    category_level3_name STRING,
    
    -- 品牌
    brand_id INT,
    brand_name STRING,
    
    -- 价格
    cost_price DECIMAL(15,2),
    retail_price DECIMAL(15,2),
    
    -- 状态
    is_online BOOLEAN,
    is_active BOOLEAN
) ENGINE=OLAP
DUPLICATE KEY(product_key)
DISTRIBUTED BY HASH(product_key) BUCKETS 10;
```

## 3. ETL开发

### 3.1 离线ETL

```sql
-- ODS -> DWD 清洗
INSERT OVERWRITE TABLE dwd.dwd_order PARTITION(dt='${dt}')
SELECT 
    -- 主键
    CAST(order_id AS BIGINT) AS order_id,
    
    -- 外键
    user_id,
    product_id,
    store_id,
    
    -- 度量值清洗
    CASE 
        WHEN amount REGEXP '^[0-9]+\\.[0-9]+$' 
        THEN CAST(amount AS DECIMAL(15,2))
        ELSE 0 
    END AS amount,
    
    -- 时间清洗
    FROM_UNIXTIME(CAST(create_time AS BIGINT)) AS order_time,
    
    -- 状态标准化
    CASE status 
        WHEN 'PAID' THEN 'PAID'
        WHEN 'PAY' THEN 'PAID'
        ELSE 'UNKNOWN'
    END AS status,
    
    -- 脱敏处理
    CONCAT(LEFT(phone, 3), '****', RIGHT(phone, 4)) AS phone,
    
    -- ETL信息
    CURRENT_TIMESTAMP() AS etl_time,
    'ods_order' AS source_system
FROM ods.ods_order
WHERE dt = '${dt}'
  AND order_id IS NOT NULL
  AND order_id != '';
```

### 3.2 实时ETL

```java
// Flink SQL CDC
CREATE TABLE orders (
    order_id STRING,
    user_id STRING,
    amount DECIMAL(15,2),
    status STRING,
    order_time TIMESTAMP(3),
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'localhost',
    'port' = '3306',
    'username' = 'root',
    'password' = '123456',
    'database-name' = 'ecommerce',
    'table-name' = 'orders'
);

-- 实时清洗
INSERT INTO dwd_order
SELECT 
    CAST(order_id AS BIGINT) AS order_id,
    CAST(user_id AS INT) AS user_id,
    amount,
    order_time,
    CASE status 
        WHEN 'PAID' THEN 'PAID'
        WHEN 'PAY' THEN 'PAID'
        ELSE 'UNKNOWN'
    END AS status,
    CURRENT_TIMESTAMP() AS etl_time
FROM orders;
```

### 3.3 增量同步

```sql
-- 每日增量同步
INSERT OVERWRITE TABLE dwd_order PARTITION(dt='${dt}')
SELECT * FROM (
    -- 新增数据
    SELECT * FROM ods_order WHERE dt = '${dt}'
    UNION ALL
    -- 变化数据(合并)
    SELECT a.* FROM dwd_order a
    JOIN ods_order b 
        ON a.order_id = b.order_id 
        AND a.dt = '${yesterday}'
    WHERE b.dt = '${dt}'
        AND a.update_time < b.update_time
) t;
```

## 4. 数据治理

### 4.1 元数据管理

```sql
-- 元数据表
CREATE TABLE metadata_table (
    table_id BIGINT PRIMARY KEY,
    table_name STRING,
    table_type STRING,          -- ODS/DWD/DWS/ADS
    business_domain STRING,
    owner_dept STRING,
    owner_name STRING,
    description STRING,
    create_time TIMESTAMP,
    update_time TIMESTAMP
);

-- 字段元数据
CREATE TABLE metadata_column (
    column_id BIGINT PRIMARY KEY,
    table_id BIGINT,
    column_name STRING,
    data_type STRING,
    is_primary_key BOOLEAN,
    is_nullable BOOLEAN,
    description STRING,
    business_name STRING
);

-- 血缘关系
CREATE TABLE lineage_关系 (
    id BIGINT PRIMARY KEY,
    source_table STRING,
    source_column STRING,
    target_table STRING,
    target_column STRING,
    transform_type STRING,       -- 直接/计算/聚合
    transform_logic STRING,
    create_time TIMESTAMP
);
```

### 4.2 数据质量

```sql
-- 完整性检查
-- 检查主键唯一性
SELECT 
    '唯一性检查' AS check_type,
    COUNT(*) AS total_count,
    COUNT(DISTINCT order_id) AS unique_count,
    COUNT(*) - COUNT(DISTINCT order_id) AS duplicate_count
FROM dwd_order
WHERE dt = '${dt}';

-- 检查空值
SELECT 
    column_name,
    COUNT(*) AS total_count,
    SUM(CASE WHEN column IS NULL THEN 1 ELSE 0 END) AS null_count
FROM dwd_order
WHERE dt = '${dt}'
GROUP BY column_name;

-- 检查值域
SELECT 
    '值域检查' AS check_type,
    status,
    COUNT(*) AS count
FROM dwd_order
WHERE dt = '${dt}'
GROUP BY status
HAVING status NOT IN ('PENDING', 'PAID', 'SHIPPED', 'CONFIRMED', 'CANCELLED');
```

### 4.3 权限管理

```sql
-- 用户权限表
CREATE TABLE user_permission (
    user_id BIGINT,
    table_name STRING,
    permission_type STRING,     -- SELECT/INSERT/UPDATE
    grant_time TIMESTAMP,
    grant_by STRING
);

-- 脱敏规则
CREATE TABLE masking_rule (
    table_name STRING,
    column_name STRING,
    masking_type STRING,        -- FULL/PARTIAL/HASH
    masking_format STRING,       -- 脱敏格式
    created_by STRING
);
```

## 5. 指标体系

### 5.1 原子指标

```sql
-- 原子指标定义
INSERT INTO dwd_dim_indicator (
    indicator_code,
    indicator_name,
    indicator_type,
    expression,
    aggregation_type,
    business_domain,
    owner_name
) VALUES
    ('gmv', 'GMV', 'ATOMIC', 'SUM(order_amount)', 'SUM', '交易域', '数据组'),
    ('order_cnt', '订单数', 'ATOMIC', 'COUNT(order_id)', 'COUNT', '交易域', '数据组'),
    ('user_cnt', '用户数', 'ATOMIC', 'COUNT(DISTINCT user_id)', 'COUNT', '用户域', '数据组'),
    ('uv', '访客数', 'ATOMIC', 'COUNT(DISTINCT user_id)', 'COUNT', '流量域', '数据组');
```

### 5.2 派生指标

```sql
-- 派生指标定义
INSERT INTO dwd_dim_indicator (
    indicator_code,
    indicator_name,
    indicator_type,
    parent_indicator_id,
    time_dimension,
    filter_conditions,
    business_domain
) VALUES
    ('gmv_1d', '当日GMV', 'DERIVED', 
     (SELECT indicator_id FROM dwd_dim_indicator WHERE indicator_code='gmv'),
     '日', 
     NULL, 
     '交易域'),
     
    ('gmv_7d', '近7日GMV', 'DERIVED',
     (SELECT indicator_id FROM dwd_dim_indicator WHERE indicator_code='gmv'),
     '日',
     "dt >= DATE_SUB('${dt}', 6)",
     '交易域'),
     
    ('gmv_30d', '近30日GMV', 'DERIVED',
     (SELECT indicator_id FROM dwd_dim_indicator WHERE indicator_code='gmv'),
     '日',
     "dt >= DATE_SUB('${dt}', 29)",
     '交易域');
```

### 5.3 复合指标

```sql
-- 复合指标计算
INSERT INTO dws_trade_indicator_1d (dt, indicator_code, indicator_value)
SELECT 
    dt,
    'avg_order_amount' AS indicator_code,
    SUM(order_amount) / COUNT(DISTINCT user_id) AS indicator_value
FROM dwd_order
WHERE dt = '${dt}'
  AND order_status != 'CANCELLED'
GROUP BY dt;

-- 转化率
INSERT INTO dws_trade_indicator_1d (dt, indicator_code, indicator_value)
SELECT 
    dt,
    'conversion_rate' AS indicator_code,
    COUNT(DISTINCT CASE WHEN order_status = 'PAID' THEN user_id END) * 1.0 / 
    COUNT(DISTINCT user_id) AS indicator_value
FROM dwd_order
WHERE dt = '${dt}'
GROUP BY dt;
```

## 6. 边界处理

### 6.1 数据延迟

```sql
-- 等待源数据就绪
-- 方案1：时间等待
WHILE CURRENT_TIMESTAMP() < DATE_ADD('${dt}', 1)
    AND (
        SELECT COUNT(*) FROM ods_order WHERE dt = '${dt}'
    ) < (SELECT COUNT(*) FROM ods_order WHERE dt = DATE_SUB('${dt}', 1)) * 0.95
DO
    SLEEP(300);  -- 5分钟检查一次
END WHILE;

-- 方案2：前置任务依赖
-- 设置调度依赖，上游任务完成才执行
```

### 6.2 数据倾斜

```sql
-- 现象：某些key数据量远超其他
-- 解决：加盐打散
SELECT 
    CONCAT(key, '_', FLOOR(RAND() * 10)) AS salt_key,
    COUNT(*) AS cnt
FROM table
GROUP BY salt_key;

-- 预聚合
INSERT INTO dws_category_sales_1d
SELECT 
    dt,
    category_id,
    region,
    SUM(amount) AS amount
FROM dwd_order
WHERE dt = '${dt}'
GROUP BY dt, category_id, region;
```

### 6.3 幂等性

```sql
-- 多次运行结果一致
-- 方案1：覆盖写入
INSERT OVERWRITE TABLE dws_user_trade_1d PARTITION(dt='${dt}')
SELECT ...
FROM dwd_order
WHERE dt = '${dt}';

-- 方案2：去重后再写入
INSERT INTO dws_user_trade_1d
SELECT ...
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY order_time DESC) AS rn
    FROM dwd_order
    WHERE dt = '${dt}'
) t
WHERE t.rn = 1;
```
