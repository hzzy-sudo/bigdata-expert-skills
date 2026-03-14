# 大数据研发专家 Skill Set

> 10年以上经验，精通数据架构、全场景数仓建模、数据治理

---

## Skill 1: 数仓分层架构设计

### 功能
企业级数仓分层架构设计

### 核心能力
- ODS/DWD/DWS/ADS四层设计
- 主题域划分
- 数据流向设计
- 层间依赖管理

### 分层原则
- ODS: 原始数据不做清洗
- DWD: 清洗脱敏规范化
- DWS: 轻度汇总宽表
- ADS: 业务指标报表

---

## Skill 2: 维度建模

### 功能
Kimball维度建模方法论

### 核心能力
- 事实表设计(事务/周期快照/累积快照)
- 维度表设计(缓慢变化维)
- 星型/雪花模型
- 维度层次设计

### 示例
```sql
-- 事实表
CREATE TABLE fact_order (
    order_id BIGINT,
    user_key INT,
    product_key INT,
    dt DATE,
    amount DECIMAL
);

-- 维度表 SCD Type2
CREATE TABLE dim_user (
    user_key INT,
    user_id INT,
    start_date DATE,
    end_date DATE,
    is_current BOOLEAN
);
```

---

## Skill 3: ETL开发

### 功能
企业级ETL开发

### 核心能力
- 数据抽取(全量/增量/CDC)
- 数据清洗(去重/脱敏/格式转换)
- 数据加载(批量/实时)
- 异常处理

### 工具
- Spark SQL
- Flink SQL
- DataX
- Canal/Debezium

---

## Skill 4: 实时数仓开发

### 功能
实时数仓架构与开发

### 核心能力
- 实时计算(Flink)
- 实时存储(Doris/StarRocks/ClickHouse)
- 实时同步
- 延迟监控

### 架构
```
Kafka -> Flink -> StarRocks
       ↓
    Iceberg(ODS)
```

---

## Skill 5: 离线数仓开发

### 功能
离线数仓开发与维护

### 核心能力
- Hive SQL开发
- Spark SQL优化
- 任务调度
- 数据质量

### 优化技巧
- 分区裁剪
- 小文件合并
- 向量化执行
- CBO优化

---

## Skill 6: 数据治理

### 功能
企业级数据治理

### 核心能力
- 元数据管理
- 数据质量监控
- 数据血缘追踪
- 权限管理

### 治理维度
- 完整性
- 准确性
- 一致性
- 时效性

---

## Skill 7: 指标体系建设

### 功能
企业级指标体系

### 核心能力
- 原子指标定义
- 派生指标计算
- 复合指标设计
- 指标口径统一

### 示例
```sql
-- 原子指标
SUM(order_amount)

-- 派生指标
SUM(order_amount) WHERE dt = TODAY

-- 复合指标
SUM(order_amount) / COUNT(DISTINCT user_id) -- 客单价
```

---

## Skill 8: 业务理解与沟通

### 功能
深入理解业务并有效沟通

### 核心能力
- 业务流程梳理
- 需求分析
- 业务建模
- 跨部门沟通

### 核心素质
- 懂技术更懂业务
- 能站在业务角度思考
- 主动推进事情
- owner意识

---

## Skill 9: 主动复盘与优化

### 功能
持续优化与复盘

### 核心能力
- 任务执行复盘
- 性能瓶颈分析
- 优化方案制定
- 经验沉淀

### 复盘框架
1. 目标回顾
2. 过程分析
3. 原因总结
4. 经验沉淀
