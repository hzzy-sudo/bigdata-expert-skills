---
name: bigdata-analyst
description: 数据分析专家技能集。用于数据分析、BI开发、数据可视化、业务洞察。包括SQL分析、Tableau/PowerBI开发、Python可视化、业务归因分析、数据报告撰写。适用于需要进行数据分析、搭建BI报表、诊断业务问题、输出数据报告的场景。
---

# 数据分析专家

## 1. SQL分析

### 1.1 复杂查询模板

```sql
-- 1. 留存分析
WITH first_activity AS (
    SELECT 
        user_id,
        MIN(dt) AS first_dt
    FROM user_behavior
    WHERE dt >= '2024-01-01'
    GROUP BY user_id
),
retention AS (
    SELECT 
        a.dt,
        DATEDIFF(b.dt, a.first_dt) AS retention_days,
        COUNT(DISTINCT a.user_id) AS users
    FROM user_behavior a
    JOIN first_activity b ON a.user_id = b.user_id
    WHERE a.dt >= '2024-01-01'
    GROUP BY a.dt, b.first_dt
)
SELECT 
    dt,
    SUM(CASE WHEN retention_days = 0 THEN users ELSE 0 END) AS d0_users,
    SUM(CASE WHEN retention_days = 1 THEN users ELSE 0 END) AS d1_users,
    SUM(CASE WHEN retention_days = 3 THEN users ELSE 0 END) AS d3_users,
    SUM(CASE WHEN retention_days = 7 THEN users ELSE 0 END) AS d7_users,
    SUM(CASE WHEN retention_days = 14 THEN users ELSE 0 END) AS d14_users,
    SUM(CASE WHEN retention_days = 30 THEN users ELSE 0 END) AS d30_users
FROM retention
GROUP BY dt
ORDER BY dt;

-- 2. 漏斗分析
WITH step1 AS (
    SELECT user_id, MIN(dt) AS step1_dt
    FROM user_behavior 
    WHERE behavior = 'pv' AND dt >= '2024-01-01'
    GROUP BY user_id
),
step2 AS (
    SELECT user_id, MIN(dt) AS step2_dt
    FROM user_behavior 
    WHERE behavior = 'cart' AND dt >= '2024-01-01'
    GROUP BY user_id
),
step3 AS (
    SELECT user_id, MIN(dt) AS step3_dt
    FROM user_behavior 
    WHERE behavior = 'order' AND dt >= '2024-01-01'
    GROUP BY user_id
)
SELECT 
    COUNT(DISTINCT step1.user_id) AS step1_users,
    COUNT(DISTINCT step2.user_id) AS step2_users,
    COUNT(DISTINCT step3.user_id) AS step3_users,
    ROUND(COUNT(DISTINCT step2.user_id) * 100.0 / COUNT(DISTINCT step1.user_id), 2) AS step1_to_step2_rate,
    ROUND(COUNT(DISTINCT step3.user_id) * 100.0 / COUNT(DISTINCT step2.user_id), 2) AS step2_to_step3_rate
FROM step1
LEFT JOIN step2 ON step1.user_id = step2.user_id
LEFT JOIN step3 ON step1.user_id = step3.user_id;

-- 3. RFM分析
WITH rfm AS (
    SELECT 
        user_id,
        MAX(dt) AS recency,
        COUNT(*) AS frequency,
        SUM(amount) AS monetary
    FROM orders
    WHERE dt >= DATE_SUB(CURRENT_DATE, 90)
    GROUP BY user_id
)
SELECT 
    user_id,
    recency,
    frequency,
    monetary,
    CASE 
        WHEN DATEDIFF(CURRENT_DATE, recency) <= 7 THEN 5
        WHEN DATEDIFF(CURRENT_DATE, recency) <= 14 THEN 4
        WHEN DATEDIFF(CURRENT_DATE, recency) <= 30 THEN 3
        WHEN DATEDIFF(CURRENT_DATE, recency) <= 60 THEN 2
        ELSE 1
    END AS r_score,
    CASE 
        WHEN frequency >= 10 THEN 5
        WHEN frequency >= 5 THEN 4
        WHEN frequency >= 3 THEN 3
        WHEN frequency >= 2 THEN 2
        ELSE 1
    END AS f_score,
    CASE 
        WHEN monetary >= 5000 THEN 5
        WHEN monetary >= 2000 THEN 4
        WHEN monetary >= 1000 THEN 3
        WHEN monetary >= 500 THEN 2
        ELSE 1
    END AS m_score
FROM rfm;

-- 4. 同期群分析
WITH cohort AS (
    SELECT 
        user_id,
        MIN(dt) AS cohort_dt
    FROM orders
    WHERE dt >= '2024-01-01'
    GROUP BY user_id
),
cohort_size AS (
    SELECT cohort_dt, COUNT(DISTINCT user_id) AS users
    FROM cohort
    GROUP BY cohort_dt
),
retention AS (
    SELECT 
        a.cohort_dt,
        DATEDIFF(b.dt, a.cohort_dt) AS month_num,
        COUNT(DISTINCT b.user_id) AS users
    FROM cohort a
    JOIN orders b ON a.user_id = b.user_id
    GROUP BY a.cohort_dt, DATEDIFF(b.dt, a.cohort_dt)
)
SELECT 
    a.cohort_dt,
    a.users AS cohort_size,
    b.month_num,
    b.users,
    ROUND(b.users * 100.0 / a.users, 2) AS retention_rate
FROM cohort_size a
JOIN retention b ON a.cohort_dt = b.cohort_dt
ORDER BY a.cohort_dt, b.month_num;
```

### 1.2 窗口函数

```sql
-- 排名
SELECT 
    user_id,
    amount,
    ROW_NUMBER() OVER (ORDER BY amount DESC) AS rank,
    RANK() OVER (ORDER BY amount DESC) AS rank2,
    DENSE_RANK() OVER (ORDER BY amount DESC) AS rank3
FROM orders;

-- 分区排名
SELECT 
    category,
    user_id,
    amount,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount DESC) AS category_rank
FROM orders;

-- 累计
SELECT 
    dt,
    amount,
    SUM(amount) OVER (ORDER BY dt) AS cumulative_amount,
    AVG(amount) OVER (ORDER BY dt ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg_7d
FROM daily_sales;

-- 首尾值
SELECT 
    user_id,
    dt,
    amount,
    FIRST_VALUE(amount) OVER (PARTITION BY user_id ORDER BY dt) AS first_amount,
    LAST_VALUE(amount) OVER (PARTITION BY user_id ORDER BY dt) AS last_amount
FROM user_daily;

-- 占比
SELECT 
    category,
    amount,
    amount * 100.0 / SUM(amount) OVER () AS pct_of_total,
    amount * 100.0 / SUM(amount) OVER (PARTITION BY category) AS pct_of_category
FROM category_sales;
```

### 1.3 性能优化

```sql
-- 1. 避免SELECT *
SELECT user_id, order_id, amount FROM orders;

-- 2. 使用分区裁剪
SELECT * FROM orders WHERE dt = '2024-01-01';

-- 3. 减少JOIN
-- 优化前
SELECT a.*, b.name, c.title
FROM orders a
JOIN user b ON a.user_id = b.user_id
JOIN product c ON a.product_id = c.product_id;

-- 优化后：先用WHERE过滤
SELECT a.user_id, a.order_id, a.amount, b.name, c.title
FROM (SELECT * FROM orders WHERE dt = '2024-01-01') a
JOIN (SELECT user_id, name FROM user) b ON a.user_id = b.user_id
JOIN (SELECT product_id, title FROM product) c ON a.product_id = c.product_id;

-- 4. 预聚合
INSERT INTO dws_category_sales
SELECT category, dt, SUM(amount), COUNT(*)
FROM dwd_order
WHERE dt = '${dt}'
GROUP BY category, dt;

-- 5. 小表广播
SELECT /*+ BROADCAST(t_small) */ *
FROM large_table t_large
JOIN small_table t_small ON t_large.id = t_small.id;
```

## 2. Python数据分析

### 2.1 数据处理

```python
import pandas as pd
import numpy as np

# 数据读取
df = pd.read_csv('data.csv')
df = pd.read_sql(query, connection)
df = pd.read_parquet('data.parquet')

# 数据清洗
df.dropna()                           # 删除空值
df.fillna(0)                          # 填充空值
df.drop_duplicates()                   # 去重
df['date'] = pd.to_datetime(df['date'])  # 类型转换

# 数据筛选
df[df['amount'] > 1000]               # 条件筛选
df.query('amount > 1000 & status == "PAID"')  # 表达式筛选

# 数据转换
df['amount'] = df['amount'].apply(lambda x: x * 1.1)  # Apply
df['category'] = df['category'].map({'A': 1, 'B': 2})  # Map

# 分组聚合
df.groupby('category').agg({
    'amount': ['sum', 'mean', 'count'],
    'user_id': 'nunique'
})

# 透视表
pd.pivot_table(df, 
    index='category', 
    columns='month', 
    values='amount', 
    aggfunc='sum',
    fill_value=0)
```

### 2.2 可视化

```python
import matplotlib.pyplot as plt
import seaborn as sns

# 设置中文
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False

# 折线图
plt.figure(figsize=(12, 6))
plt.plot(df['date'], df['amount'])
plt.title('Sales Trend')
plt.xlabel('Date')
plt.ylabel('Amount')
plt.grid(True)
plt.show()

# 柱状图
plt.figure(figsize=(10, 6))
df.groupby('category')['amount'].sum().plot(kind='bar')
plt.title('Sales by Category')
plt.show()

# 散点图
plt.figure(figsize=(10, 6))
plt.scatter(df['pv'], df['conversion'], alpha=0.5)
plt.xlabel('Page Views')
plt.ylabel('Conversion Rate')
plt.show()

# 热力图
plt.figure(figsize=(12, 8))
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm')
plt.show()

# 多图组合
fig, axes = plt.subplots(2, 2, figsize=(14, 10))
axes[0, 0].plot(df['date'], df['amount'])
axes[0, 1].bar(df['category'], df['amount'])
axes[1, 0].hist(df['amount'], bins=30)
axes[1, 1].scatter(df['x'], df['y'])
plt.tight_layout()
plt.show()
```

### 2.3 统计分析

```python
from scipy import stats
import numpy as np

# 描述统计
df.describe()
df['amount'].describe()

# 相关性
df.corr()
stats.pearsonr(x, y)     # 皮尔逊相关
stats.spearmanr(x, y)    # 斯皮尔曼相关

# 假设检验
# T检验
t_stat, p_value = stats.ttest_ind(group1, group2)

# 卡方检验
chi2, p_value, dof, expected = stats.chi2_contingency(contingency_table)

# ANOVA
f_stat, p_value = stats.f_oneway(*groups)

# 回归分析
from sklearn.linear_model import LinearRegression
model = LinearRegression()
model.fit(X, y)
```

## 3. BI开发

### 3.1 Tableau

```sql
-- Tableau数据源准备
-- 1. 提取关键字段
-- 2. 提前计算聚合
-- 3. 优化数据类型

-- Tableau计算字段
// 同比
SUM([Sales]) / 
LOOKUP(SUM([Sales]), -1)

// 累计
RUNNING_SUM(SUM([Sales]))

// 占比
SUM([Sales]) / TOTAL(SUM([Sales]))

// 动态筛选
IF [Date] >= {MAX([Date])} THEN 'Current'
ELSE 'History'
END
```

### 3.2 PowerBI

```python
# PowerBI DAX
# 度量值
Total Sales = SUM('Sales'[Amount])

Sales YoY = 
VAR CurrentSales = [Total Sales]
VAR LastYearSales = CALCULATE([Total Sales], SAMEPERIODLASTYEAR('Date'[Date]))
RETURN CurrentSales - LastYearSales

% to Total = 
DIVIDE([Total Sales], CALCULATE([Total Sales], ALL('Product'[Category])))

Moving Average = 
AVERAGEX(
    DATESINPERIOD('Date'[Date], LASTDATE('Date'[Date]), -30, DAY),
    [Total Sales]
)
```

### 3.3 数据大屏

```javascript
// 大屏设计原则
{
  "布局": "16:9或32:9",
  "配色": "深色背景，亮色数据",
  "刷新": "5-30秒自动刷新",
  "交互": "支持点击下钻",
  "字体": "数字用等宽字体",
  "动画": "数据变化有动画"
}

// 数据校验
function validateData(data) {
  // 检查数据完整性
  if (!data || data.length === 0) {
    return { valid: false, message: '数据为空' };
  }
  
  // 检查数值范围
  if (data.some(v => v < 0)) {
    return { valid: false, message: '存在负数' };
  }
  
  return { valid: true };
}
```

## 4. 业务洞察

### 4.1 分析框架

```python
# 1. 5W2H分析法
# What: 发生了什么？
# Why: 为什么会发生？
# When: 什么时候发生的？
# Where: 在哪里发生的？
# Who: 谁参与的？
# How: 怎么发生的？
# How much: 多少？

# 2. 拆解法
# 收入 = UV * 转化率 * 客单价
# 收入 = DAU * 人均访问次数 * 转化率 * 客单价
# 逐层拆解找到问题点

# 3. 对比分析
# 同比: 去年同期
# 环比: 上个周期
# 竞品: 竞争对手
# 目标: 计划值
```

### 4.2 归因分析

```python
# 首次归因
def first_attribution(conversions):
    # 归因给第一个触点
    return conversions.groupby('first_channel')['amount'].sum()

# 末次归因
def last_attribution(conversions):
    # 归因给最后一个触点
    return conversions.groupby('last_channel')['amount'].sum()

# 线性归因
def linear_attribution(conversions):
    # 每个触点均分
    def linear(row):
        channels = row['channels']
        amount = row['amount']
        return amount / len(channels)
    return conversions.apply(linear).groupby('channel').sum()

# 时间衰减归因
def time_decay_attribution(conversions, decay=0.8):
    # 越接近转化的触点权重越高
    def decay_weight(row):
        channels = row['channels']
        amount = row['amount']
        weights = [decay ** (len(channels) - i - 1) for i in range(len(channels))]
        return sum([w * amount / sum(weights) for w in weights])
    return conversions.apply(decay_weight).groupby('channel').sum()
```

### 4.3 异常检测

```python
# 1. 3σ原则
def detect_3sigma(data, column):
    mean = data[column].mean()
    std = data[column].std()
    lower = mean - 3 * std
    upper = mean + 3 * std
    return data[(data[column] < lower) | (data[column] > upper)]

# 2. IQR方法
def detect_iqr(data, column):
    Q1 = data[column].quantile(0.25)
    Q3 = data[column].quantile(0.75)
    IQR = Q3 - Q1
    lower = Q1 - 1.5 * IQR
    upper = Q3 + 1.5 * IQR
    return data[(data[column] < lower) | (data[column] > upper)]

# 3. 移动平均
def detect_moving_average(data, column, window=7):
    rolling_mean = data[column].rolling(window=window).mean()
    rolling_std = data[column].rolling(window=window).std()
    threshold = 2 * rolling_std
    anomalies = data[(data[column] > rolling_mean + threshold) | 
                     (data[column] < rolling_mean - threshold)]
    return anomalies
```

## 5. 报告撰写

### 5.1 报告结构

```markdown
# 数据分析报告

## 1. 背景与目标
- 业务背景
- 分析目标
- 关键问题

## 2. 数据概览
- 数据来源
- 时间范围
- 数据量级
- 关键指标

## 3. 核心发现
- 发现1
- 发现2
- 发现3

## 4. 原因分析
- 维度拆解
- 归因分析
- 对比分析

## 5. 行动建议
- 短期建议
- 中期建议
- 长期建议

## 6. 附录
- 数据字典
- 方法说明
- 限制说明
```

### 5.2 图表选择

| 分析目的 | 推荐图表 |
|---------|---------|
| 趋势 | 折线图、面积图 |
| 比较 | 柱状图、条形图 |
| 构成 | 饼图、堆叠图 |
| 分布 | 直方图、箱线图 |
| 关系 | 散点图、热力图 |
| 排名 | 柱状图、条形图 |

### 5.3 建议模板

```sql
-- 业务建议SQL
SELECT 
    issue,
    impact,
    current_value,
    target_value,
    action,
    owner,
    deadline
FROM (
    SELECT 
        '转化率低' AS issue,
        '收入减少' AS impact,
        '2.5%' AS current_value,
        '3.5%' AS target_value,
        '优化落地页' AS action,
        '产品组' AS owner,
        '2024-04-01' AS deadline
) t
WHERE current_value < target_value;
```

## 6. 边界处理

### 6.1 数据异常

```python
# 异常值处理
def handle_outliers(df, column, method='clip'):
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    
    if method == 'clip':
        df[column] = df[column].clip(Q1 - 1.5*IQR, Q3 + 1.5*IQR)
    elif method == 'remove':
        df = df[(df[column] >= Q1 - 1.5*IQR) & (df[column] <= Q3 + 1.5*IQR)]
    elif method == 'median':
        median = df[column].median()
        df.loc[df[column] < Q1 - 1.5*IQR, column] = median
        df.loc[df[column] > Q3 + 1.5*IQR, column] = median
    
    return df
```

### 6.2 缺失值

```python
# 缺失值处理策略
def handle_missing(df):
    # 1. 删除
    df.dropna(axis=0, how='all')    # 删除全为空的行
    df.dropna(axis=1, how='all')    # 删除全为空的列
    
    # 2. 填充
    df['numeric'].fillna(df['numeric'].mean())      # 均值
    df['numeric'].fillna(df['numeric'].median())    # 中位数
    df['category'].fillna(df['category'].mode()[0])  # 众数
    df['numeric'].fillna(method='ffill')           # 前向填充
    df['numeric'].fillna(method='bfill')           # 后向填充
    
    # 3. 预测填充
    from sklearn.impute import KNNImputer
    imputer = KNNImputer(n_neighbors=5)
    df_filled = pd.DataFrame(imputer.fit_transform(df), columns=df.columns)
    
    return df
```

### 6.3 数据对齐

```python
# 时间对齐
def align_time(df1, df2, time_col='date'):
    # 内连接
    merged = pd.merge(df1, df2, on=time_col, how='inner')
    
    # 左连接 + 填充
    merged = pd.merge(df1, df2, on=time_col, how='left')
    merged.fillna(0, inplace=True)
    
    return merged
```
