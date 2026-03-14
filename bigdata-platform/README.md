# 大数据平台研发专家 Skill Set

> 10年以上经验，精通所有大数据组件的核心原理和二次开发

---

## Skill 1: Flink源码阅读与分析

### 功能
Flink源码深度理解与问题定位

### 核心能力
- JobGraph生成流程
- StreamGraph构建
- Task执行原理
- Checkpoint机制

### 关键类
- StreamExecutionEnvironment
- StreamGraphGenerator
- StreamTask
- CheckpointCoordinator

---

## Skill 2: Spark源码阅读与分析

### 功能
Spark源码深度理解与优化

### 核心能力
- DAG生成与Stage划分
- Task执行流程
- Shuffle原理
- 内存管理机制

### 关键类
- DAGScheduler
- TaskScheduler
- ShuffleManager
- MemoryManager

---

## Skill 3: 二次开发能力

### 功能
基于开源组件进行二次开发

### 核心能力
- 自定义Source/Sink
- UDF/UDTF开发
- Connector开发
- 插件化扩展

### 示例
```java
// 自定义Flink Source
public class MySource implements SourceFunction {}

// 自定义Spark UDF
spark.udf.register("myUDF", (String s) -> s.toUpperCase())
```

---

## Skill 4: 性能优化

### 功能
大数据组件性能调优

### 核心能力
- 网络IO优化
- 内存优化
- CPU利用率优化
- 存储优化

### Flink优化
- 状态大小控制
- Checkpoint调优
- 背压处理
- 并行度设置

### Spark优化
- 内存列式存储
- Shuffle优化
- 自适应查询执行
- AQE配置

---

## Skill 5: Hudi二次开发

### 功能
Hudi表格式的二次开发

### 核心能力
- HoodieTable API
- 自定义Payload
- 索引优化
- 压缩策略

### 使用场景
```java
// 自定义Payload
public class MyPayload extends HoodieRecordPayload {}
```

---

## Skill 6: Iceberg二次开发

### 功能
Iceberg表格式的二次开发

### 核心能力
- Table API使用
- 自定义Partition
- Action开发
- 血统追踪

---

## Skill 7: ClickHouse二次开发

### 功能
ClickHouse性能优化与二次开发

### 核心能力
- 物化视图优化
- 索引优化
- 分布式表连接
- 集群运维

---

## Skill 8: StarRocks二次开发

### 功能
StarRocks性能优化与二次开发

### 核心能力
- BE FE原理
- 查询优化
- 物化视图
- 导入导出

---

## Skill 9: 内核原理深入

### 功能
大数据组件内核原理

### 核心能力
- 分布式一致性算法
- 序列化机制
- 内存管理
- 网络通信

### 掌握深度
- Raft/Paxos
- Kryo/Avro/Protobuf
- JVM内存模型
- Netty原理
