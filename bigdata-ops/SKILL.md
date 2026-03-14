---
name: bigdata-ops
description: 大数据运维专家技能集。用于大数据集群部署、运维、故障排查、监控告警等工作。包括Hadoop、Kafka、Flink、Spark等组件的安装配置、性能调优、故障诊断。适用于需要部署大数据集群、排查组件故障、配置监控告警的场景。
---

# 大数据运维专家

## 1. Hadoop集群部署

### 1.1 环境准备

```bash
# 1. 修改hostname
hostnamectl set-hostname node1

# 2. 配置hosts
cat >> /etc/hosts << EOF
192.168.1.101 node1
192.168.1.102 node2
192.168.1.103 node3
EOF

# 3. 关闭防火墙
systemctl stop firewalld
systemctl disable firewalld

# 4. 关闭SELinux
setenforce 0
sed -i 's/SELINUX=enforcing/SELINUX=disabled/' /etc/selinux/config

# 5. 配置SSH免密
ssh-keygen -t rsa
ssh-copy-id node2
ssh-copy-id node3

# 6. 安装JDK
yum install -y java-1.8.0-openjdk java-1.8.0-openjdk-devel
echo "JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk" >> /etc/profile
```

### 1.2 HDFS部署

```bash
# 1. 下载Hadoop
wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -zxf hadoop-3.3.6.tar.gz -C /opt/

# 2. 配置环境变量
export HADOOP_HOME=/opt/hadoop-3.3.6
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# 3. core-site.xml
cat > $HADOOP_HOME/etc/hadoop/core-site.xml << EOF
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://node1:9000</value>
    </property>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/opt/hadoop/tmp</value>
    </property>
</configuration>

# 4. hdfs-site.xml
cat > $HADOOP_HOME/etc/hadoop/hdfs-site.xml << EOF
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>3</value>
    </property>
    <property>
        <name>dfs.namenode.http-address</name>
        <value>node1:9870</value>
    </property>
    <property>
        <name>dfs.namenode.secondary.http-address</name>
        <value>node2:9868</value>
    </property>
</configuration>

# 5. workers文件
cat > $HADOOP_HOME/etc/hadoop/workers << EOF
node1
node2
node3
EOF

# 6. 格式化NN
hdfs namenode -format

# 7. 启动HDFS
start-dfs.sh
```

### 1.3 YARN部署

```bash
# 1. mapred-site.xml
cat > $HADOOP_HOME/etc/hadoop/mapred-site.xml << EOF
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>mapreduce.jobhistory.address</name>
        <value>node1:10020</value>
    </property>
</configuration>

# 2. yarn-site.xml
cat > $HADOOP_HOME/etc/hadoop/yarn-site.xml << EOF
<configuration>
    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>node1</value>
    </property>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.nodemanager.resource.memory-mb</name>
        <value>16384</value>
    </property>
    <property>
        <name>yarn.nodemanager.resource.cpu-vcores</name>
        <value>8</value>
    </property>
</configuration>

# 3. 启动YARN
start-yarn.sh
```

## 2. Kafka集群部署

### 2.1 安装配置

```bash
# 1. 下载Kafka
wget https://archive.apache.org/dist/kafka/3.6.0/kafka_2.13-3.6.0.tgz
tar -zxf kafka_2.13-3.6.0.tgz -C /opt/

# 2. server.properties配置
cat > /opt/kafka_2.13-3.6.0/config/server.properties << EOF
broker.id=0
listeners=PLAINTEXT://node1:9092
advertised.listeners=PLAINTEXT://node1:9092
zookeeper.connect=node1:2181,node2:2181,node3:2181
log.dirs=/opt/kafka/logs
num.network.threads=8
num.io.threads=16
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
num.partitions=3
num.recovery.threads.per.data.dir=1
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000
zookeeper.connect.timeout.ms=18000
group.initial.rebalance.delay.ms=0
EOF

# 3. 多节点配置
# node2: broker.id=1, listeners=PLAINTEXT://node2:9092
# node3: broker.id=2, listeners=PLAINTEXT://node3:9092
```

### 2.2 启动管理

```bash
# 1. 启动Zookeeper
zookeeper-server-start.sh -daemon config/zookeeper.properties

# 2. 启动Kafka
kafka-server-start.sh -daemon config/server.properties

# 3. 创建Topic
kafka-topics.sh --create \
  --topic test-topic \
  --partitions 6 \
  --replication-factor 3 \
  --bootstrap-server node1:9092,node2:9092,node3:9092

# 4. 查看Topic
kafka-topics.sh --describe \
  --topic test-topic \
  --bootstrap-server node1:9092
```

### 2.3 运维命令

```bash
# 生产消息
kafka-console-producer.sh \
  --topic test-topic \
  --bootstrap-server node1:9092

# 消费消息
kafka-console-consumer.sh \
  --topic test-topic \
  --from-beginning \
  --bootstrap-server node1:9092

# 查看消费组
kafka-consumer-groups.sh \
  --bootstrap-server node1:9092 \
  --list

# 查看积压
kafka-consumer-groups.sh \
  --bootstrap-server node1:9092 \
  --group test-group \
  --describe

# 调整分区
kafka-topics.sh --alter \
  --topic test-topic \
  --partitions 9 \
  --bootstrap-server node1:9092
```

## 3. Flink集群部署

### 3.1 Standalone模式

```bash
# 1. 下载Flink
wget https://archive.apache.org/dist/flink/flink-1.18.1/flink-1.18.1-bin-scala_2.12.tgz
tar -zxf flink-1.18.1-bin-scala_2.12.tgz -C /opt/

# 2. 配置flink-conf.yaml
cat > /opt/flink-1.18.1/conf/flink-conf.yaml << EOF
jobmanager.memory.process.size: 2048m
taskmanager.memory.process.size: 4096m
taskmanager.numberOfTaskSlots: 8
parallelism.default: 2
rest.port: 8081
EOF

# 3. 配置workers
cat > /opt/flink-1.18.1/conf/workers << EOF
node1
node2
node3
EOF

# 4. 启动集群
/opt/flink-1.18.1/bin/start-cluster.sh
```

### 3.2 YARN模式

```bash
# 1. 提交Job到YARN
flink run-application \
  --target yarn-application \
  -yjm 2048m \
  -ytm 4096m \
  -yD taskmanager.numberOfTaskSlots=8 \
  /path/to/your-job.jar

# 2. Session模式
yarn-session.sh -n 3 -tm 4096m -s 8

# 3. 提交Job
flink run \
  --target yarn-session \
  /path/to/your-job.jar
```

### 3.3 高可用配置

```bash
# flink-conf.yaml
high-availability: zookeeper
high-availability.storageDir: hdfs:///flink/ha/
zookeeper.quorum: node1:2181,node2:2181,node3:2181
```

## 4. 故障排查

### 4.1 HDFS故障

```bash
# 1. NameNode无法启动
# 检查日志
tail -f /opt/hadoop/logs/hadoop-root-namenode-*.log

# 修复损坏的元数据
# 步骤：
# 1. 停止集群
# 2. 备份元数据目录
# 3. 使用secondary NN恢复
hdfs namenode -bootstrapStandby

# 2. DataNode无法连接
# 检查网络和配置
hdfs dfsadmin -report
hdfs dfsadmin -safemode leave

# 3. 块丢失恢复
hdfs fsck / -files -blocks -locations
```

### 4.2 Kafka故障

```bash
# 1. 消息积压处理
# 增加消费者
# 调整fetch参数
kafka-consumer-groups.sh --bootstrap-server node1:9092 \
  --group test-group --reset-offsets \
  --to-earliest --topic test-topic --execute

# 2. Leader不平衡
kafka-leader-election.sh --election-type PREFERRED \
  --topic test-topic --partition 0,1,2,3,4,5

# 3. 磁盘满处理
# 清理日志
kafka-logs --delete \
  --topic test-topic \
  --partition 0 \
  --interval-days 7
```

### 4.3 Flink故障

```bash
# 1. Checkpoint失败
# 检查state大小
# 调整checkpoint间隔
# 检查外部存储

# 2. 反压处理
# 查看backpressure
curl http://jobmanager:8081/jobs/<jobid>/vertices/<vertexid>/backpressure

# 3. 内存问题
# 调整taskmanager.memory
# 检查GC
jstat -gcutil <pid> 1000
```

## 5. 监控告警

### 5.1 Prometheus配置

```yaml
# prometheus.yml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'hadoop'
    static_configs:
      - targets: ['node1:9870', 'node2:9870', 'node3:9870']

  - job_name: 'kafka'
    static_configs:
      - targets: ['node1:9090', 'node2:9090', 'node3:9090']

  - job_name: 'flink'
    static_configs:
      - targets: ['node1:8081']
```

### 5.2 告警规则

```yaml
# alert rules
groups:
  - name: hadoop_alerts
    rules:
      - alert: HDFSDataNodeDown
        expr: up{job="hadoop"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "HDFS DataNode down"
          
      - alert: HighDiskUsage
        expr: (1 - (node_filesystem_avail_bytes{mountpoint="/data"}/node_filesystem_size_bytes{mountpoint="/data"})) * 100 > 85
        for: 5m
        labels:
          severity: warning
```

## 6. 边界情况处理

### 6.1 数据倾斜

```bash
# 现象：某些节点负载明显高于其他节点
# 解决：
# 1. 调整bucket数量
# 2. 使用key加盐打散
# 3. 调整JVM内存
```

### 6.2 网络超时

```bash
# 现象：任务经常超时失败
# 解决：
# 1. 调整超时参数
# 2. 增加重试次数
# 3. 检查网络稳定性
```

### 6.3 磁盘空间不足

```bash
# 现象：写入失败，磁盘满
# 解决：
# 1. 清理历史数据
# 2. 调整日志保留策略
# 3. 扩容或挂载新磁盘
```
