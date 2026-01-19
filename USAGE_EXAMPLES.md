# BlockTransactionPrinter 并发处理使用示例

## 基本用法

### 1. 使用默认线程数（CPU 核心数 * 2）
```bash
java -cp "build/libs/*" org.tron.program.BlockTransactionPrinter 1000 1100 -f trigger
```

### 2. 指定线程数
```bash
# 使用 4 个线程
java -cp "build/libs/*" org.tron.program.BlockTransactionPrinter 1000 1100 -f trigger -threads 4

# 使用 8 个线程
java -cp "build/libs/*" org.tron.program.BlockTransactionPrinter 1000 1100 -f trigger -threads 8
```

### 3. 结合其他参数
```bash
# 指定配置文件和数据目录
java -cp "build/libs/*" org.tron.program.BlockTransactionPrinter 1000 1100 \
  -c main_net_config.conf \
  -d /tron/light/ \
  -f trigger \
  -threads 8

# 结合 Kafka 输出
java -cp "build/libs/*" org.tron.program.BlockTransactionPrinter 1000 1100 \
  -f trigger \
  -threads 8 \
  -kb localhost:9092 \
  -kt tron-transactions
```

## 性能优化建议

### 1. 线程数选择

#### CPU 密集型任务
```bash
# 线程数 = CPU 核心数
java -cp "build/libs/*" org.tron.program.BlockTransactionPrinter 1000 1100 -f trigger -threads 4
```

#### I/O 密集型任务
```bash
# 线程数 = CPU 核心数 * 2
java -cp "build/libs/*" org.tron.program.BlockTransactionPrinter 1000 1100 -f trigger -threads 8
```

#### 高并发场景
```bash
# 可以尝试更多线程
java -cp "build/libs/*" org.tron.program.BlockTransactionPrinter 1000 1100 -f trigger -threads 16
```

### 2. 内存配置

#### 增加 JVM 堆内存
```bash
java -Xmx4g -Xms2g -cp "build/libs/*" org.tron.program.BlockTransactionPrinter 1000 1100 -f trigger -threads 8
```

#### 优化垃圾回收
```bash
java -Xmx4g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 \
  -cp "build/libs/*" org.tron.program.BlockTransactionPrinter 1000 1100 -f trigger -threads 8
```

## 不同场景的使用示例

### 1. 小规模测试（单线程，确保输出顺序）
```bash
java -cp "build/libs/*" org.tron.program.BlockTransactionPrinter 1000 1010 -f trigger -threads 1
```

### 2. 中等规模处理
```bash
java -cp "build/libs/*" org.tron.program.BlockTransactionPrinter 1000 2000 -f trigger -threads 4
```

### 3. 大规模批量处理
```bash
java -cp "build/libs/*" org.tron.program.BlockTransactionPrinter 1000 10000 -f trigger -threads 16
```

### 4. 实时数据流处理（结合 Kafka）
```bash
java -cp "build/libs/*" org.tron.program.BlockTransactionPrinter 73592989 73593000 \
  -f trigger \
  -threads 8 \
  -kb localhost:9092,broker2:9092 \
  -kt tron-transactions
```

## 监控和调试

### 1. 启用详细日志
```bash
java -Dlogging.level.org.tron.program=DEBUG \
  -cp "build/libs/*" org.tron.program.BlockTransactionPrinter 1000 1100 -f trigger -threads 8
```

### 2. 监控 JVM 性能
```bash
java -XX:+PrintGCDetails -XX:+PrintGCTimeStamps \
  -cp "build/libs/*" org.tron.program.BlockTransactionPrinter 1000 1100 -f trigger -threads 8
```

### 3. 使用 JProfiler 或类似工具
```bash
java -agentpath:/path/to/jprofiler/bin/linux-x64/libjprofilerti.so=port=8849 \
  -cp "build/libs/*" org.tron.program.BlockTransactionPrinter 1000 1100 -f trigger -threads 8
```

## 性能测试脚本

### 运行性能测试
```bash
# 使用提供的测试脚本
./test_concurrent_performance.sh

# 或者手动测试不同配置
for threads in 1 2 4 8 16; do
  echo "Testing with $threads threads..."
  time java -cp "build/libs/*" org.tron.program.BlockTransactionPrinter 1000 1100 -f trigger -threads $threads
done
```

## 故障排除

### 1. 内存不足
```bash
# 增加堆内存
java -Xmx8g -cp "build/libs/*" org.tron.program.BlockTransactionPrinter 1000 1100 -f trigger -threads 8
```

### 2. 线程池超时
```bash
# 减少线程数
java -cp "build/libs/*" org.tron.program.BlockTransactionPrinter 1000 1100 -f trigger -threads 4
```

### 3. 数据库连接问题
```bash
# 使用单线程避免连接池问题
java -cp "build/libs/*" org.tron.program.BlockTransactionPrinter 1000 1100 -f trigger -threads 1
```

## 最佳实践

### 1. 生产环境配置
```bash
# 推荐的生产环境配置
java -Xmx4g -Xms4g -XX:+UseG1GC \
  -cp "build/libs/*" org.tron.program.BlockTransactionPrinter 1000 10000 \
  -c production_config.conf \
  -d /data/tron/ \
  -f trigger \
  -threads 8 \
  -kb kafka1:9092,kafka2:9092,kafka3:9092 \
  -kt tron-transactions
```

### 2. 开发环境配置
```bash
# 开发环境快速测试
java -cp "build/libs/*" org.tron.program.BlockTransactionPrinter 1000 1010 \
  -f trigger \
  -threads 2
```

### 3. 调试配置
```bash
# 调试模式（单线程，详细输出）
java -Dlogging.level.root=DEBUG \
  -cp "build/libs/*" org.tron.program.BlockTransactionPrinter 1000 1005 \
  -f both \
  -threads 1
```

## 性能预期

基于测试，您可以期待以下性能提升：

- **2 线程**: 相比单线程提升 50-80%
- **4 线程**: 相比单线程提升 150-250%
- **8 线程**: 相比单线程提升 300-500%
- **16 线程**: 提升效果取决于硬件和 I/O 性能

注意：实际性能提升取决于：
- CPU 核心数
- 内存大小
- 磁盘 I/O 性能
- 网络延迟（如果使用 Kafka）
- 区块中交易的复杂度
