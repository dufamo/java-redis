# Java Redis 实现

一个完整的Java Redis实现，包含Redis的核心功能，用于学习Redis内部原理。

## 项目特性

### 核心功能
- ✅ **数据结构**：SDS、RedisObject、List、Hash、Set、ZSet（跳表实现）
- ✅ **持久化**：RDB快照、AOF追加日志
- ✅ **主从复制**：全量复制 + 命令传播
- ✅ **哨兵模式**：S_DOWN/O_DOWN检测、故障转移
- ✅ **集群模式**：16384槽位、CRC16哈希、MOVED重定向
- ✅ **网络模型**：基于Netty的NIO服务器
- ✅ **命令支持**：100+ Redis命令
- ✅ **RESP协议**：完整的Redis序列化协议实现

### 技术特点
- **详细中文注释**：所有代码都有详细的中文注释，便于学习
- **模块化设计**：清晰的代码结构，易于理解和扩展
- **高性能**：跳表实现的ZSet、高效的内存管理
- **完整测试**：包含单元测试，确保功能正确性

## 目录结构

```
src/
├── main/
│   ├── java/com/learn/redis/
│   │   ├── cluster/        # 集群相关
│   │   ├── command/        # 命令执行器
│   │   ├── config/         # 配置管理
│   │   ├── data/          # 数据结构
│   │   ├── example/       # 示例代码
│   │   ├── network/       # 网络服务
│   │   ├── persistence/   # 持久化
│   │   ├── protocol/       # RESP协议
│   │   ├── replication/   # 主从复制
│   │   ├── sentinel/      # 哨兵
│   │   └── RedisServer.java  # 服务器启动类
│   └── resources/         # 资源文件
└── test/                  # 单元测试
```

## 核心模块说明

### 1. 数据结构
- **SDS**：简单动态字符串，Redis的基础字符串实现
- **RedisObject**：对象系统，支持类型检查和多态
- **RedisList**：列表实现，支持双向操作
- **RedisHash**：哈希表实现
- **RedisSet**：集合实现
- **RedisZSet**：有序集合，基于跳表实现
- **RedisDatabase**：数据库核心，管理键值对

### 2. 持久化
- **PersistenceManager**：RDB持久化，定期生成快照
- **AOFManager**：AOF持久化，记录写操作命令

### 3. 复制与高可用
- **ReplicationManager**：主从复制管理
- **SentinelManager**：哨兵高可用
- **ClusterManager**：集群管理

### 4. 网络与协议
- **RedisServer**：服务器启动类，主入口
- **NettyServer**：基于Netty的NIO服务器
- **RedisClientHandler**：客户端连接处理
- **RESPParser**：RESP协议解析
- **CommandExecutor**：命令执行器

## 快速开始

### 环境要求
- JDK 8+
- Maven 3.6+

### 构建项目
```bash
mvn clean package
```

### 启动服务器
```bash
# 使用默认配置启动（端口6379，16个数据库）
java -cp target/java-redis-1.0.0.jar com.learn.redis.RedisServer

# 自定义端口
java -cp target/java-redis-1.0.0.jar com.learn.redis.RedisServer --port 6380

# 启用AOF持久化
java -cp target/java-redis-1.0.0.jar com.learn.redis.RedisServer --aof-enabled true

# 配置从节点
java -cp target/java-redis-1.0.0.jar com.learn.redis.RedisServer --replicaof 127.0.0.1:6379
```

### 运行示例
```bash
java -cp target/java-redis-1.0.0.jar com.learn.redis.example.RedisExample
```

### 运行测试
```bash
mvn test
```

### 停止服务器
使用 `Ctrl+C` 优雅关闭服务器

## 学习指南

### 推荐学习顺序
1. **数据结构**：从SDS开始，了解Redis的基础数据结构
2. **协议**：学习RESP协议的实现
3. **命令**：了解命令执行流程
4. **持久化**：理解RDB和AOF的工作原理
5. **复制**：学习主从复制机制
6. **哨兵**：了解高可用实现
7. **集群**：学习分布式实现

### 重要概念
- **SDS**：Redis的字符串实现，比C字符串更高效
- **跳表**：ZSet的底层实现，提供O(logN)的操作复杂度
- **RESP协议**：Redis的序列化协议，简单高效
- **RDB/AOF**：两种持久化方式的原理和区别
- **主从复制**：数据同步机制
- **哨兵**：自动故障转移
- **集群**：数据分片和高可用

## 功能演示

### 字符串操作
```java
// 设置和获取字符串
redis.set("name", "Redis");
String value = redis.get("name"); // "Redis"

// 数值操作
redis.set("counter", "10");
redis.incr("counter"); // 11
```

### 列表操作
```java
// 列表操作
redis.lpush("list", "a", "b", "c");
redis.rpush("list", "d");
List<String> elements = redis.lrange("list", 0, -1); // ["c", "b", "a", "d"]
```

### 哈希操作
```java
// 哈希操作
redis.hset("user", "name", "Tom");
redis.hset("user", "age", "25");
String name = redis.hget("user", "name"); // "Tom"
```

### 集合操作
```java
// 集合操作
redis.sadd("set", "a", "b", "c");
boolean exists = redis.sismember("set", "a"); // true
```

### 有序集合操作
```java
// 有序集合操作
redis.zadd("zset", 1.0, "one");
redis.zadd("zset", 2.0, "two");
List<ZSetEntry> entries = redis.zrange("zset", 0, -1);
```

## 配置选项

```java
RedisConfig config = new RedisConfig();
config.setPort(6379);           // 端口
config.setDatabases(16);        // 数据库数量
config.setRdbEnabled(true);     // 启用RDB
config.setAofEnabled(false);    // 启用AOF
config.setReplicationEnabled(false); // 启用复制
config.setClusterEnabled(false);     // 启用集群
config.setSentinelEnabled(false);    // 启用哨兵
```

## 性能特性

- **内存使用**：优化的内存管理，减少内存碎片
- **速度**：基于Netty的NIO模型，支持高并发
- **可靠性**：完善的持久化机制
- **扩展性**：模块化设计，易于扩展

## 注意事项

- 本项目主要用于学习Redis原理，不是生产级实现
- 部分高级功能可能未完全实现
- 性能优化方面还有提升空间

## 贡献

欢迎提交Issue和Pull Request，一起完善这个项目！

## 许可证

MIT License
