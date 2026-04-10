package com.learn.redis.example;

import com.learn.redis.data.*;
import com.learn.redis.protocol.*;
import com.learn.redis.command.*;

import java.util.*;

/**
 * RedisExample - Redis实现功能演示示例
 * 
 * 【功能说明】
 * 本类演示了Java Redis实现的各种功能，包括：
 * 1. 字符串(String)操作
 * 2. 列表(List)操作
 * 3. 哈希(Hash)操作
 * 4. 集合(Set)操作
 * 5. 有序集合(Sorted Set)操作
 * 6. RESP协议编码
 * 7. 命令执行器使用
 * 
 * 【运行方式】
 * 直接运行main方法即可看到所有演示输出。
 * 
 * 【学习建议】
 * 1. 先阅读每个方法的注释，了解该数据结构的特点
 * 2. 对照输出结果，理解每个命令的作用
 * 3. 尝试修改代码，探索更多功能
 * 4. 结合源码实现，深入理解原理
 */
public class RedisExample {
    
    /**
     * 主入口方法
     * 
     * 依次调用各个演示方法，展示Redis的不同功能模块。
     * 
     * @param args 命令行参数（未使用）
     */
    public static void main(String[] args) {
        System.out.println("=== Java Redis 实现示例 ===\n");
        
        // 演示字符串操作
        demonstrateStringOperations();
        
        // 演示列表操作
        demonstrateListOperations();
        
        // 演示哈希操作
        demonstrateHashOperations();
        
        // 演示集合操作
        demonstrateSetOperations();
        
        // 演示有序集合操作
        demonstrateZSetOperations();
        
        // 演示RESP协议
        demonstrateRESPProtocol();
        
        // 演示命令执行器
        demonstrateCommandExecutor();
    }
    
    /**
     * 演示字符串(String)操作
     * 
     * 【String简介】
     * String是Redis最基本的数据类型，可以存储：
     * - 字符串
     * - 整数（自动转换）
     * - 浮点数（自动转换）
     * - 二进制数据（如图片）
     * 
     * 【最大大小】
     * 一个String类型的value最大可以是512MB
     * 
     * 【常用命令】
     * SET/GET: 设置/获取值
     * INCR/DECR: 自增/自减
     * APPEND: 追加字符串
     * GETRANGE: 获取子串
     * SETEX: 设置值并指定过期时间
     */
    private static void demonstrateStringOperations() {
        System.out.println("【String 操作示例】");
        
        // 创建数据库实例，16个数据库
        RedisDatabase db = new RedisDatabase(16);
        
        // 基本的SET/GET操作
        db.set("name", "Redis");
        System.out.println("SET name Redis -> OK");
        System.out.println("GET name -> " + db.get("name"));
        
        // 数值操作：INCR/INCRBY/DECR
        // Redis会自动将字符串解析为整数进行运算
        db.set("counter", "10");
        System.out.println("\nSET counter 10 -> OK");
        System.out.println("INCR counter -> " + db.incr("counter"));  // 11
        System.out.println("INCRBY counter 5 -> " + db.incrBy("counter", 5));  // 16
        System.out.println("DECR counter -> " + db.decr("counter"));  // 15
        
        // 字符串操作：GETRANGE/APPEND
        db.set("hello", "Hello World");
        System.out.println("\nSET hello \"Hello World\" -> OK");
        System.out.println("GETRANGE hello 0 4 -> " + db.getRange("hello", 0, 4));  // "Hello"
        System.out.println("APPEND hello \"!\" -> " + db.append("hello", "!"));  // 返回新长度12
        System.out.println("GET hello -> " + db.get("hello"));  // "Hello World!"
        
        // 过期时间操作
        // SET key value EX seconds：设置值并指定过期时间
        db.set("temp", "value", 5000);  // 5000毫秒后过期
        System.out.println("\nSET temp value EX 5 -> OK");
        System.out.println("TTL temp -> " + db.ttl("temp") + "秒");  // 显示剩余过期时间
        
        System.out.println();
    }
    
    /**
     * 演示列表(List)操作
     * 
     * 【List简介】
     * List是一个简单的字符串列表，按照插入顺序排序。
     * 可以在列表的两端进行插入(PUSH)和弹出(POP)操作。
     * 
     * 【底层实现】
     * Redis 3.2之后使用quicklist（快速列表）：
     * - quicklist = ziplist + linkedlist的组合
     * - 兼顾内存效率和操作性能
     * 
     * 【时间复杂度】
     * LPUSH/RPUSH: O(1) - 头部/尾部插入
     * LPOP/RPOP: O(1) - 头部/尾部删除
     * LINDEX: O(N) - 按索引访问
     * LRANGE: O(N) - 范围查询
     * LLEN: O(1) - 获取长度
     * 
     * 【应用场景】
     * - 消息队列
     * - 最新文章列表
     * - 时间线
     */
    private static void demonstrateListOperations() {
        System.out.println("【List 操作示例】");
        
        // 创建List对象
        RedisObject listObj = RedisObject.createList();
        RedisList list = listObj.getValue();
        
        // RPUSH: 在列表尾部插入元素
        list.rpush("a", "b", "c");
        System.out.println("RPUSH mylist a b c -> " + list.llen());
        
        // LPUSH: 在列表头部插入元素
        list.lpush("x");
        System.out.println("LPUSH mylist x -> " + list.llen());
        
        // LRANGE: 获取指定范围的元素
        // 0表示第一个元素，-1表示最后一个元素
        System.out.println("LRANGE mylist 0 -1 -> " + list.lrange(0, -1));
        
        // LINDEX: 获取指定索引的元素
        System.out.println("LINDEX mylist 0 -> " + list.lindex(0));  // "x"
        System.out.println("LINDEX mylist -1 -> " + list.lindex(-1));  // "c"
        
        // LPOP/RPOP: 从头部/尾部弹出元素
        System.out.println("LPOP mylist -> " + list.lpop());  // "x"
        System.out.println("RPOP mylist -> " + list.rpop());  // "c"
        System.out.println("LLEN mylist -> " + list.llen());  // 2
        
        System.out.println();
    }
    
    /**
     * 演示哈希(Hash)操作
     * 
     * 【Hash简介】
     * Hash是一个键值对集合，适合存储对象。
     * 每个Hash可以存储2^32-1个键值对。
     * 
     * 【底层实现】
     * Redis使用两种编码：
     * 1. ziplist(压缩列表)：元素少时使用，节省内存
     * 2. hashtable(哈希表)：元素多时使用，查找效率O(1)
     * 
     * 【与String存储对象的对比】
     * 使用String存储用户信息：
     * SET user:1:name "Tom"
     * SET user:1:age "25"
     * SET user:1:city "Beijing"
     * 
     * 使用Hash存储用户信息：
     * HMSET user:1 name "Tom" age "25" city "Beijing"
     * 
     * Hash方式更节省内存，且操作更方便。
     * 
     * 【应用场景】
     * - 存储用户信息
     * - 存储商品详情
     * - 存储配置信息
     */
    private static void demonstrateHashOperations() {
        System.out.println("【Hash 操作示例】");
        
        // 创建Hash对象
        RedisObject hashObj = RedisObject.createHash();
        RedisHash hash = hashObj.getValue();
        
        // HSET: 设置哈希字段的值
        // 返回值：1表示新字段，0表示更新已有字段
        hash.hset("name", "Redis");
        hash.hset("version", "7.0");
        hash.hset("port", "6379");
        System.out.println("HSET info name Redis -> " + hash.hset("name", "Redis"));
        System.out.println("HSET info version 7.0 -> " + hash.hset("version", "7.0"));
        System.out.println("HSET info port 6379 -> " + hash.hset("port", "6379"));
        
        // HGET: 获取哈希字段的值
        System.out.println("\nHGET info name -> " + hash.hget("name"));
        
        // HGETALL: 获取所有字段和值
        System.out.println("HGETALL info -> " + hash.hgetall());
        
        // HKEYS: 获取所有字段名
        System.out.println("HKEYS info -> " + hash.hkeys());
        
        // HVALS: 获取所有字段值
        System.out.println("HVALS info -> " + hash.hvals());
        
        // HLEN: 获取字段数量
        System.out.println("HLEN info -> " + hash.hlen());
        
        // HINCRBY: 哈希字段整数值增量
        hash.hset("count", "100");
        System.out.println("\nHSET info count 100 -> " + hash.hset("count", "100"));
        System.out.println("HINCRBY info count 50 -> " + hash.hincrby("count", 50));  // 150
        
        System.out.println();
    }
    
    /**
     * 演示集合(Set)操作
     * 
     * 【Set简介】
     * Set是一个无序的字符串集合，集合中的元素是唯一的。
     * Redis支持集合间的交集、并集、差集运算。
     * 
     * 【底层实现】
     * 1. intset(整数集合)：所有元素都是整数且数量较少时
     * 2. hashtable(哈希表)：其他情况
     * 
     * 【时间复杂度】
     * SADD: O(1) 单个元素
     * SISMEMBER: O(1)
     * SINTER/SUNION/SDIFF: O(N*M)
     * 
     * 【应用场景】
     * - 标签系统
     * - 共同好友
     * - 抽奖系统
     * - 社交网络关注关系
     */
    private static void demonstrateSetOperations() {
        System.out.println("【Set 操作示例】");
        
        // 创建第一个集合
        RedisSet set1 = new RedisSet();
        set1.sadd("a", "b", "c", "d");
        System.out.println("SADD set1 a b c d -> " + set1.scard());
        
        // 创建第二个集合
        RedisSet set2 = new RedisSet();
        set2.sadd("c", "d", "e", "f");
        System.out.println("SADD set2 c d e f -> " + set2.scard());
        
        // SISMEMBER: 检查元素是否在集合中
        System.out.println("\nSISMEMBER set1 a -> " + set1.sismember("a"));  // true
        System.out.println("SISMEMBER set1 e -> " + set1.sismember("e"));  // false
        
        // 集合运算
        // SINTER: 交集 - 同时存在于两个集合的元素
        System.out.println("\nSINTER set1 set2 -> " + RedisSet.sinter(set1, set2));  // [c, d]
        
        // SUNION: 并集 - 两个集合的所有元素
        System.out.println("SUNION set1 set2 -> " + RedisSet.sunion(set1, set2));  // [a, b, c, d, e, f]
        
        // SDIFF: 差集 - 在set1但不在set2的元素
        System.out.println("SDIFF set1 set2 -> " + RedisSet.sdiff(set1, set2));  // [a, b]
        
        // SPOP: 随机弹出元素
        System.out.println("\nSPOP set1 -> " + set1.spop());
        System.out.println("SCARD set1 -> " + set1.scard());  // 弹出后数量减1
        
        System.out.println();
    }
    
    /**
     * 演示有序集合(Sorted Set)操作
     * 
     * 【ZSet简介】
     * Sorted Set是一个有序的字符串集合，每个元素关联一个分数(score)。
     * Redis根据分数对元素进行排序。
     * 
     * 【底层实现】
     * Redis使用两个数据结构：
     * 1. 哈希表：存储member -> score映射，O(1)查询分数
     * 2. 跳表：存储score -> member有序映射，支持范围查询
     * 
     * 【为什么使用跳表而不是红黑树？】
     * 1. 实现简单：跳表比红黑树更容易实现和维护
     * 2. 内存占用：跳表每个节点平均只需要1.33个指针
     * 3. 范围查询：跳表更适合范围查询
     * 4. 并发友好：跳表更容易实现无锁并发操作
     * 
     * 【时间复杂度】
     * ZADD: O(logN)
     * ZREM: O(logN)
     * ZSCORE: O(1)
     * ZRANK: O(logN)
     * ZRANGE: O(logN + M)
     * 
     * 【应用场景】
     * - 排行榜
     * - 带权重的消息队列
     * - 时间轴
     */
    private static void demonstrateZSetOperations() {
        System.out.println("【Sorted Set 操作示例】");
        
        // 创建ZSet对象
        RedisObject zsetObj = RedisObject.createZSet();
        RedisZSet zset = zsetObj.getValue();
        
        // ZADD: 添加元素，指定分数
        zset.zadd(1.0, "one");
        zset.zadd(2.0, "two");
        zset.zadd(3.0, "three");
        zset.zadd(4.0, "four");
        System.out.println("ZADD zset 1 one 2 two 3 three 4 four -> 4");
        
        // ZRANGE: 获取指定排名范围的元素
        // 0表示排名第一，-1表示最后一名
        System.out.println("\nZRANGE zset 0 -1 -> ");
        List<RedisZSet.ZSetEntry> range = zset.zrange(0, -1);
        for (RedisZSet.ZSetEntry entry : range) {
            System.out.println("  " + entry.member + " (score: " + entry.score + ")");
        }
        
        // ZSCORE: 获取元素的分数
        System.out.println("\nZSCORE zset two -> " + zset.zscore("two"));  // 2.0
        
        // ZRANK: 获取元素的排名（从0开始，按分数升序）
        System.out.println("ZRANK zset three -> " + zset.zrank("three"));  // 2
        
        // ZREVRANK: 获取元素的排名（按分数降序）
        System.out.println("ZREVRANK zset three -> " + zset.zrevrank("three"));  // 1
        
        // ZINCRBY: 增加元素的分数
        System.out.println("\nZINCRBY zset 0.5 two -> " + zset.zincrby(0.5, "two"));  // 2.5
        System.out.println("ZSCORE zset two -> " + zset.zscore("two"));  // 2.5
        
        // ZRANGEBYSCORE: 获取指定分数范围的元素
        System.out.println("\nZRANGEBYSCORE zset 1.5 3.0 -> ");
        List<RedisZSet.ZSetEntry> scoreRange = zset.zrangebyscore(1.5, 3.0, true, true);
        for (RedisZSet.ZSetEntry entry : scoreRange) {
            System.out.println("  " + entry.member + " (score: " + entry.score + ")");
        }
        
        System.out.println();
    }
    
    /**
     * 演示RESP协议
     * 
     * 【RESP协议简介】
     * RESP (REdis Serialization Protocol) 是Redis使用的序列化协议。
     * 它是一种简单、高效、可读性强的二进制安全协议。
     * 
     * 【数据类型】
     * 1. Simple String（简单字符串）：+OK\r\n
     * 2. Error（错误）：-ERR message\r\n
     * 3. Integer（整数）：:1000\r\n
     * 4. Bulk String（批量字符串）：$5\r\nhello\r\n
     * 5. Array（数组）：*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
     * 
     * 【协议特点】
     * - 简单：易于实现和调试
     * - 高效：使用前缀标识类型，解析速度快
     * - 二进制安全：可以传输任意二进制数据
     * - 可读：纯文本协议，便于调试
     */
    private static void demonstrateRESPProtocol() {
        System.out.println("【RESP 协议示例】");
        
        // Simple String: 简单字符串，用于状态回复
        // 格式：+OK\r\n
        System.out.println("Simple String: " + new String(RESPParser.encodeSimpleString("OK")));
        
        // Error: 错误消息
        // 格式：-ERR message\r\n
        System.out.println("Error: " + new String(RESPParser.encodeError("ERR unknown command")));
        
        // Integer: 整数
        // 格式：:1000\r\n
        System.out.println("Integer: " + new String(RESPParser.encodeInteger(1000)));
        
        // Bulk String: 批量字符串，可以包含任意二进制数据
        // 格式：$11\r\nhello world\r\n
        System.out.println("Bulk String: " + new String(RESPParser.encodeBulkString("hello world")));
        
        // Array: 数组，可以包含多个元素
        // 格式：*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
        List<RESPObject> array = new ArrayList<>();
        array.add(new RESPObject(RESPObject.Type.BULK_STRING, "SET"));
        array.add(new RESPObject(RESPObject.Type.BULK_STRING, "key"));
        array.add(new RESPObject(RESPObject.Type.BULK_STRING, "value"));
        System.out.println("Array: " + new String(RESPParser.encodeArray(array)));
        
        System.out.println();
    }
    
    /**
     * 演示命令执行器
     * 
     * 【CommandExecutor简介】
     * CommandExecutor是Redis服务器的核心组件，负责：
     * 1. 解析客户端发送的命令
     * 2. 执行相应的操作
     * 3. 返回RESP格式的响应
     * 
     * 【命令处理流程】
     * 1. 从RESPObject中提取命令名称和参数
     * 2. 根据命令名称路由到对应的处理方法
     * 3. 执行命令逻辑
     * 4. 将结果编码为RESP格式返回
     * 
     * 【支持的命令】
     * 本实现支持100多个Redis命令，包括：
     * - 字符串命令：SET, GET, INCR, DECR等
     * - 列表命令：LPUSH, RPUSH, LPOP, RPOP等
     * - 哈希命令：HSET, HGET, HGETALL等
     * - 集合命令：SADD, SREM, SINTER等
     * - 有序集合命令：ZADD, ZRANGE, ZSCORE等
     */
    private static void demonstrateCommandExecutor() {
        System.out.println("【命令执行器示例】");
        
        // 创建数据库和命令执行器
        RedisDatabase db = new RedisDatabase(16);
        CommandExecutor executor = new CommandExecutor(db);
        
        // 构建SET命令
        // RESP格式：*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n
        List<RESPObject> setCmd = new ArrayList<>();
        setCmd.add(new RESPObject(RESPObject.Type.BULK_STRING, "SET"));
        setCmd.add(new RESPObject(RESPObject.Type.BULK_STRING, "mykey"));
        setCmd.add(new RESPObject(RESPObject.Type.BULK_STRING, "myvalue"));
        RESPObject setCommand = new RESPObject(RESPObject.Type.ARRAY, setCmd);
        
        // 执行SET命令
        byte[] result = executor.execute(setCommand);
        System.out.println("SET mykey myvalue -> " + new String(result).trim());
        
        // 构建GET命令
        List<RESPObject> getCmd = new ArrayList<>();
        getCmd.add(new RESPObject(RESPObject.Type.BULK_STRING, "GET"));
        getCmd.add(new RESPObject(RESPObject.Type.BULK_STRING, "mykey"));
        RESPObject getCommand = new RESPObject(RESPObject.Type.ARRAY, getCmd);
        
        // 执行GET命令
        result = executor.execute(getCommand);
        System.out.println("GET mykey -> " + new String(result).trim());
        
        // 构建INCR命令
        List<RESPObject> incrCmd = new ArrayList<>();
        incrCmd.add(new RESPObject(RESPObject.Type.BULK_STRING, "INCR"));
        incrCmd.add(new RESPObject(RESPObject.Type.BULK_STRING, "counter"));
        RESPObject incrCommand = new RESPObject(RESPObject.Type.ARRAY, incrCmd);
        
        // 执行多次INCR命令
        executor.execute(incrCommand);
        executor.execute(incrCommand);
        executor.execute(incrCommand);
        
        // 获取计数器值
        List<RESPObject> getCounter = new ArrayList<>();
        getCounter.add(new RESPObject(RESPObject.Type.BULK_STRING, "GET"));
        getCounter.add(new RESPObject(RESPObject.Type.BULK_STRING, "counter"));
        result = executor.execute(new RESPObject(RESPObject.Type.ARRAY, getCounter));
        System.out.println("INCR counter x3, GET counter -> " + new String(result).trim());
        
        System.out.println();
        
        System.out.println("=== 示例完成 ===");
    }
}
