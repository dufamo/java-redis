package com.learn.redis;

import com.learn.redis.data.*;
import org.junit.Test;
import org.junit.Before;

import static org.junit.Assert.*;

/**
 * RedisDataStructureTest - Redis数据结构单元测试
 * 
 * 【测试目的】
 * 验证Redis各种数据结构的正确性，包括：
 * - 字符串(String)操作
 * - 列表(List)操作
 * - 哈希(Hash)操作
 * - 集合(Set)操作
 * - 有序集合(ZSet)操作
 * - 键(Key)操作
 * - 数据库(Database)操作
 * - SDS字符串操作
 * 
 * 【运行方式】
 * mvn test -Dtest=RedisDataStructureTest
 * 
 * 【测试框架】
 * 使用JUnit 4进行单元测试
 * - @Before: 每个测试方法执行前运行，用于初始化
 * - @Test: 标记测试方法
 * - assertEquals: 验证期望值与实际值相等
 * - assertTrue/false: 验证条件为真/假
 * - assertNull: 验证对象为null
 * - assertNotNull: 验证对象不为null
 */
public class RedisDataStructureTest {
    
    /**
     * 数据库实例
     * 每个测试方法执行前都会重新创建
     */
    private RedisDatabase database;
    
    /**
     * 测试前初始化
     * 
     * 在每个测试方法执行前创建一个新的数据库实例
     * 确保测试之间相互独立，不受其他测试影响
     */
    @Before
    public void setUp() {
        database = new RedisDatabase(16);
    }
    
    /**
     * 测试字符串基本操作
     * 
     * 【测试内容】
     * 1. SET/GET: 基本设置和获取
     * 2. INCR/INCRBY/DECR/DECRBY: 数值增减
     * 3. APPEND: 追加字符串
     * 4. STRLEN: 获取字符串长度
     * 5. GETRANGE: 获取子串
     * 6. GETSET: 设置新值并返回旧值
     */
    @Test
    public void testStringOperations() {
        // 测试基本的SET/GET操作
        database.set("name", "redis");
        assertEquals("redis", database.get("name"));
        
        // 测试数值增减操作
        // INCR: 自增1
        // INCRBY: 增加指定值
        // DECR: 自减1
        // DECRBY: 减少指定值
        database.set("count", "10");
        assertEquals(Long.valueOf(11), database.incr("count"));
        assertEquals(Long.valueOf(21), database.incrBy("count", 10));
        assertEquals(Long.valueOf(20), database.decr("count"));
        assertEquals(Long.valueOf(10), database.decrBy("count", 10));
        
        // 测试APPEND追加操作
        // 返回追加后的字符串长度
        assertEquals(Long.valueOf(9), database.append("name", "-db"));
        assertEquals("redis-db", database.get("name"));
        
        // 测试STRLEN获取长度
        assertEquals(Long.valueOf(8), database.strlen("name"));
        
        // 测试GETRANGE获取子串
        // 正索引从0开始，负索引从-1开始（-1表示最后一个字符）
        database.set("hello", "Hello World");
        assertEquals("Hello", database.getRange("hello", 0, 4));
        assertEquals("World", database.getRange("hello", -5, -1));
        
        // 测试GETSET：设置新值并返回旧值
        String old = database.getSet("name", "new-value");
        assertEquals("redis-db", old);
        assertEquals("new-value", database.get("name"));
    }
    
    /**
     * 测试字符串过期操作
     * 
     * 【测试内容】
     * 1. SET with EX: 设置带过期时间的键
     * 2. EXPIRE: 设置过期时间
     * 3. PTTL: 获取剩余过期时间（毫秒）
     * 4. PERSIST: 移除过期时间
     * 5. TTL: 获取剩余过期时间（秒）
     */
    @Test
    public void testStringExpire() {
        // 设置带过期时间的键（1000毫秒）
        database.set("temp", "value", 1000);
        assertNotNull(database.get("temp"));
        
        // EXPIRE: 设置过期时间（秒）
        assertEquals(Long.valueOf(1), database.expire("temp", 1));
        
        // PTTL: 获取剩余过期时间（毫秒）
        // 应该大于0，表示键还未过期
        assertTrue(database.pttl("temp") > 0);
        
        // PERSIST: 移除过期时间
        assertEquals(Long.valueOf(1), database.persist("temp"));
        
        // TTL返回-1表示键存在但没有设置过期时间
        assertEquals(Long.valueOf(-1), database.ttl("temp"));
    }
    
    /**
     * 测试列表(List)操作
     * 
     * 【测试内容】
     * 1. LPUSH/RPUSH: 头部/尾部插入
     * 2. LINDEX: 按索引获取元素
     * 3. LPOP/RPOP: 头部/尾部弹出
     * 4. LLEN: 获取列表长度
     * 5. LRANGE: 获取范围元素
     * 6. LSET: 设置指定索引的元素
     * 7. LREM: 移除指定元素
     */
    @Test
    public void testListOperations() {
        // 创建List对象
        RedisObject listObj = RedisObject.createList();
        RedisList list = listObj.getValue();
        
        // LPUSH: 在头部插入元素，返回列表长度
        // 插入顺序：c, b, a（a在最前面）
        assertEquals(3, list.lpush("c", "b", "a"));
        
        // RPUSH: 在尾部插入元素
        assertEquals(4, list.rpush("d"));
        
        // LINDEX: 获取指定索引的元素
        // 正索引从0开始，负索引从-1开始
        assertEquals("a", list.lindex(0));   // 第一个元素
        assertEquals("d", list.lindex(-1));  // 最后一个元素
        
        // LPOP/RPOP: 从头部/尾部弹出元素
        assertEquals("a", list.lpop());
        assertEquals("d", list.rpop());
        
        // LLEN: 获取列表长度
        assertEquals(2, list.llen());
        
        // LRANGE: 获取指定范围的元素
        // 0表示第一个，-1表示最后一个
        java.util.List<String> range = list.lrange(0, -1);
        assertEquals(2, range.size());
        assertEquals("b", range.get(0));
        assertEquals("c", range.get(1));
        
        // LSET: 设置指定索引的元素
        assertEquals("OK", list.lset(0, "x"));
        assertEquals("x", list.lindex(0));
        
        // LREM: 移除指定数量的匹配元素
        list.rpush("x", "y", "x");
        assertEquals(2, list.lrem(2, "x"));  // 移除2个"x"
    }
    
    /**
     * 测试哈希(Hash)操作
     * 
     * 【测试内容】
     * 1. HSET/HGET: 设置/获取字段
     * 2. HEXISTS: 检查字段是否存在
     * 3. HLEN: 获取字段数量
     * 4. HINCRBY/HINCRBYFLOAT: 字段值增减
     * 5. HDEL: 删除字段
     */
    @Test
    public void testHashOperations() {
        // 创建Hash对象
        RedisObject hashObj = RedisObject.createHash();
        RedisHash hash = hashObj.getValue();
        
        // HSET: 设置字段值
        // 返回1表示新字段，返回0表示更新已有字段
        assertEquals(1, hash.hset("field1", "value1"));
        assertEquals(0, hash.hset("field1", "updated"));
        
        // HGET: 获取字段值
        assertEquals("updated", hash.hget("field1"));
        
        // HEXISTS: 检查字段是否存在
        assertTrue(hash.hexists("field1"));
        assertFalse(hash.hexists("field2"));
        
        // HLEN: 获取字段数量
        assertEquals(1, hash.hlen());
        
        // HINCRBY: 字段整数值增量
        assertEquals(1, hash.hset("count", "10"));
        assertEquals("15", hash.hincrby("count", 5));
        
        // HINCRBYFLOAT: 字段浮点数值增量
        assertEquals("15.5", hash.hincrbyfloat("count", 0.5));
        
        // HDEL: 删除字段
        assertEquals(1, hash.hdel("field1"));
        assertFalse(hash.hexists("field1"));
    }
    
    /**
     * 测试集合(Set)基本操作
     * 
     * 【测试内容】
     * 1. SADD: 添加元素
     * 2. SISMEMBER: 检查元素是否存在
     * 3. SCARD: 获取元素数量
     * 4. SMEMBERS: 获取所有元素
     * 5. SREM: 移除元素
     * 6. SPOP: 随机弹出元素
     */
    @Test
    public void testSetOperations() {
        // 创建Set对象
        RedisObject setObj = RedisObject.createSet();
        RedisSet set = setObj.getValue();
        
        // SADD: 添加元素，返回实际添加的数量
        // 重复元素不会被添加
        assertEquals(3, set.sadd("a", "b", "c"));
        assertEquals(0, set.sadd("a", "b"));  // 已存在，不添加
        
        // SISMEMBER: 检查元素是否在集合中
        assertTrue(set.sismember("a"));
        assertFalse(set.sismember("d"));
        
        // SCARD: 获取集合元素数量
        assertEquals(3, set.scard());
        
        // SMEMBERS: 获取所有元素
        java.util.Set<String> members = set.smembers();
        assertEquals(3, members.size());
        assertTrue(members.contains("a"));
        assertTrue(members.contains("b"));
        assertTrue(members.contains("c"));
        
        // SREM: 移除元素
        assertEquals(1, set.srem("a"));
        assertFalse(set.sismember("a"));
        
        // SPOP: 随机弹出元素
        String popped = set.spop();
        assertNotNull(popped);
    }
    
    /**
     * 测试集合运算操作
     * 
     * 【测试内容】
     * 1. SINTER: 交集运算
     * 2. SUNION: 并集运算
     * 3. SDIFF: 差集运算
     */
    @Test
    public void testSetOperations2() {
        // 创建两个集合
        RedisSet set1 = new RedisSet();
        RedisSet set2 = new RedisSet();
        
        set1.sadd("a", "b", "c");
        set2.sadd("b", "c", "d");
        
        // SINTER: 交集 - 同时存在于两个集合的元素
        java.util.Set<String> inter = RedisSet.sinter(set1, set2);
        assertEquals(2, inter.size());
        assertTrue(inter.contains("b"));
        assertTrue(inter.contains("c"));
        
        // SUNION: 并集 - 两个集合的所有元素
        java.util.Set<String> union = RedisSet.sunion(set1, set2);
        assertEquals(4, union.size());
        
        // SDIFF: 差集 - 在set1但不在set2的元素
        java.util.Set<String> diff = RedisSet.sdiff(set1, set2);
        assertEquals(1, diff.size());
        assertTrue(diff.contains("a"));
    }
    
    /**
     * 测试有序集合(ZSet)操作
     * 
     * 【测试内容】
     * 1. ZADD: 添加元素
     * 2. ZSCORE: 获取元素分数
     * 3. ZCARD: 获取元素数量
     * 4. ZRANK/ZREVRANK: 获取排名
     * 5. ZRANGE: 获取范围元素
     * 6. ZINCRBY: 增加分数
     * 7. ZRANGEBYSCORE: 按分数范围获取
     * 8. ZREM: 移除元素
     */
    @Test
    public void testZSetOperations() {
        // 创建ZSet对象
        RedisObject zsetObj = RedisObject.createZSet();
        RedisZSet zset = zsetObj.getValue();
        
        // ZADD: 添加元素，返回实际添加的数量
        assertEquals(1, zset.zadd(1.0, "one"));
        assertEquals(0, zset.zadd(1.0, "one"));  // 已存在，更新分数
        assertEquals(1, zset.zadd(2.0, "two"));
        assertEquals(1, zset.zadd(3.0, "three"));
        
        // ZSCORE: 获取元素分数
        assertEquals(Double.valueOf(1.0), zset.zscore("one"));
        
        // ZCARD: 获取元素数量
        assertEquals(3, zset.zcard());
        
        // ZRANK: 获取排名（按分数升序，从0开始）
        assertEquals(Long.valueOf(0), zset.zrank("one"));    // 分数最低
        assertEquals(Long.valueOf(2), zset.zrevrank("one")); // 降序排名
        
        // ZRANGE: 获取指定排名范围的元素
        java.util.List<RedisZSet.ZSetEntry> range = zset.zrange(0, 1);
        assertEquals(2, range.size());
        assertEquals("one", range.get(0).member);
        assertEquals("two", range.get(1).member);
        
        // ZINCRBY: 增加元素分数
        assertEquals(Double.valueOf(2.5), zset.zincrby("two", 0.5));
        assertEquals(Double.valueOf(2.5), zset.zscore("two"));
        
        // ZRANGEBYSCORE: 获取指定分数范围的元素
        java.util.List<RedisZSet.ZSetEntry> scoreRange = zset.zrangebyscore(1.0, 2.5, true, true);
        assertEquals(2, scoreRange.size());
        
        // ZREM: 移除元素
        assertEquals(1, zset.zrem("one"));
        assertNull(zset.zscore("one"));
    }
    
    /**
     * 测试键(Key)操作
     * 
     * 【测试内容】
     * 1. EXISTS: 检查键是否存在
     * 2. DEL: 删除键
     * 3. TYPE: 获取键类型
     * 4. KEYS: 查找匹配的键
     * 5. RENAME: 重命名键
     */
    @Test
    public void testKeyOperations() {
        // 设置一些测试数据
        database.set("key1", "value1");
        database.set("key2", "value2");
        database.set("prefix:a", "1");
        database.set("prefix:b", "2");
        
        // EXISTS: 检查键是否存在
        assertTrue(database.exists("key1"));
        assertFalse(database.exists("nonexistent"));
        
        // DEL: 删除键，返回实际删除的数量
        assertEquals(2, database.del("key1", "key2"));
        assertFalse(database.exists("key1"));
        assertFalse(database.exists("key2"));
        
        // TYPE: 获取键类型
        database.set("str", "hello");
        assertEquals("string", database.type("str"));
        
        // KEYS: 查找匹配的键（支持通配符）
        java.util.Set<String> keys = database.keys("prefix:*");
        assertEquals(2, keys.size());
        
        // RENAME: 重命名键
        assertEquals("OK", database.rename("prefix:a", "prefix:c"));
        assertTrue(database.exists("prefix:c"));
        assertFalse(database.exists("prefix:a"));
    }
    
    /**
     * 测试数据库(Database)操作
     * 
     * 【测试内容】
     * 1. DBSIZE: 获取键数量
     * 2. SELECT: 切换数据库
     * 3. FLUSHDB: 清空当前数据库
     */
    @Test
    public void testDatabaseOperations() {
        // 在数据库0设置键
        database.set("db0key", "value0");
        assertEquals(1, database.dbsize());
        
        // SELECT: 切换到数据库1
        database.select(1);
        assertEquals(1, database.getCurrentDb());
        
        // 数据库1是空的
        assertEquals(0, database.dbsize());
        
        // 在数据库1设置键
        database.set("db1key", "value1");
        assertEquals(1, database.dbsize());
        
        // 切换回数据库0
        database.select(0);
        assertEquals(1, database.dbsize());
        assertTrue(database.exists("db0key"));
        
        // FLUSHDB: 清空当前数据库
        database.flushdb();
        assertEquals(0, database.dbsize());
    }
    
    /**
     * 测试SDS(Simple Dynamic String)操作
     * 
     * 【测试内容】
     * 1. length(): 获取字符串长度
     * 2. append(): 追加字符串
     * 3. range(): 获取子串
     * 4. trim(): 去除指定字符
     * 5. dup(): 复制SDS
     * 6. clear(): 清空SDS
     */
    @Test
    public void testSDS() {
        // 创建SDS并测试length()
        SDS sds = new SDS("hello");
        assertEquals(5, sds.length());
        
        // append(): 追加字符串
        sds.append(" world");
        assertEquals("hello world", sds.toString());
        assertEquals(11, sds.length());
        
        // range(): 获取子串并修改原SDS
        sds.range(0, 4);
        assertEquals("hello", sds.toString());
        
        // trim(): 去除指定字符
        SDS sds2 = new SDS("  hello  ");
        sds2.trim(" ");
        assertEquals("hello", sds2.toString());
        
        // dup(): 复制SDS
        SDS sds3 = new SDS("test");
        SDS dup = sds3.dup();
        assertEquals(sds3.toString(), dup.toString());
        
        // clear(): 清空SDS
        sds3.clear();
        assertEquals(0, sds3.length());
        assertEquals(4, dup.length());  // 复制的SDS不受影响
    }
}
