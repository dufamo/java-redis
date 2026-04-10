package com.learn.redis.data;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * RedisDatabase - Redis数据库核心实现
 * 
 * 【数据库简介】
 * Redis服务器默认创建16个数据库(编号0-15)
 * 客户端可以通过SELECT命令切换数据库
 * 每个数据库是独立的，数据不共享
 * 
 * 【数据库结构】
 * Redis使用字典(dict)存储所有键值对:
 * - 键(key): 字符串，使用SDS存储
 * - 值(value): RedisObject对象
 * 
 * 【过期策略】
 * Redis使用两种过期策略:
 * 1. 惰性删除(Lazy Expiration)
 *    - 访问键时检查是否过期
 *    - 过期则删除
 *    - 优点: CPU友好
 *    - 缺点: 过期键可能长期占用内存
 * 
 * 2. 定期删除(Periodic Expiration)
 *    - 定期随机检查部分键
 *    - 删除过期的键
 *    - 优点: 平衡CPU和内存
 *    - 缺点: 实现复杂
 * 
 * 【内存淘汰策略】
 * 当内存不足时，Redis提供多种淘汰策略:
 * - noeviction: 不淘汰，返回错误
 * - allkeys-lru: 从所有键中淘汰最少使用的
 * - volatile-lru: 从设置了过期时间的键中淘汰最少使用的
 * - allkeys-random: 随机淘汰
 * - volatile-random: 从设置了过期时间的键中随机淘汰
 * - volatile-ttl: 淘汰即将过期的键
 */
public class RedisDatabase {
    
    /**
     * 数据库数组
     * 
     * 每个元素是一个字典，存储该数据库的所有键值对
     * 使用ConcurrentHashMap保证线程安全
     */
    private final List<Map<String, RedisObject>> databases;
    
    /**
     * 当前选中的数据库索引
     */
    private int currentDb;
    
    /**
     * 过期键清理定时器
     * 
     * 定期执行过期键清理任务
     */
    private final ScheduledExecutorService expireExecutor;
    
    /**
     * 构造函数
     * 
     * @param dbCount 数据库数量
     */
    public RedisDatabase(int dbCount) {
        this.databases = new ArrayList<>();
        for (int i = 0; i < dbCount; i++) {
            // 使用ConcurrentHashMap保证线程安全
            databases.add(new ConcurrentHashMap<>());
        }
        this.currentDb = 0;
        this.expireExecutor = Executors.newSingleThreadScheduledExecutor();
        startExpireTask();
    }
    
    /**
     * 启动过期键定期清理任务
     * 
     * 【实现细节】
     * 每100毫秒执行一次清理:
     * 1. 遍历所有数据库
     * 2. 检查并删除过期键
     * 
     * Redis的实现更复杂:
     * - 每次随机抽查一部分键
     * - 限制每次清理的时间
     * - 自适应调整清理频率
     */
    private void startExpireTask() {
        expireExecutor.scheduleAtFixedRate(() -> {
            for (Map<String, RedisObject> db : databases) {
                Iterator<Map.Entry<String, RedisObject>> it = db.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<String, RedisObject> entry = it.next();
                    if (entry.getValue().isExpired()) {
                        it.remove();
                    }
                }
            }
        }, 100, 100, TimeUnit.MILLISECONDS);
    }
    
    /**
     * SELECT - 切换数据库
     * 
     * @param index 数据库索引
     */
    public void select(int index) {
        if (index >= 0 && index < databases.size()) {
            currentDb = index;
        }
    }
    
    /**
     * 获取当前数据库索引
     */
    public int getCurrentDb() {
        return currentDb;
    }
    
    /**
     * 获取数据库数量
     */
    public int getDbCount() {
        return databases.size();
    }
    
    /**
     * 获取当前数据库
     */
    private Map<String, RedisObject> current() {
        return databases.get(currentDb);
    }
    
    // ==================== String 操作 ====================
    
    /**
     * SET - 设置键值对
     * 
     * @param key   键
     * @param value 值
     * @return "OK"
     */
    public String set(String key, String value) {
        RedisObject obj = RedisObject.createString(value);
        current().put(key, obj);
        return "OK";
    }
    
    /**
     * SET - 设置键值对(带过期时间)
     * 
     * @param key      键
     * @param value    值
     * @param expireMs 过期时间(毫秒)
     * @return "OK"
     */
    public String set(String key, String value, long expireMs) {
        RedisObject obj = RedisObject.createString(value);
        obj.setExpireTime(System.currentTimeMillis() + expireMs);
        current().put(key, obj);
        return "OK";
    }
    
    /**
     * SETEX - 设置键值对并指定过期时间(秒)
     */
    public String setex(String key, long seconds, String value) {
        return set(key, value, seconds * 1000);
    }
    
    /**
     * SETNX - 仅当键不存在时设置
     * 
     * NX = Not eXists
     * 常用于实现分布式锁
     */
    public String setnx(String key, String value) {
        if (current().containsKey(key)) {
            return null;
        }
        set(key, value);
        return "OK";
    }
    
    /**
     * GET - 获取键的值
     * 
     * 【实现细节】
     * 1. 查找键
     * 2. 惰性删除检查：如果过期则返回null
     * 3. 返回值
     */
    public String get(String key) {
        RedisObject obj = current().get(key);
        if (obj == null || obj.isExpired()) {
            return null;
        }
        return obj.getStringValue();
    }
    
    /**
     * INCR - 键的值自增1
     * 
     * 如果键不存在，初始值为0再自增
     */
    public Long incr(String key) {
        return incrBy(key, 1);
    }
    
    /**
     * INCRBY - 键的值增加指定数量
     */
    public Long incrBy(String key, long increment) {
        RedisObject obj = current().get(key);
        if (obj == null) {
            set(key, String.valueOf(increment));
            return increment;
        }
        if (obj.isExpired()) {
            current().remove(key);
            set(key, String.valueOf(increment));
            return increment;
        }
        try {
            long value = Long.parseLong(obj.getStringValue());
            value += increment;
            current().put(key, RedisObject.createString(String.valueOf(value)));
            return value;
        } catch (NumberFormatException e) {
            return null;
        }
    }
    
    /**
     * DECR - 键的值自减1
     */
    public Long decr(String key) {
        return incrBy(key, -1);
    }
    
    /**
     * DECRBY - 键的值减少指定数量
     */
    public Long decrBy(String key, long decrement) {
        return incrBy(key, -decrement);
    }
    
    /**
     * APPEND - 追加字符串
     */
    public Long append(String key, String value) {
        RedisObject obj = current().get(key);
        if (obj == null) {
            set(key, value);
            return (long) value.length();
        }
        if (obj.isExpired()) {
            current().remove(key);
            set(key, value);
            return (long) value.length();
        }
        String newValue = obj.getStringValue() + value;
        current().put(key, RedisObject.createString(newValue));
        return (long) newValue.length();
    }
    
    /**
     * GETRANGE - 获取字符串指定范围的子串
     */
    public String getRange(String key, long start, long end) {
        String value = get(key);
        if (value == null) return "";
        if (start < 0) start = value.length() + start;
        if (end < 0) end = value.length() + end;
        if (start < 0) start = 0;
        if (end >= value.length()) end = value.length() - 1;
        if (start > end) return "";
        return value.substring((int) start, (int) end + 1);
    }
    
    /**
     * SETRANGE - 替换字符串指定位置开始的内容
     */
    public Long setRange(String key, long offset, String value) {
        RedisObject obj = current().get(key);
        if (obj == null) {
            StringBuilder sb = new StringBuilder();
            for (long i = 0; i < offset; i++) {
                sb.append('\0');
            }
            sb.append(value);
            set(key, sb.toString());
            return (long) sb.length();
        }
        String str = obj.getStringValue();
        StringBuilder sb = new StringBuilder(str);
        while (sb.length() < offset) {
            sb.append('\0');
        }
        sb.replace((int) offset, (int) (offset + value.length()), value);
        current().put(key, RedisObject.createString(sb.toString()));
        return (long) sb.length();
    }
    
    /**
     * GETSET - 设置新值并返回旧值
     */
    public String getSet(String key, String value) {
        String oldValue = get(key);
        set(key, value);
        return oldValue;
    }
    
    /**
     * STRLEN - 获取字符串长度
     */
    public Long strlen(String key) {
        String value = get(key);
        return value == null ? 0L : (long) value.length();
    }
    
    // ==================== 键操作 ====================
    
    /**
     * EXISTS - 检查键是否存在
     * 
     * 包含惰性删除检查
     */
    public boolean exists(String key) {
        RedisObject obj = current().get(key);
        if (obj == null) return false;
        if (obj.isExpired()) {
            current().remove(key);
            return false;
        }
        return true;
    }
    
    /**
     * DEL - 删除键
     */
    public long del(String... keys) {
        long deleted = 0;
        for (String key : keys) {
            if (current().remove(key) != null) {
                deleted++;
            }
        }
        return deleted;
    }
    
    /**
     * EXPIRE - 设置键的过期时间(秒)
     */
    public long expire(String key, long seconds) {
        return pexpire(key, seconds * 1000);
    }
    
    /**
     * PEXPIRE - 设置键的过期时间(毫秒)
     */
    public long pexpire(String key, long milliseconds) {
        RedisObject obj = current().get(key);
        if (obj == null || obj.isExpired()) return 0;
        obj.setExpireTime(System.currentTimeMillis() + milliseconds);
        return 1;
    }
    
    /**
     * EXPIREAT - 设置键的过期时间戳(秒)
     */
    public long expireAt(String key, long timestamp) {
        return pexpireAt(key, timestamp * 1000);
    }
    
    /**
     * PEXPIREAT - 设置键的过期时间戳(毫秒)
     */
    public long pexpireAt(String key, long timestamp) {
        RedisObject obj = current().get(key);
        if (obj == null || obj.isExpired()) return 0;
        obj.setExpireTime(timestamp);
        return 1;
    }
    
    /**
     * PERSIST - 移除键的过期时间
     */
    public long persist(String key) {
        RedisObject obj = current().get(key);
        if (obj == null || !obj.hasExpire()) return 0;
        obj.setExpireTime(-1);
        return 1;
    }
    
    /**
     * TTL - 获取键的剩余生存时间(秒)
     * 
     * 返回值:
     * - 正数: 剩余秒数
     * - -1: 永不过期
     * - -2: 键不存在
     */
    public Long ttl(String key) {
        return pttl(key) / 1000;
    }
    
    /**
     * PTTL - 获取键的剩余生存时间(毫秒)
     */
    public Long pttl(String key) {
        RedisObject obj = current().get(key);
        if (obj == null) return -2L;
        if (!obj.hasExpire()) return -1L;
        long remaining = obj.getExpireTime() - System.currentTimeMillis();
        return remaining > 0 ? remaining : -2L;
    }
    
    /**
     * TYPE - 获取键的类型
     */
    public String type(String key) {
        RedisObject obj = current().get(key);
        if (obj == null || obj.isExpired()) return "none";
        return obj.getType().name().toLowerCase();
    }
    
    /**
     * KEYS - 查找匹配的键
     * 
     * 支持通配符:
     * - *: 匹配任意数量字符
     * - ?: 匹配单个字符
     * 
     * 注意: 生产环境慎用，时间复杂度O(N)
     */
    public Set<String> keys(String pattern) {
        Set<String> result = new HashSet<>();
        String regex = pattern.replace("*", ".*").replace("?", ".");
        for (String key : current().keySet()) {
            if (key.matches(regex)) {
                RedisObject obj = current().get(key);
                if (obj != null && !obj.isExpired()) {
                    result.add(key);
                }
            }
        }
        return result;
    }
    
    /**
     * RENAME - 重命名键
     */
    public String rename(String oldKey, String newKey) {
        RedisObject obj = current().remove(oldKey);
        if (obj == null) return null;
        current().put(newKey, obj);
        return "OK";
    }
    
    /**
     * RENAMENX - 仅当新键不存在时重命名
     */
    public long renamenx(String oldKey, String newKey) {
        if (current().containsKey(newKey)) return 0;
        return rename(oldKey, newKey) != null ? 1 : 0;
    }
    
    /**
     * DBSIZE - 获取当前数据库的键数量
     */
    public long dbsize() {
        return current().size();
    }
    
    /**
     * FLUSHDB - 清空当前数据库
     */
    public void flushdb() {
        current().clear();
    }
    
    /**
     * FLUSHALL - 清空所有数据库
     */
    public void flushall() {
        for (Map<String, RedisObject> db : databases) {
            db.clear();
        }
    }
    
    // ==================== 对象操作 ====================
    
    /**
     * 获取RedisObject对象
     */
    public RedisObject getObject(String key) {
        RedisObject obj = current().get(key);
        if (obj == null || obj.isExpired()) {
            return null;
        }
        return obj;
    }
    
    /**
     * 设置RedisObject对象
     */
    public void setObject(String key, RedisObject obj) {
        current().put(key, obj);
    }
    
    /**
     * 获取指定索引的数据库
     */
    public Map<String, RedisObject> getDb(int index) {
        if (index >= 0 && index < databases.size()) {
            return databases.get(index);
        }
        return null;
    }
    
    /**
     * 获取当前数据库的数据副本
     */
    public Map<String, RedisObject> getCurrentDbData() {
        return new HashMap<>(current());
    }
    
    /**
     * 获取所有数据库的数据副本
     */
    public List<Map<String, RedisObject>> getAllDatabases() {
        List<Map<String, RedisObject>> result = new ArrayList<>();
        for (Map<String, RedisObject> db : databases) {
            result.add(new HashMap<>(db));
        }
        return result;
    }
    
    /**
     * 关闭数据库
     */
    public void shutdown() {
        expireExecutor.shutdown();
    }
}
