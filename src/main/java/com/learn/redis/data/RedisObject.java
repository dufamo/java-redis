package com.learn.redis.data;

/**
 * RedisObject - Redis对象系统
 * 
 * Redis中的每个键值对都是一个RedisObject。
 * Redis使用对象系统来实现：
 * 1. 类型检查 - 确保命令只能用于正确的类型
 * 2. 多态 - 同一命令可以用于不同类型
 * 3. 内存回收 - 引用计数或垃圾回收
 * 4. 对象共享 - 共享相同对象节省内存
 * 5. 对象过期 - 设置键的生存时间
 * 
 * 【Redis对象的内存布局】
 * +-------------------+--------------------+-------------------+
 * |      type (4bit)  |  encoding (4bit)   |       ptr         |
 * +-------------------+--------------------+-------------------+
 * |                  expireTime (可选)                          |
 * +-------------------------------------------------------------+
 * 
 * type:     对象类型（STRING, LIST, HASH, SET, ZSET）
 * encoding: 底层编码方式
 * ptr:      指向实际数据的指针
 * 
 * 【为什么需要对象系统？】
 * 1. 统一接口：不同类型的数据可以通过统一的对象接口操作
 * 2. 编码转换：可以根据使用情况自动选择最优编码
 * 3. 内存优化：小对象可以使用特殊编码节省内存
 * 4. 类型安全：可以在执行命令前检查类型
 */
public class RedisObject {
    
    /**
     * 对象类型枚举
     * 
     * Redis支持5种基本数据类型：
     * - STRING: 字符串，可以存储字符串、整数、浮点数、二进制数据
     * - LIST: 列表，有序可重复的字符串集合
     * - HASH: 哈希表，键值对集合
     * - SET: 集合，无序不重复的字符串集合
     * - ZSET: 有序集合，带分数的有序不重复字符串集合
     */
    public enum Type {
        STRING,     // 字符串类型
        LIST,       // 列表类型
        HASH,       // 哈希类型
        SET,        // 集合类型
        ZSET        // 有序集合类型
    }
    
    /**
     * 编码方式枚举
     * 
     * Redis的每种类型都有多种编码方式，根据数据特征自动选择最优编码：
     * 
     * 【STRING的编码】
     * - INT:    整数值，直接存储long，不需要SDS
     * - EMBSTR: 嵌入式字符串，短字符串（<=44字节），SDS和RedisObject连续存储
     * - RAW:    原始字符串，长字符串，SDS和RedisObject分开存储
     * 
     * 【LIST的编码】
     * - ZIPLIST:    压缩列表，元素少且小时使用，内存连续
     * - LINKEDLIST: 双端链表，元素多或大时使用
     * 
     * 【HASH的编码】
     * - ZIPLIST: 压缩列表，键值对少且小时使用
     * - HT:      哈希表，键值对多或大时使用
     * 
     * 【SET的编码】
     * - INTSET: 整数集合，全是整数且数量少时使用
     * - HT:     哈希表，有非整数或数量多时使用
     * 
     * 【ZSET的编码】
     * - ZIPLIST:  压缩列表，元素少且小时使用
     * - SKIPLIST: 跳表+哈希表，元素多或大时使用
     */
    public enum Encoding {
        RAW,        // 原始字符串（SDS）
        INT,        // 整数编码
        EMBSTR,     // 嵌入式字符串
        ZIPLIST,    // 压缩列表
        LINKEDLIST, // 双端链表
        HT,         // 哈希表（dict）
        SKIPLIST,   // 跳表
        INTSET      // 整数集合
    }
    
    /**
     * 对象类型
     * 决定了对象可以执行哪些命令
     */
    private Type type;
    
    /**
     * 编码方式
     * 决定了对象的底层实现
     * 同一类型可以有多种编码，Redis会根据情况自动转换
     */
    private Encoding encoding;
    
    /**
     * 指向实际数据的指针
     * 根据encoding的不同，指向不同的数据结构：
     * - INT:     直接存储long值（不是指针）
     * - EMBSTR:  String对象
     * - RAW:     SDS对象
     * - HT:      RedisHash或RedisSet对象
     * - SKIPLIST: RedisZSet对象
     * - LINKEDLIST: RedisList对象
     */
    private Object ptr;
    
    /**
     * 过期时间（毫秒时间戳）
     * -1表示永不过期
     * 其他值表示过期的时间戳
     * 
     * Redis的过期策略：
     * 1. 惰性删除：访问时检查是否过期
     * 2. 定期删除：定期随机检查并删除过期键
     */
    private long expireTime;
    
    /**
     * 构造函数
     * 
     * @param type     对象类型
     * @param encoding 编码方式
     * @param ptr      数据指针
     */
    public RedisObject(Type type, Encoding encoding, Object ptr) {
        this.type = type;
        this.encoding = encoding;
        this.ptr = ptr;
        this.expireTime = -1;  // 默认永不过期
    }
    
    /**
     * 创建字符串对象 - 自动选择最优编码
     * 
     * 【编码选择策略】
     * 1. 如果能解析为整数，使用INT编码
     *    - 节省内存：直接存储long，不需要SDS
     *    - 操作高效：数值运算直接进行
     * 
     * 2. 如果长度<=44字节，使用EMBSTR编码
     *    - 内存连续：RedisObject和SDS连续存储
     *    - 分配一次：只需一次内存分配
     *    - 缓存友好：连续内存提高缓存命中率
     * 
     * 3. 如果长度>44字节，使用RAW编码
     *    - 分开存储：RedisObject和SDS分开
     *    - 灵活扩容：SDS可以独立扩容
     * 
     * 为什么是44字节？
     * - RedisObject头部约16字节
     * - SDS头部约3字节
     * - 内存分配器jemalloc有64字节的小块
     * - 64 - 16 - 3 = 45字节可用于数据
     * - 所以44字节以内的字符串可以用EMBSTR
     * 
     * @param value 字符串值
     * @return RedisObject对象
     */
    public static RedisObject createString(String value) {
        // 尝试解析为整数
        try {
            long longValue = Long.parseLong(value);
            // INT编码：直接存储long值，节省内存
            return new RedisObject(Type.STRING, Encoding.INT, longValue);
        } catch (NumberFormatException e) {
            // 不是整数，根据长度选择EMBSTR或RAW
            if (value.length() <= 44) {
                // EMBSTR编码：短字符串，内存连续
                return new RedisObject(Type.STRING, Encoding.EMBSTR, value);
            }
            // RAW编码：长字符串，使用SDS
            return new RedisObject(Type.STRING, Encoding.RAW, new SDS(value));
        }
    }
    
    /**
     * 创建列表对象
     * 
     * 这里简化实现，直接使用LinkedList
     * 实际Redis会根据条件选择ziplist或linkedlist
     * 
     * @return 列表对象
     */
    public static RedisObject createList() {
        return new RedisObject(Type.LIST, Encoding.LINKEDLIST, new RedisList());
    }
    
    /**
     * 创建哈希对象
     * 
     * 这里简化实现，直接使用哈希表
     * 实际Redis会根据条件选择ziplist或hashtable
     * 
     * @return 哈希对象
     */
    public static RedisObject createHash() {
        return new RedisObject(Type.HASH, Encoding.HT, new RedisHash());
    }
    
    /**
     * 创建集合对象
     * 
     * 这里简化实现，直接使用哈希表
     * 实际Redis会根据条件选择intset或hashtable
     * 
     * @return 集合对象
     */
    public static RedisObject createSet() {
        return new RedisObject(Type.SET, Encoding.HT, new RedisSet());
    }
    
    /**
     * 创建有序集合对象
     * 
     * 使用跳表实现，O(logN)的插入、删除、查询复杂度
     * 
     * @return 有序集合对象
     */
    public static RedisObject createZSet() {
        return new RedisObject(Type.ZSET, Encoding.SKIPLIST, new RedisZSet());
    }
    
    // ==================== Getter和Setter方法 ====================
    
    public Type getType() { return type; }
    public void setType(Type type) { this.type = type; }
    
    public Encoding getEncoding() { return encoding; }
    public void setEncoding(Encoding encoding) { this.encoding = encoding; }
    
    public Object getPtr() { return ptr; }
    public void setPtr(Object ptr) { this.ptr = ptr; }
    
    public long getExpireTime() { return expireTime; }
    public void setExpireTime(long expireTime) { this.expireTime = expireTime; }
    
    /**
     * 检查对象是否已过期
     * 
     * 实现惰性删除策略：
     * 每次访问对象时检查是否过期，如果过期则删除
     * 
     * @return true如果已过期，false如果未过期或永不过期
     */
    public boolean isExpired() {
        if (expireTime == -1) return false;  // 永不过期
        return System.currentTimeMillis() > expireTime;
    }
    
    /**
     * 检查对象是否设置了过期时间
     * 
     * @return true如果设置了过期时间
     */
    public boolean hasExpire() {
        return expireTime != -1;
    }
    
    /**
     * 获取字符串值
     * 
     * 根据不同的编码方式，提取字符串值：
     * - INT: 将long转为字符串
     * - EMBSTR: 直接返回String
     * - RAW: 将SDS转为字符串
     * 
     * @return 字符串值，如果类型不对返回null
     */
    public String getStringValue() {
        if (encoding == Encoding.INT) {
            // INT编码：long值转字符串
            return String.valueOf((Long) ptr);
        } else if (encoding == Encoding.EMBSTR) {
            // EMBSTR编码：直接是String
            return (String) ptr;
        } else if (encoding == Encoding.RAW) {
            // RAW编码：SDS转字符串
            return ((SDS) ptr).toString();
        }
        return null;
    }
    
    /**
     * 获取实际数据对象
     * 
     * 使用泛型避免强制类型转换
     * 例如：RedisList list = obj.getValue();
     * 
     * @param <T> 数据类型
     * @return 数据对象
     */
    @SuppressWarnings("unchecked")
    public <T> T getValue() {
        return (T) ptr;
    }
}
