package com.learn.redis.data;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Collection;

/**
 * RedisHash - Redis哈希数据结构实现
 * 
 * 【哈希简介】
 * Redis的哈希是一个键值对集合，适合存储对象
 * 每个哈希可以存储2^32-1个键值对
 * 
 * 【底层实现】
 * Redis使用两种编码:
 * 1. ziplist(压缩列表)
 *    - 当元素数量较少时使用
 *    - 内存连续，节省内存
 *    - 查找效率较低O(N)
 * 
 * 2. hashtable(哈希表)
 *    - 当元素数量较多时使用
 *    - 查找效率高O(1)
 *    - 内存占用较大
 * 
 * 【编码转换条件】
 * - 哈希对象保存的键值对数量 < 512个
 * - 所有键值对的键和值的字符串长度都 < 64字节
 * 满足以上条件使用ziplist，否则使用hashtable
 * 
 * 【时间复杂度】
 * - HSET/HGET: O(1)
 * - HDEL: O(1)
 * - HGETALL: O(N)
 * - HKEYS/HVALS: O(N)
 * 
 * 【应用场景】
 * - 存储用户信息
 * - 存储商品详情
 * - 存储配置信息
 */
public class RedisHash {
    
    /**
     * 底层存储结构
     * 
     * 使用Java的HashMap实现
     * HashMap的特点:
     * - 查找: O(1)
     * - 插入: O(1)
     * - 删除: O(1)
     * 
     * Redis的dict(字典)结构类似，但更复杂:
     * - 使用两个哈希表实现渐进式rehash
     * - 支持扩容和缩容
     */
    private final Map<String, String> hash;
    
    /**
     * 构造函数 - 初始化空哈希
     */
    public RedisHash() {
        this.hash = new HashMap<>();
    }
    
    /**
     * HSET - 设置哈希字段的值
     * 
     * 【实现细节】
     * 如果字段已存在，更新值
     * 如果字段不存在，添加新字段
     * 
     * @param field 字段名
     * @param value 字段值
     * @return 新增返回1，更新返回0
     */
    public long hset(String field, String value) {
        boolean isNew = !hash.containsKey(field);
        hash.put(field, value);
        return isNew ? 1 : 0;
    }
    
    /**
     * HSETNX - 仅当字段不存在时设置值
     * 
     * NX = Not eXists
     * 用于实现分布式锁等场景
     * 
     * @param field 字段名
     * @param value 字段值
     * @return 设置成功返回1，字段已存在返回0
     */
    public long hsetnx(String field, String value) {
        if (hash.containsKey(field)) {
            return 0;
        }
        hash.put(field, value);
        return 1;
    }
    
    /**
     * HGET - 获取哈希字段的值
     * 
     * @param field 字段名
     * @return 字段值，不存在返回null
     */
    public String hget(String field) {
        return hash.get(field);
    }
    
    /**
     * HEXISTS - 检查字段是否存在
     * 
     * @param field 字段名
     * @return 存在返回true，不存在返回false
     */
    public boolean hexists(String field) {
        return hash.containsKey(field);
    }
    
    /**
     * HDEL - 删除哈希字段
     * 
     * @param fields 要删除的字段名数组
     * @return 实际删除的字段数量
     */
    public long hdel(String... fields) {
        long deleted = 0;
        for (String field : fields) {
            if (hash.remove(field) != null) {
                deleted++;
            }
        }
        return deleted;
    }
    
    /**
     * HLEN - 获取哈希中字段的数量
     * 
     * @return 字段数量
     */
    public long hlen() {
        return hash.size();
    }
    
    /**
     * HINCRBY - 哈希字段整数值增量
     * 
     * 如果字段不存在，初始值为0
     * 如果字段值不是整数，返回错误
     * 
     * @param field 字段名
     * @param increment 增量（可为负数）
     * @return 增量后的值，值不是整数返回null
     */
    public String hincrby(String field, long increment) {
        String value = hash.get(field);
        long num = 0;
        if (value != null) {
            try {
                num = Long.parseLong(value);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        num += increment;
        hash.put(field, String.valueOf(num));
        return String.valueOf(num);
    }
    
    /**
     * HINCRBYFLOAT - 哈希字段浮点数值增量
     * 
     * @param field 字段名
     * @param increment 增量（可为负数）
     * @return 增量后的值
     */
    public String hincrbyfloat(String field, double increment) {
        String value = hash.get(field);
        double num = 0;
        if (value != null) {
            try {
                num = Double.parseDouble(value);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        num += increment;
        String result = String.valueOf(num);
        hash.put(field, result);
        return result;
    }
    
    /**
     * HKEYS - 获取哈希中所有字段名
     * 
     * @return 字段名集合
     */
    public Set<String> hkeys() {
        return hash.keySet();
    }
    
    /**
     * HVALS - 获取哈希中所有字段值
     * 
     * @return 字段值集合
     */
    public Collection<String> hvals() {
        return hash.values();
    }
    
    /**
     * HGETALL - 获取哈希中所有字段和值
     * 
     * @return 字段-值映射的副本
     */
    public Map<String, String> hgetall() {
        return new HashMap<>(hash);
    }
    
    /**
     * HMSET - 批量设置多个字段的值
     * 
     * @param map 字段-值映射
     * @return 成功返回1
     */
    public long hmset(Map<String, String> map) {
        hash.putAll(map);
        return 1;
    }
    
    /**
     * HMGET - 批量获取多个字段的值
     * 
     * @param fields 字段名数组
     * @return 字段名到值的映射
     */
    public Map<String, String> hmget(String... fields) {
        Map<String, String> result = new HashMap<>();
        for (String field : fields) {
            result.put(field, hash.get(field));
        }
        return result;
    }
    
    /**
     * HSTRLEN - 获取字段值的字符串长度
     * 
     * @param field 字段名
     * @return 值的长度，字段不存在返回0
     */
    public long hstrlen(String field) {
        String value = hash.get(field);
        return value == null ? 0 : value.length();
    }
    
    /**
     * 检查哈希是否为空
     */
    public boolean isEmpty() {
        return hash.isEmpty();
    }
}
