package com.learn.redis.data;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * RedisSet - Redis集合数据结构实现
 * 
 * 【集合简介】
 * Redis的集合是一个无序的字符串集合
 * 集合中的元素是唯一的，不允许重复
 * 
 * 【底层实现】
 * Redis使用两种编码:
 * 1. intset(整数集合)
 *    - 当所有元素都是整数时使用
 *    - 元素数量 < 512个
 *    - 内存连续，节省内存
 *    - 支持升级但不支持降级
 * 
 * 2. hashtable(哈希表)
 *    - 当有非整数元素或元素较多时使用
 *    - 查找效率O(1)
 *    - 使用哈希表的key存储元素，value为null
 * 
 * 【时间复杂度】
 * - SADD: O(1) 单个元素，O(N) 多个元素
 * - SREM: O(1) 单个元素
 * - SISMEMBER: O(1)
 * - SCARD: O(1)
 * - SMEMBERS: O(N)
 * - SPOP: O(1)
 * - SRANDMEMBER: O(1)
 * - SINTER/SUNION/SDIFF: O(N*M) N为最小集合大小，M为集合数量
 * 
 * 【应用场景】
 * - 标签系统
 * - 共同好友
 * - 抽奖系统
 * - 去重
 */
public class RedisSet {
    
    /**
     * 底层存储结构
     * 
     * 使用Java的HashSet实现
     * HashSet基于HashMap实现:
     * - 使用HashMap的key存储元素
     * - value统一存储一个静态Object对象
     * - 查找、插入、删除都是O(1)
     */
    private final Set<String> set;
    
    /**
     * 构造函数 - 初始化空集合
     */
    public RedisSet() {
        this.set = new HashSet<>();
    }
    
    /**
     * SADD - 添加元素到集合
     * 
     * 【实现细节】
     * 如果元素已存在，不添加
     * 如果元素不存在，添加到集合
     * 
     * @param members 要添加的元素(可多个)
     * @return 实际添加的元素数量
     */
    public long sadd(String... members) {
        long added = 0;
        for (String member : members) {
            // HashSet.add()返回true表示元素不存在且添加成功
            if (set.add(member)) {
                added++;
            }
        }
        return added;
    }
    
    /**
     * SREM - 从集合移除元素
     * 
     * @param members 要移除的元素(可多个)
     * @return 实际移除的元素数量
     */
    public long srem(String... members) {
        long removed = 0;
        for (String member : members) {
            // HashSet.remove()返回true表示元素存在且移除成功
            if (set.remove(member)) {
                removed++;
            }
        }
        return removed;
    }
    
    /**
     * SPOP - 随机弹出元素
     * 
     * 【实现细节】
     * 随机选择一个元素，移除并返回
     * Redis使用更高效的随机算法
     * 
     * @return 被弹出的元素，集合为空返回null
     */
    public String spop() {
        if (set.isEmpty()) return null;
        
        // 随机选择一个元素
        Random random = new Random();
        int index = random.nextInt(set.size());
        int i = 0;
        for (String member : set) {
            if (i++ == index) {
                set.remove(member);
                return member;
            }
        }
        return null;
    }
    
    /**
     * SPOP - 随机弹出多个元素
     * 
     * @param count 要弹出的元素数量
     * @return 被弹出的元素集合
     */
    public Set<String> spop(long count) {
        Set<String> result = new HashSet<>();
        while (count > 0 && !set.isEmpty()) {
            String member = spop();
            if (member != null) {
                result.add(member);
                count--;
            }
        }
        return result;
    }
    
    /**
     * SRANDMEMBER - 随机获取元素(不移除)
     * 
     * @return 随机元素，集合为空返回null
     */
    public String srandmember() {
        if (set.isEmpty()) return null;
        
        Random random = new Random();
        int index = random.nextInt(set.size());
        int i = 0;
        for (String member : set) {
            if (i++ == index) {
                return member;
            }
        }
        return null;
    }
    
    /**
     * SRANDMEMBER - 随机获取多个元素(不移除)
     * 
     * 【参数规则】
     * - count > 0: 返回不重复的元素，最多min(count, size)个
     * - count < 0: 返回可能重复的元素，数量为|count|
     * 
     * @param count 要获取的元素数量
     * @return 随机元素集合
     */
    public Set<String> srandmember(long count) {
        Set<String> result = new HashSet<>();
        
        if (count >= 0) {
            // 返回不重复的元素
            java.util.List<String> list = new java.util.ArrayList<>(set);
            Random random = new Random();
            while (result.size() < count && !list.isEmpty()) {
                int index = random.nextInt(list.size());
                result.add(list.get(index));
            }
        } else {
            // 返回可能重复的元素
            java.util.List<String> list = new java.util.ArrayList<>(set);
            Random random = new Random();
            for (long i = 0; i < -count; i++) {
                int index = random.nextInt(list.size());
                result.add(list.get(index));
            }
        }
        return result;
    }
    
    /**
     * SISMEMBER - 检查元素是否在集合中
     * 
     * @param member 要检查的元素
     * @return 存在返回true，不存在返回false
     */
    public boolean sismember(String member) {
        return set.contains(member);
    }
    
    /**
     * SCARD - 获取集合元素数量
     * 
     * @return 元素数量
     */
    public long scard() {
        return set.size();
    }
    
    /**
     * SMEMBERS - 获取集合中所有元素
     * 
     * @return 所有元素的集合副本
     */
    public Set<String> smembers() {
        return new HashSet<>(set);
    }
    
    /**
     * SMOVE - 将元素从一个集合移动到另一个集合
     * 
     * 【实现细节】
     * 1. 从源集合移除元素
     * 2. 添加到目标集合
     * 两个操作是原子的
     * 
     * @param dest 目标集合
     * @param member 要移动的元素
     * @return 移动成功返回true，元素不存在返回false
     */
    public boolean smove(RedisSet dest, String member) {
        if (set.remove(member)) {
            dest.sadd(member);
            return true;
        }
        return false;
    }
    
    /**
     * SINTER - 集合交集
     * 
     * 【算法说明】
     * 返回所有集合中都存在的元素
     * 例如: A={a,b,c}, B={b,c,d}
     * SINTER A B = {b,c}
     * 
     * 【优化策略】
     * Redis会先对集合按大小排序
     * 然后从最小集合开始遍历
     * 这样可以减少比较次数
     * 
     * @param sets 要求交集的集合数组
     * @return 交集结果
     */
    public static Set<String> sinter(RedisSet... sets) {
        if (sets.length == 0) return new HashSet<>();
        
        // 以第一个集合为基础
        Set<String> result = new HashSet<>(sets[0].smembers());
        
        // 与其他集合求交集
        for (int i = 1; i < sets.length; i++) {
            result.retainAll(sets[i].smembers());
        }
        return result;
    }
    
    /**
     * SUNION - 集合并集
     * 
     * 【算法说明】
     * 返回所有集合中的所有元素(去重)
     * 例如: A={a,b,c}, B={b,c,d}
     * SUNION A B = {a,b,c,d}
     * 
     * @param sets 要求并集的集合数组
     * @return 并集结果
     */
    public static Set<String> sunion(RedisSet... sets) {
        Set<String> result = new HashSet<>();
        for (RedisSet s : sets) {
            result.addAll(s.smembers());
        }
        return result;
    }
    
    /**
     * SDIFF - 集合差集
     * 
     * 【算法说明】
     * 返回在第一个集合但不在其他集合中的元素
     * 例如: A={a,b,c}, B={b,c,d}
     * SDIFF A B = {a}
     * 
     * @param sets 要求差集的集合数组
     * @return 差集结果
     */
    public static Set<String> sdiff(RedisSet... sets) {
        if (sets.length == 0) return new HashSet<>();
        
        // 以第一个集合为基础
        Set<String> result = new HashSet<>(sets[0].smembers());
        
        // 减去其他集合的元素
        for (int i = 1; i < sets.length; i++) {
            result.removeAll(sets[i].smembers());
        }
        return result;
    }
    
    /**
     * 检查集合是否为空
     */
    public boolean isEmpty() {
        return set.isEmpty();
    }
}
