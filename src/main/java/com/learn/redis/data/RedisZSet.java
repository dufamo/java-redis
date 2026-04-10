package com.learn.redis.data;

import java.util.*;

/**
 * RedisZSet - 有序集合实现（基于跳表）
 * 
 * 【跳表(Skip List)简介】
 * 跳表是一种随机化的数据结构，基于并联的链表，其效率可比拟二叉查找树。
 * 
 * 跳表的本质是多层链表：
 * - 最底层是一个有序链表，包含所有元素
 * - 上层链表是下层链表的"快速通道"，跳过部分节点
 * - 查找时从最高层开始，逐层向下，类似二分查找
 * 
 * 【为什么Redis使用跳表而不是红黑树？】
 * 1. 实现简单：跳表比红黑树更容易实现和维护
 * 2. 内存占用：跳表每个节点平均只需要1.33个指针（红黑树需要2个）
 * 3. 范围查询：跳表更适合范围查询（ZRANGE, ZRANGEBYSCORE）
 * 4. 并发友好：跳表更容易实现无锁并发操作
 * 
 * 【跳表结构示意图】
 * Level 3:    HEAD -----------------------------> TAIL
 * Level 2:    HEAD -------------> N2 ----------> TAIL
 * Level 1:    HEAD -----> N1 ----> N2 ----> N3 -> TAIL
 * Level 0:    HEAD -> N0 -> N1 -> N2 -> N3 -> N4 -> TAIL
 * 
 * 【时间复杂度】
 * - 插入：O(logN)
 * - 删除：O(logN)
 * - 查找：O(logN)
 * - 范围查询：O(logN + M)，M为返回元素数量
 * 
 * 【ZSET的双重数据结构】
 * Redis的ZSET实际上使用了两个数据结构：
 * 1. 哈希表(dict)：存储member -> score的映射，O(1)查询分数
 * 2. 跳表(skiplist)：存储score -> member的有序映射，支持范围查询
 * 
 * 这样设计的原因：
 * - ZSCORE命令需要O(1)复杂度 -> 使用哈希表
 * - ZRANGE命令需要有序 -> 使用跳表
 */
public class RedisZSet {
    
    /**
     * 哈希表：存储member -> score的映射
     * 用于O(1)时间复杂度查询成员分数
     */
    private final Map<String, Double> dict;
    
    /**
     * 跳表：存储有序的(score, member)对
     * 用于范围查询和排序操作
     */
    private final SkipList skipList;
    
    /**
     * 构造函数：初始化哈希表和跳表
     */
    public RedisZSet() {
        this.dict = new HashMap<>();
        this.skipList = new SkipList();
    }
    
    /**
     * 添加成员及其分数
     * 
     * 【实现步骤】
     * 1. 检查成员是否已存在
     * 2. 如果存在且分数不同，先删除旧的跳表节点，再插入新的
     * 3. 如果不存在，直接插入
     * 
     * @param score  分数（用于排序）
     * @param member 成员名
     * @return 新增返回1，更新返回0
     */
    public long zadd(double score, String member) {
        Double oldScore = dict.get(member);
        
        if (oldScore != null) {
            // 成员已存在，检查分数是否变化
            if (Double.compare(oldScore, score) != 0) {
                // 分数变化，需要更新跳表中的位置
                skipList.delete(oldScore, member);  // 删除旧位置
                skipList.insert(score, member);      // 插入新位置
                dict.put(member, score);
            }
            return 0;  // 更新操作
        } else {
            // 新成员，直接插入
            dict.put(member, score);
            skipList.insert(score, member);
            return 1;  // 新增操作
        }
    }
    
    /**
     * 批量添加成员
     * 
     * @param members 成员名到分数的映射
     * @return 新增的成员数量
     */
    public long zadd(Map<String, Double> members) {
        long added = 0;
        for (Map.Entry<String, Double> entry : members.entrySet()) {
            added += zadd(entry.getValue(), entry.getKey());
        }
        return added;
    }
    
    /**
     * 删除成员
     * 
     * @param members 要删除的成员名数组
     * @return 实际删除的成员数量
     */
    public long zrem(String... members) {
        long removed = 0;
        for (String member : members) {
            Double score = dict.remove(member);
            if (score != null) {
                skipList.delete(score, member);
                removed++;
            }
        }
        return removed;
    }
    
    /**
     * 获取成员的分数 - O(1)时间复杂度
     * 
     * 使用哈希表实现，直接查询
     * 
     * @param member 成员名
     * @return 分数，不存在返回null
     */
    public Double zscore(String member) {
        return dict.get(member);
    }
    
    /**
     * 获取成员数量
     * 
     * @return 成员数量
     */
    public long zcard() {
        return dict.size();
    }
    
    /**
     * 增加成员分数
     * 
     * 如果成员不存在，相当于zadd(increment, member)
     * 
     * @param increment 增量（可为负数）
     * @param member    成员名
     * @return 新的分数
     */
    public Double zincrby(double increment, String member) {
        Double oldScore = dict.get(member);
        double newScore = (oldScore == null) ? increment : oldScore + increment;
        
        if (oldScore != null) {
            skipList.delete(oldScore, member);
        }
        skipList.insert(newScore, member);
        dict.put(member, newScore);
        
        return newScore;
    }
    
    /**
     * 获取成员的排名（从0开始）
     * 
     * 排名按分数从小到大排序
     * 
     * @param member 成员名
     * @return 排名，不存在返回null
     */
    public Long zrank(String member) {
        Double score = dict.get(member);
        if (score == null) return null;
        return skipList.getRank(score, member);
    }
    
    /**
     * 获取成员的逆序排名
     * 
     * 排名按分数从大到小排序
     * 
     * @param member 成员名
     * @return 排名，不存在返回null
     */
    public Long zrevrank(String member) {
        Long rank = zrank(member);
        if (rank == null) return null;
        return dict.size() - 1 - rank;
    }
    
    /**
     * 按排名范围获取成员
     * 
     * @param start 起始排名（支持负数，-1表示最后一个）
     * @param stop  结束排名（包含）
     * @return 成员列表（带分数）
     */
    public List<ZSetEntry> zrange(long start, long stop) {
        return skipList.getRange(start, stop);
    }
    
    /**
     * 按排名逆序获取成员
     * 
     * @param start 起始排名
     * @param stop  结束排名
     * @return 成员列表（带分数）
     */
    public List<ZSetEntry> zrevrange(long start, long stop) {
        List<ZSetEntry> result = skipList.getRange(start, stop);
        Collections.reverse(result);
        return result;
    }
    
    /**
     * 按分数范围获取成员
     * 
     * @param min          最小分数
     * @param max          最大分数
     * @param minInclusive 是否包含最小值
     * @param maxInclusive 是否包含最大值
     * @return 成员列表
     */
    public List<ZSetEntry> zrangebyscore(double min, double max, 
                                          boolean minInclusive, boolean maxInclusive) {
        return skipList.getRangeByScore(min, max, minInclusive, maxInclusive);
    }
    
    /**
     * 统计分数范围内的成员数量
     * 
     * @param min          最小分数
     * @param max          最大分数
     * @param minInclusive 是否包含最小值
     * @param maxInclusive 是否包含最大值
     * @return 成员数量
     */
    public long zcount(double min, double max, boolean minInclusive, boolean maxInclusive) {
        return skipList.countByScore(min, max, minInclusive, maxInclusive);
    }
    
    /**
     * 按排名范围删除成员
     * 
     * @param start 起始排名
     * @param stop  结束排名
     * @return 删除的成员数量
     */
    public long zremrangebyrank(long start, long stop) {
        List<ZSetEntry> entries = skipList.getRange(start, stop);
        for (ZSetEntry entry : entries) {
            dict.remove(entry.member);
            skipList.delete(entry.score, entry.member);
        }
        return entries.size();
    }
    
    /**
     * 按分数范围删除成员
     * 
     * @param min          最小分数
     * @param max          最大分数
     * @param minInclusive 是否包含最小值
     * @param maxInclusive 是否包含最大值
     * @return 删除的成员数量
     */
    public long zremrangebyscore(double min, double max, 
                                  boolean minInclusive, boolean maxInclusive) {
        List<ZSetEntry> entries = skipList.getRangeByScore(min, max, minInclusive, maxInclusive);
        for (ZSetEntry entry : entries) {
            dict.remove(entry.member);
            skipList.delete(entry.score, entry.member);
        }
        return entries.size();
    }
    
    /**
     * 检查是否为空
     */
    public boolean isEmpty() {
        return dict.isEmpty();
    }
    
    /**
     * 有序集合条目类
     * 存储成员名和分数的对
     */
    public static class ZSetEntry {
        public final String member;
        public final double score;
        
        public ZSetEntry(String member, double score) {
            this.member = member;
            this.score = score;
        }
    }
    
    /**
     * 跳表实现 - 核心数据结构
     * 
     * 【跳表的核心思想】
     * 通过多层索引加速查找，类似于书的目录
     * 
     * 【节点结构】
     * 每个节点包含：
     * - member: 成员名
     * - score: 分数
     * - level[]: 每一层的指针
     * 
     * 【随机层数】
     * 插入时随机决定节点的层数，概率分布：
     * - 第1层：100%
     * - 第2层：25%
     * - 第3层：6.25%
     * - ...
     * 这保证了跳表的平衡性
     */
    private static class SkipList {
        
        /** 最大层数限制 */
        private static final int MAX_LEVEL = 32;
        
        /** 层数增加的概率（Redis使用0.25） */
        private static final double P = 0.25;
        
        /** 头节点（哨兵） */
        private final SkipListNode head;
        
        /** 尾节点（哨兵） */
        private final SkipListNode tail;
        
        /** 当前最大层数 */
        private int level;
        
        /** 节点数量 */
        private long length;
        
        /** 随机数生成器 */
        private final Random random;
        
        /**
         * 构造函数：初始化跳表
         * 
         * 创建头尾哨兵节点，简化边界处理
         */
        public SkipList() {
            // 头节点：分数为负无穷
            this.head = new SkipListNode(null, Double.NEGATIVE_INFINITY, MAX_LEVEL);
            // 尾节点：分数为正无穷
            this.tail = new SkipListNode(null, Double.POSITIVE_INFINITY, MAX_LEVEL);
            
            // 初始化：所有层的头节点指向尾节点
            for (int i = 0; i < MAX_LEVEL; i++) {
                head.level[i].forward = tail;
            }
            
            this.level = 1;
            this.length = 0;
            this.random = new Random();
        }
        
        /**
         * 插入节点
         * 
         * 【算法步骤】
         * 1. 从最高层开始，找到每一层的插入位置
         * 2. 随机生成新节点的层数
         * 3. 在各层插入新节点
         * 4. 更新跳表的层数和长度
         * 
         * @param score  分数
         * @param member 成员名
         */
        public void insert(double score, String member) {
            // update[i]记录第i层插入位置的前驱节点
            SkipListNode[] update = new SkipListNode[MAX_LEVEL];
            SkipListNode x = head;
            
            // 从最高层向下查找插入位置
            for (int i = level - 1; i >= 0; i--) {
                // 在当前层向右移动，直到找到插入位置
                while (compare(x.level[i].forward, score, member) < 0) {
                    x = x.level[i].forward;
                }
                update[i] = x;  // 记录该层的插入位置
            }
            
            // 随机生成新节点的层数
            int newLevel = randomLevel();
            
            // 如果新层数大于当前层数，需要初始化高层
            if (newLevel > level) {
                for (int i = level; i < newLevel; i++) {
                    update[i] = head;  // 高层的前驱都是头节点
                }
                level = newLevel;
            }
            
            // 创建新节点
            x = new SkipListNode(member, score, newLevel);
            
            // 在各层插入新节点
            for (int i = 0; i < newLevel; i++) {
                x.level[i].forward = update[i].level[i].forward;
                update[i].level[i].forward = x;
            }
            
            length++;
        }
        
        /**
         * 删除节点
         * 
         * 【算法步骤】
         * 1. 找到各层要删除节点的前驱
         * 2. 在各层移除该节点
         * 3. 更新跳表层数
         * 
         * @param score  分数
         * @param member 成员名
         */
        public void delete(double score, String member) {
            SkipListNode[] update = new SkipListNode[MAX_LEVEL];
            SkipListNode x = head;
            
            // 找到各层的前驱节点
            for (int i = level - 1; i >= 0; i--) {
                while (compare(x.level[i].forward, score, member) < 0) {
                    x = x.level[i].forward;
                }
                update[i] = x;
            }
            
            // x是要删除的节点
            x = x.level[0].forward;
            
            // 确认找到了目标节点
            if (x != tail && compare(x, score, member) == 0) {
                // 在各层移除该节点
                for (int i = 0; i < level; i++) {
                    if (update[i].level[i].forward != x) break;
                    update[i].level[i].forward = x.level[i].forward;
                }
                
                // 更新层数（如果高层变空）
                while (level > 1 && head.level[level - 1].forward == tail) {
                    level--;
                }
                length--;
            }
        }
        
        /**
         * 获取成员的排名
         * 
         * 通过遍历跳表计算排名
         * 
         * @param score  分数
         * @param member 成员名
         * @return 排名（从0开始），不存在返回null
         */
        public Long getRank(double score, String member) {
            SkipListNode x = head;
            long rank = 0;
            
            // 从最高层向下遍历
            for (int i = level - 1; i >= 0; i--) {
                while (x.level[i].forward != tail && 
                       (x.level[i].forward.score < score || 
                        (Double.compare(x.level[i].forward.score, score) == 0 && 
                         x.level[i].forward.member.compareTo(member) <= 0))) {
                    rank += x.level[i].span;
                    x = x.level[i].forward;
                }
                
                // 找到目标节点
                if (x.member != null && Double.compare(x.score, score) == 0 && 
                    x.member.equals(member)) {
                    return rank - 1;  // 排名从0开始
                }
            }
            return null;
        }
        
        /**
         * 按排名范围获取节点
         * 
         * @param start 起始排名
         * @param stop  结束排名
         * @return 节点列表
         */
        public List<ZSetEntry> getRange(long start, long stop) {
            List<ZSetEntry> result = new ArrayList<>();
            
            // 处理负数索引
            if (start < 0) start = length + start;
            if (stop < 0) stop = length + stop;
            if (start < 0) start = 0;
            if (stop >= length) stop = length - 1;
            if (start > stop) return result;
            
            // 从第一个实际节点开始遍历
            SkipListNode x = head.level[0].forward;
            long index = 0;
            
            while (x != tail && index <= stop) {
                if (index >= start) {
                    result.add(new ZSetEntry(x.member, x.score));
                }
                x = x.level[0].forward;
                index++;
            }
            
            return result;
        }
        
        /**
         * 按分数范围获取节点
         * 
         * @param min          最小分数
         * @param max          最大分数
         * @param minInclusive 是否包含最小值
         * @param maxInclusive 是否包含最大值
         * @return 节点列表
         */
        public List<ZSetEntry> getRangeByScore(double min, double max, 
                                                boolean minInclusive, boolean maxInclusive) {
            List<ZSetEntry> result = new ArrayList<>();
            SkipListNode x = head;
            
            // 找到起始位置
            for (int i = level - 1; i >= 0; i--) {
                while (x.level[i].forward != tail) {
                    double forwardScore = x.level[i].forward.score;
                    if (minInclusive ? forwardScore < min : forwardScore <= min) {
                        x = x.level[i].forward;
                    } else {
                        break;
                    }
                }
            }
            
            // 遍历范围内的节点
            x = x.level[0].forward;
            while (x != tail) {
                double s = x.score;
                // 检查是否超出范围
                if (maxInclusive ? s > max : s >= max) break;
                // 检查是否在范围内
                if (minInclusive ? s >= min : s > min) {
                    result.add(new ZSetEntry(x.member, s));
                }
                x = x.level[0].forward;
            }
            
            return result;
        }
        
        /**
         * 统计分数范围内的节点数量
         */
        public long countByScore(double min, double max, 
                                  boolean minInclusive, boolean maxInclusive) {
            return getRangeByScore(min, max, minInclusive, maxInclusive).size();
        }
        
        /**
         * 比较节点与目标分数和成员
         * 
         * 先比较分数，分数相同则比较成员名
         * 
         * @param node   节点
         * @param score  目标分数
         * @param member 目标成员
         * @return 负数表示节点小，0表示相等，正数表示节点大
         */
        private int compare(SkipListNode node, double score, String member) {
            if (node == tail) return 1;  // 尾节点最大
            int cmp = Double.compare(node.score, score);
            if (cmp != 0) return cmp;
            if (node.member == null) return -1;
            return node.member.compareTo(member);
        }
        
        /**
         * 随机生成节点层数
         * 
         * 【概率分布】
         * 层数 = 1: 概率 = 1 - P = 0.75
         * 层数 = 2: 概率 = P * (1 - P) = 0.1875
         * 层数 = 3: 概率 = P² * (1 - P) = 0.0469
         * ...
         * 
         * 平均层数 = 1 / (1 - P) = 1.33
         * 
         * @return 层数（1到MAX_LEVEL之间）
         */
        private int randomLevel() {
            int level = 1;
            // 以概率P增加层数
            while (random.nextDouble() < P && level < MAX_LEVEL) {
                level++;
            }
            return level;
        }
    }
    
    /**
     * 跳表节点类
     */
    private static class SkipListNode {
        /** 成员名 */
        String member;
        
        /** 分数 */
        double score;
        
        /** 每一层的指针和跨度 */
        SkipListLevel[] level;
        
        /**
         * 构造函数
         * 
         * @param member     成员名
         * @param score      分数
         * @param levelCount 层数
         */
        SkipListNode(String member, double score, int levelCount) {
            this.member = member;
            this.score = score;
            this.level = new SkipListLevel[levelCount];
            for (int i = 0; i < levelCount; i++) {
                this.level[i] = new SkipListLevel();
            }
        }
    }
    
    /**
     * 跳表层信息
     * 
     * 每个节点的每一层都有：
     * - forward: 指向该层下一个节点的指针
     * - span: 到下一个节点的跨度（用于计算排名）
     */
    private static class SkipListLevel {
        /** 该层下一个节点 */
        SkipListNode forward;
        
        /** 到下一个节点的跨度（排名计算用） */
        long span;
        
        SkipListLevel() {
            this.forward = null;
            this.span = 0;
        }
    }
}
