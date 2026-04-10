package com.learn.redis.cluster;

/**
 * ClusterSlot - 集群槽位信息
 * 
 * 【槽位概念】
 * Redis Cluster将整个键空间划分为16384个槽位（slot）。
 * 每个键通过CRC16(key) % 16384计算属于哪个槽位。
 * 
 * 【槽位分配】
 * 每个槽位由一个主节点负责：
 * - 节点A负责槽位 0-5460
 * - 节点B负责槽位 5461-10922
 * - 节点C负责槽位 10923-16383
 * 
 * 【数据分布】
 * 当客户端访问某个键时：
 * 1. 计算键的槽位
 * 2. 找到负责该槽位的节点
 * 3. 如果是正确的节点，执行命令
 * 4. 如果是错误的节点，返回MOVED重定向
 * 
 * 【槽位迁移】
 * 在集群扩容或缩容时，槽位可以在节点间迁移：
 * - 迁移过程中使用ASK重定向
 * - 迁移完成后更新槽位分配表
 */
public class ClusterSlot {
    
    /**
     * 槽位编号
     * 
     * 范围：0 - 16383
     * 每个槽位编号唯一对应一个槽位
     */
    private final int slot;
    
    /**
     * 负责该槽位的节点
     * 
     * 每个槽位只能由一个主节点负责。
     * 该主节点的从节点会复制槽位中的数据。
     */
    private final ClusterNode node;
    
    /**
     * 构造函数
     * 
     * @param slot 槽位编号（0-16383）
     * @param node 负责该槽位的节点
     */
    public ClusterSlot(int slot, ClusterNode node) {
        this.slot = slot;
        this.node = node;
    }
    
    /**
     * 获取槽位编号
     * 
     * @return 槽位编号
     */
    public int getSlot() { return slot; }
    
    /**
     * 获取负责该槽位的节点
     * 
     * @return 节点对象
     */
    public ClusterNode getNode() { return node; }
}
