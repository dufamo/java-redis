package com.learn.redis.cluster;

/**
 * ClusterNode - 集群节点信息
 * 
 * 【节点标识】
 * 每个集群节点都有一个唯一的40位十六进制ID。
 * 这个ID在节点的整个生命周期中保持不变，即使IP地址改变。
 * 
 * 【节点属性】
 * - nodeId: 节点唯一标识
 * - host: 节点地址
 * - port: 节点端口
 * - master: 是否为主节点
 * - masterId: 如果是从节点，记录主节点ID
 * - connected: 是否已连接
 * - lastPingTime: 最后一次发送PING的时间
 * - lastPongTime: 最后一次收到PONG的时间
 * 
 * 【主从关系】
 * 集群中的每个主节点可以有多个从节点。
 * 从节点的masterId指向其主节点的nodeId。
 * 当主节点故障时，从节点可以被提升为主节点。
 */
public class ClusterNode {
    
    /**
     * 节点ID
     * 
     * 40位十六进制字符串，全局唯一。
     * 例如：07c37dfeb235213a872192d90877d0cd55635b91
     */
    private final String nodeId;
    
    /**
     * 节点主机地址
     */
    private final String host;
    
    /**
     * 节点端口
     */
    private final int port;
    
    /**
     * 是否为主节点
     * 
     * true: 主节点，负责处理部分槽位
     * false: 从节点，复制主节点的数据
     */
    private boolean master;
    
    /**
     * 主节点ID
     * 
     * 如果当前节点是从节点，这个字段记录其主节点的ID。
     * 如果当前节点是主节点，这个字段为null。
     */
    private String masterId;
    
    /**
     * 连接状态
     * 
     * true: 已连接，可以正常通信
     * false: 未连接或连接断开
     */
    private boolean connected;
    
    /**
     * 最后一次发送PING的时间戳
     * 
     * 用于计算节点响应时间，判断节点是否存活
     */
    private long lastPingTime;
    
    /**
     * 最后一次收到PONG的时间戳
     * 
     * 用于判断节点是否存活
     */
    private long lastPongTime;
    
    /**
     * 构造函数
     * 
     * @param nodeId 节点ID
     * @param host 主机地址
     * @param port 端口号
     */
    public ClusterNode(String nodeId, String host, int port) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
        this.master = false;
        this.masterId = null;
        this.connected = false;
        this.lastPingTime = 0;
        this.lastPongTime = 0;
    }
    
    // ==================== Getter/Setter方法 ====================
    
    public String getNodeId() { return nodeId; }
    public String getHost() { return host; }
    public int getPort() { return port; }
    
    public boolean isMaster() { return master; }
    public void setMaster(boolean master) { this.master = master; }
    
    public String getMasterId() { return masterId; }
    public void setMasterId(String masterId) { this.masterId = masterId; }
    
    public boolean isConnected() { return connected; }
    public void setConnected(boolean connected) { this.connected = connected; }
    
    public long getLastPingTime() { return lastPingTime; }
    public void setLastPingTime(long lastPingTime) { this.lastPingTime = lastPingTime; }
    
    public long getLastPongTime() { return lastPongTime; }
    public void setLastPongTime(long lastPongTime) { this.lastPongTime = lastPongTime; }
    
    @Override
    public String toString() {
        return "ClusterNode{" +
                "nodeId='" + nodeId + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", master=" + master +
                ", connected=" + connected +
                '}';
    }
}
