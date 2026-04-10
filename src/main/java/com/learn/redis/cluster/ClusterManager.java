package com.learn.redis.cluster;

import com.learn.redis.config.RedisConfig;
import com.learn.redis.data.RedisDatabase;
import com.learn.redis.data.RedisObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

/**
 * ClusterManager - Redis集群管理器
 * 
 * 【集群简介】
 * Redis Cluster是Redis的分布式解决方案
 * 它可以将数据自动分片到多个节点上，提供高可用和可扩展性
 * 
 * 【集群特性】
 * 1. 自动分片: 数据自动分布到16384个槽位上
 * 2. 高可用: 支持主从复制和自动故障转移
 * 3. 去中心化: 无需代理，客户端直连节点
 * 4. 最终一致性: 异步复制，可能丢失部分写操作
 * 
 * 【槽位(Slot)机制】
 * Redis Cluster将数据划分为16384个槽位
 * 每个键属于一个特定的槽位，计算公式:
 * slot = CRC16(key) % 16384
 * 
 * 槽位分配给集群中的主节点:
 * - 节点A: 0-5460
 * - 节点B: 5461-10922
 * - 节点C: 10923-16383
 * 
 * 【哈希标签(Hash Tag)】
 * 为了让相关的键分配到同一个槽位，可以使用哈希标签:
 * - user:{1000}:profile
 * - user:{1000}:settings
 * 这两个键会被分配到同一个槽位，因为只计算{}内的部分
 * 
 * 【MOVED重定向】
 * 当客户端访问错误的节点时，返回MOVED错误:
 * - MOVED 3999 127.0.0.1:6381
 * 表示槽位3999已移动到127.0.0.1:6381
 * 
 * 【ASK重定向】
 * 在槽位迁移过程中，返回ASK错误:
 * - ASK 3999 127.0.0.1:6381
 * 表示槽位3999正在迁移，请向127.0.0.1:6381发送ASKING命令
 * 
 * 【故障检测】
 * 1. PING/PONG心跳
 * 2. PFAIL(可能下线): 单个节点认为某节点下线
 * 3. FAIL(确认下线): 多数主节点认为某节点下线
 * 
 * 【故障转移】
 * 当主节点下线时，其从节点发起选举:
 * 1. 从节点向所有主节点请求投票
 * 2. 获得多数票的从节点成为新主节点
 * 3. 其他从节点开始复制新主节点
 */
public class ClusterManager {
    private static final Logger logger = LoggerFactory.getLogger(ClusterManager.class);
    
    /** 集群槽位总数 */
    public static final int CLUSTER_SLOTS = 16384;
    
    /** 集群状态常量 */
    public static final int CLUSTER_OK = 0;     // 集群正常
    public static final int CLUSTER_FAIL = 1;   // 集群故障
    
    /** 配置信息 */
    private final RedisConfig config;
    
    /** 数据库实例 */
    private final RedisDatabase database;
    
    /** 当前节点ID（40位十六进制字符串） */
    private String nodeId;
    
    /** 当前节点地址 */
    private String nodeHost;
    
    /** 当前节点端口 */
    private int nodePort;
    
    /** 集群状态 */
    private int state;
    
    /** 集群节点列表 */
    private final Map<String, ClusterNode> nodes;
    
    /** 槽位分配表 */
    private final ClusterSlot[] slots;
    
    /** 定时任务调度器 */
    private final ScheduledExecutorService scheduler;
    
    /** 异步任务执行器 */
    private final ExecutorService executor;
    
    /** 到其他节点的连接 */
    private final Map<String, ClusterConnection> connections;
    
    /**
     * 构造函数
     * 
     * @param database 数据库实例
     * @param config   配置信息
     */
    public ClusterManager(RedisDatabase database, RedisConfig config) {
        this.config = config;
        this.database = database;
        
        // 生成唯一的节点ID
        this.nodeId = generateNodeId();
        this.nodeHost = "127.0.0.1";
        this.nodePort = config.getPort();
        this.state = CLUSTER_FAIL;
        
        this.nodes = new ConcurrentHashMap<>();
        this.slots = new ClusterSlot[CLUSTER_SLOTS];
        
        this.scheduler = Executors.newScheduledThreadPool(2);
        this.executor = Executors.newCachedThreadPool();
        
        this.connections = new ConcurrentHashMap<>();
        
        // 加载集群配置
        loadClusterConfig();
    }
    
    /**
     * 生成节点ID
     * 
     * 节点ID是一个40位的十六进制字符串
     * 在节点的整个生命周期中保持不变
     */
    private String generateNodeId() {
        StringBuilder sb = new StringBuilder();
        Random random = new Random();
        for (int i = 0; i < 40; i++) {
            sb.append(Integer.toHexString(random.nextInt(16)));
        }
        return sb.toString();
    }
    
    /**
     * 加载集群配置文件
     * 
     * 配置文件格式:
     * node:<node-id> <host>:<port> <flags> <link-state>
     */
    private void loadClusterConfig() {
        File configFile = new File(config.getClusterConfigFile());
        if (configFile.exists()) {
            try (BufferedReader reader = new BufferedReader(new FileReader(configFile))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.startsWith("node:")) {
                        parseNodeConfig(line);
                    }
                }
            } catch (Exception e) {
                logger.error("Failed to load cluster config", e);
            }
        }
    }
    
    /**
     * 解析节点配置
     */
    private void parseNodeConfig(String line) {
        String[] parts = line.substring(5).split(" ");
        if (parts.length >= 3) {
            String id = parts[0];
            String host = parts[1].split(":")[0];
            int port = Integer.parseInt(parts[1].split(":")[1]);
            String flags = parts[2];
            
            ClusterNode node = new ClusterNode(id, host, port);
            node.setMaster(flags.contains("master"));
            nodes.put(id, node);
        }
    }
    
    /**
     * 启动集群管理器
     * 
     * 启动定时任务:
     * 1. 定期与其他节点通信
     * 2. 定期保存集群配置
     */
    public void start() {
        // 每秒执行集群维护
        scheduler.scheduleAtFixedRate(this::clusterCron, 0, 1, TimeUnit.SECONDS);
        
        // 每10秒保存配置
        scheduler.scheduleAtFixedRate(this::saveClusterConfig, 10, 10, TimeUnit.SECONDS);
        
        logger.info("Cluster node {} started on {}:{}", nodeId, nodeHost, nodePort);
    }
    
    /**
     * 关闭集群管理器
     */
    public void shutdown() {
        scheduler.shutdown();
        executor.shutdown();
        
        for (ClusterConnection conn : connections.values()) {
            conn.close();
        }
        
        saveClusterConfig();
    }
    
    /**
     * 集群定时任务
     * 
     * 执行:
     * 1. 与其他节点交换信息
     * 2. 检测节点故障
     * 3. 更新集群状态
     */
    private void clusterCron() {
        // PING其他节点
        for (ClusterNode node : nodes.values()) {
            if (!node.getNodeId().equals(nodeId)) {
                pingNode(node);
            }
        }
        
        // 检查集群状态
        checkClusterState();
    }
    
    /**
     * PING指定节点
     * 
     * @param node 目标节点
     */
    private void pingNode(ClusterNode node) {
        executor.submit(() -> {
            try {
                ClusterConnection conn = getConnection(node);
                if (conn != null) {
                    conn.sendPing();
                    node.setLastPingTime(System.currentTimeMillis());
                    node.setLastPongTime(System.currentTimeMillis());
                    node.setConnected(true);
                }
            } catch (Exception e) {
                node.setConnected(false);
                logger.debug("Failed to ping node {}: {}", node.getNodeId(), e.getMessage());
            }
        });
    }
    
    /**
     * 获取到节点的连接
     */
    private ClusterConnection getConnection(ClusterNode node) {
        String key = node.getNodeId();
        ClusterConnection conn = connections.get(key);
        
        if (conn == null || !conn.isConnected()) {
            try {
                conn = new ClusterConnection(node.getHost(), node.getPort());
                connections.put(key, conn);
            } catch (Exception e) {
                return null;
            }
        }
        
        return conn;
    }
    
    /**
     * 检查集群状态
     * 
     * 如果所有槽位都有节点负责，则集群正常
     */
    private void checkClusterState() {
        int coveredSlots = 0;
        for (ClusterSlot slot : slots) {
            if (slot != null && slot.getNode() != null) {
                coveredSlots++;
            }
        }
        
        if (coveredSlots == CLUSTER_SLOTS) {
            state = CLUSTER_OK;
        } else {
            state = CLUSTER_FAIL;
        }
    }
    
    /**
     * 保存集群配置文件
     */
    private void saveClusterConfig() {
        try (PrintWriter writer = new PrintWriter(new FileWriter(config.getClusterConfigFile()))) {
            writer.println("# Cluster configuration file");
            
            for (ClusterNode node : nodes.values()) {
                StringBuilder sb = new StringBuilder();
                sb.append("node:").append(node.getNodeId());
                sb.append(" ").append(node.getHost()).append(":").append(node.getPort());
                sb.append(" ").append(node.isMaster() ? "master" : "slave");
                sb.append(" ").append(node.isConnected() ? "connected" : "disconnected");
                writer.println(sb.toString());
            }
        } catch (Exception e) {
            logger.error("Failed to save cluster config", e);
        }
    }
    
    /**
     * 计算键的槽位
     * 
     * 【算法】
     * 1. 检查键是否包含{}（哈希标签）
     * 2. 如果有，只计算{}内的部分
     * 3. 使用CRC16计算哈希值
     * 4. 对16384取模得到槽位
     * 
     * @param key 键名
     * @return 槽位号(0-16383)
     */
    public int getSlot(String key) {
        int hash = 0;
        
        // 处理哈希标签
        int start = key.indexOf('{');
        int end = key.indexOf('}');
        
        if (start >= 0 && end > start + 1) {
            // 只计算{}内的部分
            key = key.substring(start + 1, end);
        }
        
        // 简化的CRC16计算（实际Redis使用标准CRC16）
        for (int i = 0; i < key.length(); i++) {
            hash = hash * 31 + key.charAt(i);
        }
        
        return Math.abs(hash) % CLUSTER_SLOTS;
    }
    
    /**
     * 根据槽位获取负责的节点
     * 
     * @param slot 槽位号
     * @return 负责该槽位的节点
     */
    public ClusterNode getNodeBySlot(int slot) {
        if (slot < 0 || slot >= CLUSTER_SLOTS) {
            return null;
        }
        
        ClusterSlot cs = slots[slot];
        return cs != null ? cs.getNode() : null;
    }
    
    /**
     * 检查槽位是否由当前节点负责
     * 
     * @param slot 槽位号
     * @return 是否由当前节点负责
     */
    public boolean isMySlot(int slot) {
        ClusterNode node = getNodeBySlot(slot);
        return node != null && node.getNodeId().equals(nodeId);
    }
    
    /**
     * 处理命令（检查槽位归属）
     * 
     * @param key     键名
     * @param command 命令
     * @param args    参数
     * @return 如果槽位不归当前节点，返回MOVED错误；否则返回null
     */
    public byte[] handleCommand(String key, String command, String[] args) {
        int slot = getSlot(key);
        
        if (!isMySlot(slot)) {
            ClusterNode targetNode = getNodeBySlot(slot);
            if (targetNode != null) {
                // 返回MOVED重定向
                String moved = "MOVED " + slot + " " + targetNode.getHost() + ":" + targetNode.getPort();
                return ("-" + moved + "\r\n").getBytes(StandardCharsets.UTF_8);
            } else {
                // 槽位未分配
                return "-CLUSTERDOWN Hash slot not served\r\n".getBytes(StandardCharsets.UTF_8);
            }
        }
        
        return null;  // 槽位归当前节点，正常执行命令
    }
    
    // ==================== 集群管理命令 ====================
    
    /**
     * 添加节点到集群
     * 
     * @param nodeId 节点ID
     * @param host   节点地址
     * @param port   节点端口
     */
    public void addNode(String nodeId, String host, int port) {
        ClusterNode node = new ClusterNode(nodeId, host, port);
        node.setMaster(true);
        nodes.put(nodeId, node);
        logger.info("Added cluster node {}: {}:{}", nodeId, host, port);
    }
    
    /**
     * 从集群移除节点
     * 
     * @param nodeId 节点ID
     */
    public void removeNode(String nodeId) {
        nodes.remove(nodeId);
        connections.remove(nodeId);
        
        // 移除该节点负责的槽位
        for (int i = 0; i < CLUSTER_SLOTS; i++) {
            ClusterSlot slot = slots[i];
            if (slot != null && slot.getNode() != null && 
                slot.getNode().getNodeId().equals(nodeId)) {
                slots[i] = null;
            }
        }
        
        logger.info("Removed cluster node {}", nodeId);
    }
    
    /**
     * 分配槽位给当前节点
     * 
     * @param slotIndices 槽位号数组
     */
    public void addSlots(int... slotIndices) {
        ClusterNode self = getSelfNode();
        for (int slot : slotIndices) {
            if (slot >= 0 && slot < CLUSTER_SLOTS) {
                slots[slot] = new ClusterSlot(slot, self);
            }
        }
        logger.info("Added {} slots to node {}", slotIndices.length, nodeId);
    }
    
    /**
     * 移除当前节点的槽位
     * 
     * @param slotIndices 槽位号数组
     */
    public void delSlots(int... slotIndices) {
        for (int slot : slotIndices) {
            if (slot >= 0 && slot < CLUSTER_SLOTS) {
                slots[slot] = null;
            }
        }
        logger.info("Deleted {} slots from node {}", slotIndices.length, nodeId);
    }
    
    /**
     * 设置槽位归属
     * 
     * @param slot   槽位号
     * @param nodeId 节点ID
     */
    public void setSlot(int slot, String nodeId) {
        ClusterNode node = nodes.get(nodeId);
        if (node != null && slot >= 0 && slot < CLUSTER_SLOTS) {
            slots[slot] = new ClusterSlot(slot, node);
        }
    }
    
    /**
     * 获取当前节点对象
     */
    private ClusterNode getSelfNode() {
        ClusterNode self = nodes.get(nodeId);
        if (self == null) {
            self = new ClusterNode(nodeId, nodeHost, nodePort);
            self.setMaster(true);
            self.setConnected(true);
            nodes.put(nodeId, self);
        }
        return self;
    }
    
    /**
     * 设置当前节点为指定节点的从节点
     * 
     * @param masterNodeId 主节点ID
     */
    public void replicate(String masterNodeId) {
        ClusterNode master = nodes.get(masterNodeId);
        if (master != null) {
            ClusterNode self = getSelfNode();
            self.setMaster(false);
            self.setMasterId(masterNodeId);
            logger.info("Replicating from master {}", masterNodeId);
        }
    }
    
    // ==================== 信息获取方法 ====================
    
    /**
     * 获取集群信息
     */
    public String getInfo() {
        StringBuilder sb = new StringBuilder();
        sb.append("cluster_state:").append(state == CLUSTER_OK ? "ok" : "fail").append("\n");
        sb.append("cluster_slots_assigned:").append(countAssignedSlots()).append("\n");
        sb.append("cluster_slots_ok:").append(countAssignedSlots()).append("\n");
        sb.append("cluster_slots_pfail:0\n");
        sb.append("cluster_slots_fail:0\n");
        sb.append("cluster_known_nodes:").append(nodes.size()).append("\n");
        sb.append("cluster_size:").append(countMasters()).append("\n");
        sb.append("cluster_current_epoch:1\n");
        sb.append("cluster_my_epoch:1\n");
        return sb.toString();
    }
    
    /**
     * 统计已分配的槽位数量
     */
    private int countAssignedSlots() {
        int count = 0;
        for (ClusterSlot slot : slots) {
            if (slot != null && slot.getNode() != null) {
                count++;
            }
        }
        return count;
    }
    
    /**
     * 统计主节点数量
     */
    private int countMasters() {
        int count = 0;
        for (ClusterNode node : nodes.values()) {
            if (node.isMaster()) {
                count++;
            }
        }
        return count;
    }
    
    /**
     * 获取节点列表信息
     */
    public String getNodesInfo() {
        StringBuilder sb = new StringBuilder();
        
        for (ClusterNode node : nodes.values()) {
            sb.append(node.getNodeId());
            sb.append(" ").append(node.getHost()).append(":").append(node.getPort());
            sb.append(" ").append(node.isMaster() ? "master" : "slave");
            
            if (node.getNodeId().equals(nodeId)) {
                sb.append(" myself");
            }
            
            if (!node.isConnected()) {
                sb.append(" fail");
            }
            
            // 显示槽位范围
            if (node.isMaster()) {
                sb.append(" links ");
                int startSlot = -1;
                for (int i = 0; i <= CLUSTER_SLOTS; i++) {
                    ClusterSlot slot = (i < CLUSTER_SLOTS) ? slots[i] : null;
                    boolean isMySlot = slot != null && slot.getNode() != null && 
                                      slot.getNode().getNodeId().equals(node.getNodeId());
                    
                    if (isMySlot && startSlot == -1) {
                        startSlot = i;
                    } else if (!isMySlot && startSlot != -1) {
                        if (startSlot == i - 1) {
                            sb.append(startSlot);
                        } else {
                            sb.append(startSlot).append("-").append(i - 1);
                        }
                        sb.append(" ");
                        startSlot = -1;
                    }
                }
            } else if (node.getMasterId() != null) {
                sb.append(" ").append(node.getMasterId());
            }
            
            sb.append("\n");
        }
        
        return sb.toString();
    }
    
    public String getNodeId() {
        return nodeId;
    }
    
    public int getState() {
        return state;
    }
    
    public Collection<ClusterNode> getNodes() {
        return nodes.values();
    }
}
