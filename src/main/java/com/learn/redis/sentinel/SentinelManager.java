package com.learn.redis.sentinel;

import com.learn.redis.config.RedisConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * SentinelManager - Redis哨兵管理器
 * 
 * 【哨兵简介】
 * Redis Sentinel是Redis的高可用性解决方案
 * 它可以监控Redis主从复制，在主节点故障时自动进行故障转移
 * 
 * 【哨兵的功能】
 * 1. 监控(Monitoring): 持续检查主从节点是否正常运行
 * 2. 通知(Notification): 当节点出现问题时通知管理员
 * 3. 自动故障转移(Automatic Failover): 主节点故障时自动提升从节点
 * 4. 配置提供者(Configuration Provider): 为客户端提供当前主节点地址
 * 
 * 【故障检测机制】
 * 1. 主观下线(S_DOWN, Subjectively Down)
 *    - 单个哨兵认为节点下线
 *    - 超过down-after-milliseconds没有回复
 * 
 * 2. 客观下线(O_DOWN, Objectively Down)
 *    - 多数哨兵认为节点下线
 *    - 达到quorum配置的数量
 * 
 * 【故障转移流程】
 * 1. 发现主节点客观下线
 * 2. 哨兵之间选举领导者
 * 3. 领导者选择一个从节点提升为主节点
 * 4. 其他从节点开始复制新的主节点
 * 5. 通知客户端新的主节点地址
 * 
 * 【选举算法】
 * 使用Raft算法的简化版本:
 * - 每个哨兵都有一个纪元(epoch)
 * - 纪元递增后请求其他哨兵投票
 * - 获得多数票的哨兵成为领导者
 */
public class SentinelManager {
    private static final Logger logger = LoggerFactory.getLogger(SentinelManager.class);
    
    /**
     * 主节点状态枚举
     */
    public enum MasterState {
        OK,       // 正常
        S_DOWN,   // 主观下线
        O_DOWN    // 客观下线
    }
    
    /** 监控的主节点名称 */
    private final String masterName;
    
    /** 主节点地址 */
    private final String masterHost;
    
    /** 主节点端口 */
    private final int masterPort;
    
    /** 判定客观下线所需的哨兵数量 */
    private final int quorum;
    
    /** 判定主观下线的超时时间(毫秒) */
    private final long downAfter;
    
    /** 故障转移超时时间(毫秒) */
    private final long failoverTimeout;
    
    /** 主节点当前状态 */
    private MasterState masterState;
    
    /** 最后一次PING的时间 */
    private long lastPingTime;
    
    /** 最后一次收到回复的时间 */
    private long lastOkReplyTime;
    
    /** 从节点信息 */
    private final Map<String, SentinelSlave> slaves;
    
    /** 其他哨兵信息 */
    private final Map<String, SentinelInfo> sentinels;
    
    /** 定时任务调度器 */
    private final ScheduledExecutorService scheduler;
    
    /** 异步任务执行器 */
    private final ExecutorService executor;
    
    /** 是否正在进行故障转移 */
    private volatile boolean failoverInProgress;
    
    /** 当前故障转移的领导者 */
    private String currentFailoverLeader;
    
    /** 故障转移开始时间 */
    private long failoverStartTime;
    
    /** 故障转移纪元(用于选举) */
    private int failoverEpoch;
    
    /** 到主节点的客户端连接 */
    private SentinelClient sentinelClient;
    
    /**
     * 构造函数
     * 
     * @param config 配置信息
     */
    public SentinelManager(RedisConfig config) {
        String monitor = config.getSentinelMonitorMaster();
        if (monitor != null) {
            String[] parts = monitor.split(" ");
            this.masterName = parts[0];
            this.masterHost = parts.length > 1 ? parts[1] : "127.0.0.1";
            this.masterPort = parts.length > 2 ? Integer.parseInt(parts[2]) : 6379;
        } else {
            this.masterName = "mymaster";
            this.masterHost = "127.0.0.1";
            this.masterPort = 6379;
        }
        
        this.quorum = config.getSentinelMonitorQuorum();
        this.downAfter = config.getSentinelDownAfter();
        this.failoverTimeout = config.getSentinelFailoverTimeout();
        
        this.masterState = MasterState.OK;
        this.lastPingTime = System.currentTimeMillis();
        this.lastOkReplyTime = System.currentTimeMillis();
        
        this.slaves = new ConcurrentHashMap<>();
        this.sentinels = new ConcurrentHashMap<>();
        
        this.scheduler = Executors.newScheduledThreadPool(2);
        this.executor = Executors.newCachedThreadPool();
        
        this.failoverInProgress = false;
        this.failoverEpoch = 0;
        
        this.sentinelClient = new SentinelClient(masterHost, masterPort);
    }
    
    /**
     * 启动哨兵
     * 
     * 启动多个定时任务:
     * 1. 定期PING主节点
     * 2. 检查主节点状态
     * 3. 发现从节点
     * 4. 发现其他哨兵
     */
    public void start() {
        // 每秒PING主节点
        scheduler.scheduleAtFixedRate(this::pingMaster, 0, 1, TimeUnit.SECONDS);
        
        // 每秒检查主节点状态
        scheduler.scheduleAtFixedRate(this::checkMasterState, 1, 1, TimeUnit.SECONDS);
        
        // 每10秒发现从节点
        scheduler.scheduleAtFixedRate(this::discoverSlaves, 5, 10, TimeUnit.SECONDS);
        
        // 每10秒发现其他哨兵
        scheduler.scheduleAtFixedRate(this::discoverSentinels, 5, 10, TimeUnit.SECONDS);
        
        logger.info("Sentinel started monitoring master '{}' at {}:{}", masterName, masterHost, masterPort);
    }
    
    /**
     * 关闭哨兵
     */
    public void shutdown() {
        scheduler.shutdown();
        executor.shutdown();
        if (sentinelClient != null) {
            sentinelClient.close();
        }
    }
    
    /**
     * PING主节点
     * 
     * 【实现细节】
     * 1. 发送PING命令
     * 2. 如果收到PONG回复，更新lastOkReplyTime
     * 3. 如果之前是主观下线，恢复为OK状态
     */
    private void pingMaster() {
        lastPingTime = System.currentTimeMillis();
        
        try {
            boolean isUp = sentinelClient.ping();
            if (isUp) {
                lastOkReplyTime = System.currentTimeMillis();
                if (masterState == MasterState.S_DOWN) {
                    logger.info("Master {}:{} is now reachable", masterHost, masterPort);
                    masterState = MasterState.OK;
                }
            }
        } catch (Exception e) {
            logger.debug("Failed to ping master: {}", e.getMessage());
        }
    }
    
    /**
     * 检查主节点状态
     * 
     * 【状态转换】
     * OK -> S_DOWN: 超过downAfter没有收到回复
     * S_DOWN -> O_DOWN: 多数哨兵认为下线
     * O_DOWN -> 开始故障转移
     */
    private void checkMasterState() {
        long now = System.currentTimeMillis();
        
        // 检查是否主观下线
        if (masterState == MasterState.OK) {
            if (now - lastOkReplyTime > downAfter) {
                masterState = MasterState.S_DOWN;
                logger.warn("Master {}:{} is subjectively down (s_down)", masterHost, masterPort);
                
                // 询问其他哨兵的意见
                askOtherSentinels();
            }
        }
        
        // 检查是否客观下线
        if (masterState == MasterState.S_DOWN) {
            int downVotes = countDownVotes();
            if (downVotes >= quorum) {
                masterState = MasterState.O_DOWN;
                logger.warn("Master {}:{} is objectively down (o_down). Starting failover...", masterHost, masterPort);
                
                // 开始故障转移
                startFailover();
            }
        }
    }
    
    /**
     * 询问其他哨兵是否认为主节点下线
     */
    private void askOtherSentinels() {
        for (SentinelInfo sentinel : sentinels.values()) {
            executor.submit(() -> {
                try {
                    SentinelClient client = new SentinelClient(sentinel.host, sentinel.port);
                    boolean agrees = client.askMasterDown(masterName);
                    sentinel.votedDown = agrees;
                    client.close();
                } catch (Exception e) {
                    logger.debug("Failed to ask sentinel {}:{}", sentinel.host, sentinel.port);
                }
            });
        }
    }
    
    /**
     * 统计认为主节点下线的哨兵数量
     */
    private int countDownVotes() {
        int votes = 1;  // 自己的一票
        for (SentinelInfo sentinel : sentinels.values()) {
            if (sentinel.votedDown) {
                votes++;
            }
        }
        return votes;
    }
    
    /**
     * 开始故障转移
     * 
     * 【故障转移流程】
     * 1. 选举领导者哨兵
     * 2. 领导者选择新的主节点
     * 3. 提升从节点为主节点
     * 4. 重新配置其他从节点
     */
    private void startFailover() {
        if (failoverInProgress) {
            return;
        }
        
        failoverInProgress = true;
        failoverEpoch++;
        failoverStartTime = System.currentTimeMillis();
        
        logger.info("Starting failover for master '{}', epoch {}", masterName, failoverEpoch);
        
        executor.submit(() -> {
            try {
                // 选举领导者
                String leader = runElection();
                if (leader != null) {
                    currentFailoverLeader = leader;
                    logger.info("Failover leader elected: {}", leader);
                    
                    // 如果自己是领导者，执行故障转移
                    if (leader.equals(getMyId())) {
                        performFailover();
                    }
                }
            } catch (Exception e) {
                logger.error("Failover failed", e);
            } finally {
                failoverInProgress = false;
            }
        });
    }
    
    /**
     * 运行选举
     * 
     * 【Raft选举算法】
     * 1. 增加纪元
     * 2. 请求其他哨兵投票
     * 3. 获得多数票的成为领导者
     * 
     * @return 领导者ID
     */
    private String runElection() {
        Map<String, Integer> votes = new HashMap<>();
        String myId = getMyId();
        votes.put(myId, 1);
        
        // 请求其他哨兵投票
        for (SentinelInfo sentinel : sentinels.values()) {
            try {
                SentinelClient client = new SentinelClient(sentinel.host, sentinel.port);
                String vote = client.requestVote(masterName, failoverEpoch);
                if (vote != null) {
                    votes.merge(vote, 1, Integer::sum);
                }
                client.close();
            } catch (Exception e) {
                logger.debug("Failed to get vote from sentinel {}:{}", sentinel.host, sentinel.port);
            }
        }
        
        // 找出得票最多的候选者
        String winner = null;
        int maxVotes = 0;
        for (Map.Entry<String, Integer> entry : votes.entrySet()) {
            if (entry.getValue() > maxVotes) {
                maxVotes = entry.getValue();
                winner = entry.getKey();
            }
        }
        
        // 需要获得多数票
        if (maxVotes >= quorum) {
            return winner;
        }
        return null;
    }
    
    /**
     * 执行故障转移
     * 
     * 【步骤】
     * 1. 选择最佳的从节点
     * 2. 将从节点提升为主节点
     * 3. 重新配置其他从节点
     */
    private void performFailover() {
        logger.info("Performing failover for master '{}'", masterName);
        
        // 选择最佳从节点
        SentinelSlave bestSlave = selectBestSlave();
        if (bestSlave == null) {
            logger.error("No suitable slave found for promotion");
            return;
        }
        
        logger.info("Selected slave {}:{} for promotion", bestSlave.host, bestSlave.port);
        
        try {
            SentinelClient slaveClient = new SentinelClient(bestSlave.host, bestSlave.port);
            
            // 提升从节点为主节点
            slaveClient.sendCommand("SLAVEOF", "NO", "ONE");
            logger.info("Promoted slave {}:{} to master", bestSlave.host, bestSlave.port);
            
            Thread.sleep(1000);
            
            // 重新配置其他从节点
            for (SentinelSlave slave : slaves.values()) {
                if (!slave.host.equals(bestSlave.host) || slave.port != bestSlave.port) {
                    try {
                        SentinelClient sClient = new SentinelClient(slave.host, slave.port);
                        sClient.sendCommand("SLAVEOF", bestSlave.host, String.valueOf(bestSlave.port));
                        sClient.close();
                    } catch (Exception e) {
                        logger.warn("Failed to reconfigure slave {}:{}", slave.host, slave.port);
                    }
                }
            }
            
            slaveClient.close();
            
            logger.info("Failover completed. New master is {}:{}", bestSlave.host, bestSlave.port);
            
            // 更新主节点连接
            if (sentinelClient != null) {
                sentinelClient.close();
            }
            sentinelClient = new SentinelClient(bestSlave.host, bestSlave.port);
            
            masterState = MasterState.OK;
            
        } catch (Exception e) {
            logger.error("Failed to perform failover", e);
        }
    }
    
    /**
     * 选择最佳从节点
     * 
     * 【选择策略】
     * 1. 排除不健康的从节点
     * 2. 选择复制偏移量最大的（数据最新）
     * 3. 如果偏移量相同，选择ID最小的
     */
    private SentinelSlave selectBestSlave() {
        SentinelSlave best = null;
        long maxOffset = -1;
        
        for (SentinelSlave slave : slaves.values()) {
            if (slave.isUp && slave.replOffset > maxOffset) {
                maxOffset = slave.replOffset;
                best = slave;
            }
        }
        
        return best;
    }
    
    /**
     * 发现从节点
     * 
     * 通过INFO replication命令获取从节点信息
     */
    private void discoverSlaves() {
        try {
            String info = sentinelClient.getInfo("replication");
            parseSlaveInfo(info);
        } catch (Exception e) {
            logger.debug("Failed to discover slaves: {}", e.getMessage());
        }
    }
    
    /**
     * 解析从节点信息
     */
    private void parseSlaveInfo(String info) {
        if (info == null) return;
        
        String[] lines = info.split("\n");
        for (String line : lines) {
            if (line.startsWith("slave")) {
                String[] parts = line.split(":");
                if (parts.length >= 2) {
                    String[] attrs = parts[1].split(",");
                    String host = null;
                    int port = 0;
                    long offset = 0;
                    String state = "";
                    
                    for (String attr : attrs) {
                        String[] kv = attr.split("=");
                        if (kv.length == 2) {
                            switch (kv[0]) {
                                case "ip":
                                    host = kv[1];
                                    break;
                                case "port":
                                    port = Integer.parseInt(kv[1]);
                                    break;
                                case "offset":
                                    offset = Long.parseLong(kv[1]);
                                    break;
                                case "state":
                                    state = kv[1];
                                    break;
                            }
                        }
                    }
                    
                    if (host != null && port > 0) {
                        String key = host + ":" + port;
                        SentinelSlave slave = slaves.get(key);
                        if (slave == null) {
                            slave = new SentinelSlave(host, port);
                            slaves.put(key, slave);
                        }
                        slave.replOffset = offset;
                        slave.isUp = "online".equals(state);
                    }
                }
            }
        }
    }
    
    /**
     * 发现其他哨兵
     */
    private void discoverSentinels() {
        // 实际实现中通过发布订阅发现其他哨兵
    }
    
    /**
     * 获取自己的哨兵ID
     */
    private String getMyId() {
        return "sentinel-" + ProcessHandle.current().pid() + "-" + Thread.currentThread().getId();
    }
    
    /**
     * 获取哨兵信息
     */
    public String getInfo() {
        StringBuilder sb = new StringBuilder();
        sb.append("# Sentinel\n");
        sb.append("sentinel_masters:1\n");
        sb.append("sentinel_tilt:0\n");
        sb.append("sentinel_running_scripts:0\n");
        sb.append("sentinel_scripts_queue_length:0\n");
        sb.append("sentinel_simulate_failure_flags:0\n");
        sb.append("master0:name=").append(masterName)
          .append(",status=").append(masterState.name().toLowerCase())
          .append(",address=").append(masterHost).append(":").append(masterPort)
          .append(",slaves=").append(slaves.size())
          .append(",sentinels=").append(sentinels.size())
          .append("\n");
        return sb.toString();
    }
    
    /**
     * 获取主节点信息
     */
    public String getMasterInfo() {
        StringBuilder sb = new StringBuilder();
        sb.append("name:").append(masterName).append("\n");
        sb.append("ip:").append(masterHost).append("\n");
        sb.append("port:").append(masterPort).append("\n");
        sb.append("flags:").append(masterState.name().toLowerCase()).append("\n");
        sb.append("num-slaves:").append(slaves.size()).append("\n");
        sb.append("num-other-sentinels:").append(sentinels.size()).append("\n");
        sb.append("quorum:").append(quorum).append("\n");
        return sb.toString();
    }
    
    /**
     * 从节点信息类
     */
    private static class SentinelSlave {
        String host;
        int port;
        long replOffset;
        boolean isUp;
        
        SentinelSlave(String host, int port) {
            this.host = host;
            this.port = port;
            this.replOffset = 0;
            this.isUp = true;
        }
    }
    
    /**
     * 哨兵信息类
     */
    private static class SentinelInfo {
        String id;
        String host;
        int port;
        boolean votedDown;
        
        SentinelInfo(String id, String host, int port) {
            this.id = id;
            this.host = host;
            this.port = port;
            this.votedDown = false;
        }
    }
}
