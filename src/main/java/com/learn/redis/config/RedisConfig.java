package com.learn.redis.config;

/**
 * RedisConfig - Redis配置类
 * 
 * 【功能说明】
 * 集中管理Redis服务器的所有配置选项。
 * 支持通过命令行参数或配置文件进行配置。
 * 
 * 【配置分类】
 * 1. 基础配置
 *    - port: 监听端口
 *    - databases: 数据库数量
 *    - bind: 绑定地址
 * 
 * 2. RDB持久化配置
 *    - rdbEnabled: 是否启用RDB
 *    - rdbFilename: RDB文件名
 *    - rdbSaveInterval: 保存间隔
 *    - rdbSaveChanges: 触发保存的修改次数
 * 
 * 3. AOF持久化配置
 *    - aofEnabled: 是否启用AOF
 *    - aofFilename: AOF文件名
 *    - aofFsync: fsync策略
 * 
 * 4. 主从复制配置
 *    - replicationEnabled: 是否启用复制
 *    - replicaof: 主节点地址
 *    - replicaReadonly: 从节点只读
 * 
 * 5. 集群配置
 *    - clusterEnabled: 是否启用集群
 *    - clusterConfigFile: 集群配置文件
 *    - clusterNodeTimeout: 节点超时时间
 * 
 * 6. 哨兵配置
 *    - sentinelEnabled: 是否启用哨兵
 *    - sentinelMonitorMaster: 监控的主节点
 *    - sentinelMonitorQuorum: 客观下线票数
 *    - sentinelDownAfter: 主观下线超时
 *    - sentinelFailoverTimeout: 故障转移超时
 * 
 * 【配置示例】
 * 命令行启动：
 * java RedisServer --port 6380 --databases 32 --aof-enabled true
 */
public class RedisConfig {
    
    // ==================== 基础配置 ====================
    
    /**
     * 监听端口
     * 
     * 默认值：6379
     * Redis默认端口，以Merz命名（Alessia Merz的电话号码）
     */
    private int port = 6379;
    
    /**
     * 数据库数量
     * 
     * 默认值：16
     * Redis默认创建16个数据库（编号0-15）
     * 客户端可以通过SELECT命令切换数据库
     */
    private int databases = 16;
    
    /**
     * 绑定地址
     * 
     * 默认值：0.0.0.0（监听所有网卡）
     * 可以设置为特定IP以提高安全性
     */
    private String bind = "0.0.0.0";
    
    // ==================== RDB持久化配置 ====================
    
    /**
     * 是否启用RDB持久化
     * 
     * 默认值：true
     * RDB是Redis的默认持久化方式
     */
    private boolean rdbEnabled = true;
    
    /**
     * RDB文件名
     * 
     * 默认值：dump.rdb
     */
    private String rdbFilename = "dump.rdb";
    
    /**
     * RDB保存间隔（秒）
     * 
     * 默认值：60秒
     * 配合rdbSaveChanges使用
     */
    private int rdbSaveInterval = 60;
    
    /**
     * 触发RDB保存的修改次数
     * 
     * 默认值：1000次
     * 当在rdbSaveInterval秒内有rdbSaveChanges次修改时触发保存
     */
    private int rdbSaveChanges = 1000;
    
    // ==================== AOF持久化配置 ====================
    
    /**
     * 是否启用AOF持久化
     * 
     * 默认值：false
     * AOF提供更高的数据安全性，但性能略低
     */
    private boolean aofEnabled = false;
    
    /**
     * AOF文件名
     * 
     * 默认值：appendonly.aof
     */
    private String aofFilename = "appendonly.aof";
    
    /**
     * AOF fsync策略
     * 
     * 可选值：
     * - always: 每个命令都fsync（最安全，最慢）
     * - everysec: 每秒fsync（推荐，最多丢失1秒数据）
     * - no: 由操作系统决定（最快，可能丢失数据）
     */
    private String aofFsync = "everysec";
    
    // ==================== 主从复制配置 ====================
    
    /**
     * 是否启用主从复制
     * 
     * 当配置了replicaof时自动启用
     */
    private boolean replicationEnabled = false;
    
    /**
     * 主节点地址
     * 
     * 格式：host:port
     * 例如：127.0.0.1:6379
     * 配置后当前节点成为从节点
     */
    private String replicaof = null;
    
    /**
     * 从节点是否只读
     * 
     * 默认值：1（只读）
     * 从节点默认只读，防止数据不一致
     */
    private int replicaReadonly = 1;
    
    // ==================== 集群配置 ====================
    
    /**
     * 是否启用集群模式
     * 
     * 默认值：false
     */
    private boolean clusterEnabled = false;
    
    /**
     * 集群配置文件
     * 
     * 默认值：nodes.conf
     * 存储集群节点信息和槽位分配
     */
    private String clusterConfigFile = "nodes.conf";
    
    /**
     * 集群节点超时时间（毫秒）
     * 
     * 默认值：15000（15秒）
     * 超过此时间未收到节点响应，认为节点下线
     */
    private int clusterNodeTimeout = 15000;
    
    /**
     * 集群从节点有效性因子
     * 
     * 用于计算从节点是否能参与故障转移
     */
    private int clusterReplicaValidityFactor = 10;
    
    // ==================== 哨兵配置 ====================
    
    /**
     * 是否启用哨兵模式
     * 
     * 默认值：false
     */
    private boolean sentinelEnabled = false;
    
    /**
     * 监控的主节点
     * 
     * 格式：masterName host port
     * 例如：mymaster 127.0.0.1 6379
     */
    private String sentinelMonitorMaster = null;
    
    /**
     * 客观下线所需票数
     * 
     * 默认值：2
     * 当quorum个哨兵认为主节点下线时，触发故障转移
     */
    private int sentinelMonitorQuorum = 2;
    
    /**
     * 主观下线超时时间（毫秒）
     * 
     * 默认值：30000（30秒）
     * 超过此时间未收到主节点响应，哨兵认为其主观下线
     */
    private int sentinelDownAfter = 30000;
    
    /**
     * 故障转移超时时间（毫秒）
     * 
     * 默认值：180000（3分钟）
     * 故障转移的最长时间
     */
    private int sentinelFailoverTimeout = 180000;
    
    /**
     * 默认构造函数
     */
    public RedisConfig() {}
    
    /**
     * 从命令行参数解析配置
     * 
     * 支持的参数：
     * --port: 监听端口
     * --databases: 数据库数量
     * --rdb-enabled: 是否启用RDB
     * --aof-enabled: 是否启用AOF
     * --replicaof: 主节点地址
     * --cluster-enabled: 是否启用集群
     * --sentinel-enabled: 是否启用哨兵
     * 
     * @param args 命令行参数数组
     * @return 配置对象
     */
    public static RedisConfig fromArgs(String[] args) {
        RedisConfig config = new RedisConfig();
        
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port":
                    config.port = Integer.parseInt(args[++i]);
                    break;
                case "--databases":
                    config.databases = Integer.parseInt(args[++i]);
                    break;
                case "--rdb-enabled":
                    config.rdbEnabled = Boolean.parseBoolean(args[++i]);
                    break;
                case "--aof-enabled":
                    config.aofEnabled = Boolean.parseBoolean(args[++i]);
                    break;
                case "--replicaof":
                    config.replicaof = args[++i];
                    config.replicationEnabled = true;
                    break;
                case "--cluster-enabled":
                    config.clusterEnabled = Boolean.parseBoolean(args[++i]);
                    break;
                case "--sentinel-enabled":
                    config.sentinelEnabled = Boolean.parseBoolean(args[++i]);
                    break;
            }
        }
        
        return config;
    }
    
    // ==================== Getter/Setter方法 ====================
    
    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }
    
    public int getDatabases() { return databases; }
    public void setDatabases(int databases) { this.databases = databases; }
    
    public String getBind() { return bind; }
    public void setBind(String bind) { this.bind = bind; }
    
    public boolean isRdbEnabled() { return rdbEnabled; }
    public void setRdbEnabled(boolean rdbEnabled) { this.rdbEnabled = rdbEnabled; }
    
    public String getRdbFilename() { return rdbFilename; }
    public void setRdbFilename(String rdbFilename) { this.rdbFilename = rdbFilename; }
    
    public int getRdbSaveInterval() { return rdbSaveInterval; }
    public void setRdbSaveInterval(int rdbSaveInterval) { this.rdbSaveInterval = rdbSaveInterval; }
    
    public int getRdbSaveChanges() { return rdbSaveChanges; }
    public void setRdbSaveChanges(int rdbSaveChanges) { this.rdbSaveChanges = rdbSaveChanges; }
    
    public boolean isAofEnabled() { return aofEnabled; }
    public void setAofEnabled(boolean aofEnabled) { this.aofEnabled = aofEnabled; }
    
    public String getAofFilename() { return aofFilename; }
    public void setAofFilename(String aofFilename) { this.aofFilename = aofFilename; }
    
    public String getAofFsync() { return aofFsync; }
    public void setAofFsync(String aofFsync) { this.aofFsync = aofFsync; }
    
    public boolean isReplicationEnabled() { return replicationEnabled; }
    public void setReplicationEnabled(boolean replicationEnabled) { this.replicationEnabled = replicationEnabled; }
    
    public String getReplicaof() { return replicaof; }
    public void setReplicaof(String replicaof) { this.replicaof = replicaof; }
    
    public int getReplicaReadonly() { return replicaReadonly; }
    public void setReplicaReadonly(int replicaReadonly) { this.replicaReadonly = replicaReadonly; }
    
    public boolean isClusterEnabled() { return clusterEnabled; }
    public void setClusterEnabled(boolean clusterEnabled) { this.clusterEnabled = clusterEnabled; }
    
    public String getClusterConfigFile() { return clusterConfigFile; }
    public void setClusterConfigFile(String clusterConfigFile) { this.clusterConfigFile = clusterConfigFile; }
    
    public int getClusterNodeTimeout() { return clusterNodeTimeout; }
    public void setClusterNodeTimeout(int clusterNodeTimeout) { this.clusterNodeTimeout = clusterNodeTimeout; }
    
    public boolean isSentinelEnabled() { return sentinelEnabled; }
    public void setSentinelEnabled(boolean sentinelEnabled) { this.sentinelEnabled = sentinelEnabled; }
    
    public String getSentinelMonitorMaster() { return sentinelMonitorMaster; }
    public void setSentinelMonitorMaster(String sentinelMonitorMaster) { this.sentinelMonitorMaster = sentinelMonitorMaster; }
    
    public int getSentinelMonitorQuorum() { return sentinelMonitorQuorum; }
    public void setSentinelMonitorQuorum(int sentinelMonitorQuorum) { this.sentinelMonitorQuorum = sentinelMonitorQuorum; }
    
    public int getSentinelDownAfter() { return sentinelDownAfter; }
    public void setSentinelDownAfter(int sentinelDownAfter) { this.sentinelDownAfter = sentinelDownAfter; }
    
    public int getSentinelFailoverTimeout() { return sentinelFailoverTimeout; }
    public void setSentinelFailoverTimeout(int sentinelFailoverTimeout) { this.sentinelFailoverTimeout = sentinelFailoverTimeout; }
    
    public int getClusterReplicaValidityFactor() { return clusterReplicaValidityFactor; }
    public void setClusterReplicaValidityFactor(int clusterReplicaValidityFactor) { this.clusterReplicaValidityFactor = clusterReplicaValidityFactor; }
}
