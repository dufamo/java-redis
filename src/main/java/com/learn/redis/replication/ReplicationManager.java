package com.learn.redis.replication;

import com.learn.redis.config.RedisConfig;
import com.learn.redis.data.RedisDatabase;
import com.learn.redis.data.RedisObject;
import com.learn.redis.data.RedisList;
import com.learn.redis.data.RedisHash;
import com.learn.redis.data.RedisSet;
import com.learn.redis.data.RedisZSet;
import com.learn.redis.protocol.RESPObject;
import com.learn.redis.protocol.RESPParser;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.bytes.ByteArrayDecoder;
import io.netty.handler.codec.bytes.ByteArrayEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * ReplicationManager - 主从复制管理器
 * 
 * 【主从复制简介】
 * Redis的主从复制是指将一台Redis服务器的数据复制到其他Redis服务器
 * - 主节点(Master): 负责写操作
 * - 从节点(Slave): 负责读操作，数据来自主节点
 * 
 * 【复制的作用】
 * 1. 数据冗余: 实现数据的热备份
 * 2. 读写分离: 主节点写，从节点读，分担负载
 * 3. 高可用基础: 为哨兵和集群提供基础
 * 
 * 【复制过程】
 * 1. 从节点连接主节点
 * 2. 发送SYNC命令请求同步
 * 3. 主节点执行BGSAVE生成RDB快照
 * 4. 主节点发送RDB给从节点
 * 5. 从节点加载RDB
 * 6. 主节点发送期间的写命令给从节点
 * 7. 进入命令传播阶段
 * 
 * 【复制类型】
 * 1. 全量复制: 从节点第一次连接时
 * 2. 部分复制: 网络断开重连时（使用复制偏移量）
 * 
 * 【心跳机制】
 * - 主节点每秒发送PING
 * - 从节点每秒发送REPLCONF ACK <offset>
 */
public class ReplicationManager {
    private static final Logger logger = LoggerFactory.getLogger(ReplicationManager.class);
    
    /**
     * 节点角色枚举
     */
    public enum Role {
        MASTER,  // 主节点
        SLAVE    // 从节点
    }
    
    /** 数据库实例 */
    private final RedisDatabase database;
    
    /** 配置信息 */
    private final RedisConfig config;
    
    /** 当前节点角色 */
    private Role role;
    
    /** 主节点地址 */
    private String masterHost;
    
    /** 主节点端口 */
    private int masterPort;
    
    /** 复制偏移量 */
    private long masterReplOffset;
    
    /** 复制ID */
    private String masterReplId;
    
    /** 从节点连接列表（主节点使用） */
    private final List<ChannelHandlerContext> slaves;
    
    /** 到主节点的连接（从节点使用） */
    private Channel masterChannel;
    
    /** 定时任务调度器 */
    private final ScheduledExecutorService scheduler;
    
    /** Netty事件循环组 */
    private final EventLoopGroup workerGroup;
    
    /**
     * 构造函数
     * 
     * @param database 数据库实例
     * @param config   配置信息
     */
    public ReplicationManager(RedisDatabase database, RedisConfig config) {
        this.database = database;
        this.config = config;
        this.slaves = new CopyOnWriteArrayList<>();
        this.scheduler = Executors.newScheduledThreadPool(2);
        this.workerGroup = new NioEventLoopGroup();
        
        // 根据配置决定角色
        if (config.getReplicaof() != null) {
            // 配置了replicaof，作为从节点
            String[] parts = config.getReplicaof().split(":");
            this.masterHost = parts[0];
            this.masterPort = parts.length > 1 ? Integer.parseInt(parts[1]) : 6379;
            this.role = Role.SLAVE;
        } else {
            // 没有配置，作为主节点
            this.role = Role.MASTER;
            this.masterReplId = generateReplId();
        }
        
        this.masterReplOffset = 0;
    }
    
    /**
     * 生成复制ID
     * 
     * 复制ID是一个随机字符串，用于标识复制流
     * 主从切换时会生成新的复制ID
     */
    private String generateReplId() {
        return Long.toHexString(System.currentTimeMillis()) + 
               Long.toHexString((long) (Math.random() * Long.MAX_VALUE));
    }
    
    /**
     * 启动复制管理器
     * 
     * - 主节点: 启动心跳检测
     * - 从节点: 连接主节点
     */
    public void start() {
        if (role == Role.SLAVE) {
            connectToMaster();
        } else {
            startHeartbeat();
        }
    }
    
    /**
     * 关闭复制管理器
     */
    public void shutdown() {
        scheduler.shutdown();
        workerGroup.shutdownGracefully();
        
        for (ChannelHandlerContext ctx : slaves) {
            ctx.close();
        }
        
        if (masterChannel != null) {
            masterChannel.close();
        }
    }
    
    /**
     * 连接到主节点（从节点调用）
     * 
     * 【连接流程】
     * 1. 创建Netty客户端
     * 2. 连接主节点
     * 3. 发送同步命令
     * 4. 定时重连
     */
    private void connectToMaster() {
        scheduler.scheduleWithFixedDelay(() -> {
            if (masterChannel == null || !masterChannel.isActive()) {
                try {
                    Bootstrap bootstrap = new Bootstrap();
                    bootstrap.group(workerGroup)
                            .channel(NioSocketChannel.class)
                            .option(ChannelOption.SO_KEEPALIVE, true)
                            .handler(new ChannelInitializer<SocketChannel>() {
                                @Override
                                protected void initChannel(SocketChannel ch) {
                                    ch.pipeline()
                                            .addLast(new ByteArrayDecoder())
                                            .addLast(new ByteArrayEncoder())
                                            .addLast(new SlaveHandler(ReplicationManager.this));
                                }
                            });
                    
                    ChannelFuture future = bootstrap.connect(masterHost, masterPort).sync();
                    masterChannel = future.channel();
                    
                    sendSyncCommand();
                    
                    logger.info("Connected to master {}:{}", masterHost, masterPort);
                } catch (Exception e) {
                    logger.error("Failed to connect to master", e);
                }
            }
        }, 0, 5, TimeUnit.SECONDS);
    }
    
    /**
     * 发送同步命令
     * 
     * 【命令序列】
     * 1. REPLCONF listening-port <port> - 告知从节点端口
     * 2. SYNC <offset> - 请求同步数据
     */
    private void sendSyncCommand() {
        if (masterChannel != null && masterChannel.isActive()) {
            // 发送端口信息
            String syncCmd = "*3\r\n$8\r\nREPLCONF\r\n$10\r\nlistening-port\r\n$" + 
                    String.valueOf(config.getPort()).length() + "\r\n" + config.getPort() + "\r\n";
            masterChannel.writeAndFlush(syncCmd.getBytes(StandardCharsets.UTF_8));
            
            // 发送同步请求
            String syncCmd2 = "*2\r\n$4\r\nSYNC\r\n$" + String.valueOf(masterReplOffset).length() + 
                    "\r\n" + masterReplOffset + "\r\n";
            masterChannel.writeAndFlush(syncCmd2.getBytes(StandardCharsets.UTF_8));
        }
    }
    
    /**
     * 启动心跳检测（主节点调用）
     * 
     * 定期向从节点发送PING命令
     */
    private void startHeartbeat() {
        scheduler.scheduleAtFixedRate(() -> {
            for (ChannelHandlerContext ctx : slaves) {
                if (ctx.channel().isActive()) {
                    String ping = "+PING\r\n";
                    ctx.writeAndFlush(ping.getBytes(StandardCharsets.UTF_8));
                }
            }
        }, 1, 1, TimeUnit.SECONDS);
    }
    
    /**
     * 添加从节点连接（主节点调用）
     * 
     * @param ctx Netty连接上下文
     */
    public void addSlave(ChannelHandlerContext ctx) {
        slaves.add(ctx);
        logger.info("Slave connected: {}", ctx.channel().remoteAddress());
        
        // 发送全量同步数据
        sendFullSync(ctx);
    }
    
    /**
     * 移除从节点连接
     */
    public void removeSlave(ChannelHandlerContext ctx) {
        slaves.remove(ctx);
        logger.info("Slave disconnected: {}", ctx.channel().remoteAddress());
    }
    
    /**
     * 发送全量同步数据
     * 
     * 【全量同步流程】
     * 1. 发送FULLSYNC命令（包含复制ID和偏移量）
     * 2. 遍历所有数据库
     * 3. 发送所有键值对
     */
    private void sendFullSync(ChannelHandlerContext ctx) {
        try {
            StringBuilder sb = new StringBuilder();
            
            // 发送同步信息
            sb.append("*5\r\n$4\r\nFULLSYNC\r\n");
            sb.append("$").append(masterReplId.length()).append("\r\n").append(masterReplId).append("\r\n");
            sb.append("$").append(String.valueOf(masterReplOffset).length()).append("\r\n").append(masterReplOffset).append("\r\n");
            
            // 遍历所有数据库
            for (int dbIndex = 0; dbIndex < database.getDbCount(); dbIndex++) {
                Map<String, RedisObject> dbData = database.getDb(dbIndex);
                if (dbData == null || dbData.isEmpty()) continue;
                
                // 发送SELECT命令
                String selectCmd = "*2\r\n$6\r\nSELECT\r\n$" + String.valueOf(dbIndex).length() + 
                        "\r\n" + dbIndex + "\r\n";
                sb.append(selectCmd);
                
                // 发送所有键值对
                for (Map.Entry<String, RedisObject> entry : dbData.entrySet()) {
                    String key = entry.getKey();
                    RedisObject obj = entry.getValue();
                    
                    if (obj.isExpired()) continue;
                    
                    String cmd = generateSetCommand(key, obj);
                    if (cmd != null) {
                        sb.append(cmd);
                    }
                }
            }
            
            ctx.writeAndFlush(sb.toString().getBytes(StandardCharsets.UTF_8));
            
        } catch (Exception e) {
            logger.error("Failed to send full sync", e);
        }
    }
    
    /**
     * 生成SET命令
     * 
     * 根据对象类型生成对应的命令
     */
    private String generateSetCommand(String key, RedisObject obj) {
        StringBuilder sb = new StringBuilder();
        
        switch (obj.getType()) {
            case STRING:
                String value = obj.getStringValue();
                sb.append("*3\r\n$3\r\nSET\r\n");
                sb.append("$").append(key.length()).append("\r\n").append(key).append("\r\n");
                sb.append("$").append(value.length()).append("\r\n").append(value).append("\r\n");
                break;
                
            case LIST:
                RedisList list = obj.getValue();
                List<String> items = list.getAll();
                if (items.isEmpty()) return null;
                sb.append("*").append(items.size() + 2).append("\r\n$5\r\nRPUSH\r\n");
                sb.append("$").append(key.length()).append("\r\n").append(key).append("\r\n");
                for (String item : items) {
                    sb.append("$").append(item.length()).append("\r\n").append(item).append("\r\n");
                }
                break;
                
            case HASH:
                RedisHash hash = obj.getValue();
                Map<String, String> hashData = hash.hgetall();
                if (hashData.isEmpty()) return null;
                int hashCount = hashData.size() * 2 + 2;
                sb.append("*").append(hashCount).append("\r\n$4\r\nHMSET\r\n");
                sb.append("$").append(key.length()).append("\r\n").append(key).append("\r\n");
                for (Map.Entry<String, String> entry : hashData.entrySet()) {
                    sb.append("$").append(entry.getKey().length()).append("\r\n").append(entry.getKey()).append("\r\n");
                    sb.append("$").append(entry.getValue().length()).append("\r\n").append(entry.getValue()).append("\r\n");
                }
                break;
                
            case SET:
                RedisSet set = obj.getValue();
                java.util.Set<String> members = set.smembers();
                if (members.isEmpty()) return null;
                int setCount = members.size() + 2;
                sb.append("*").append(setCount).append("\r\n$4\r\nSADD\r\n");
                sb.append("$").append(key.length()).append("\r\n").append(key).append("\r\n");
                for (String member : members) {
                    sb.append("$").append(member.length()).append("\r\n").append(member).append("\r\n");
                }
                break;
                
            case ZSET:
                RedisZSet zset = obj.getValue();
                List<RedisZSet.ZSetEntry> entries = zset.zrange(0, -1);
                if (entries.isEmpty()) return null;
                int zsetCount = entries.size() * 2 + 2;
                sb.append("*").append(zsetCount).append("\r\n$4\r\nZADD\r\n");
                sb.append("$").append(key.length()).append("\r\n").append(key).append("\r\n");
                for (RedisZSet.ZSetEntry entry : entries) {
                    String scoreStr = String.valueOf(entry.score);
                    sb.append("$").append(scoreStr.length()).append("\r\n").append(scoreStr).append("\r\n");
                    sb.append("$").append(entry.member.length()).append("\r\n").append(entry.member).append("\r\n");
                }
                break;
        }
        
        return sb.toString();
    }
    
    /**
     * 传播写命令到从节点（主节点调用）
     * 
     * 【命令传播】
     * 主节点执行写命令后，将命令发送给所有从节点
     * 从节点执行相同的命令，保持数据一致
     * 
     * @param command 命令字符串
     */
    public void propagateCommand(String command) {
        if (role != Role.MASTER) return;
        
        masterReplOffset++;
        
        byte[] data = encodeCommand(command);
        for (ChannelHandlerContext ctx : slaves) {
            if (ctx.channel().isActive()) {
                ctx.writeAndFlush(data);
            }
        }
    }
    
    /**
     * 编码命令为RESP格式
     */
    private byte[] encodeCommand(String command) {
        String[] parts = command.split(" ");
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(parts.length).append("\r\n");
        for (String part : parts) {
            sb.append("$").append(part.length()).append("\r\n");
            sb.append(part).append("\r\n");
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }
    
    /**
     * 处理主节点发送的数据（从节点调用）
     * 
     * @param data RESP格式的命令数据
     */
    public void handleMasterData(byte[] data) {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            RESPParser parser = new RESPParser(bais);
            
            while (bais.available() > 0) {
                RESPObject obj = parser.parse();
                if (obj != null) {
                    com.learn.redis.command.CommandExecutor executor = 
                            new com.learn.redis.command.CommandExecutor(database);
                    executor.execute(obj);
                    masterReplOffset++;
                }
            }
        } catch (Exception e) {
            logger.error("Failed to handle master data", e);
        }
    }
    
    // ==================== Getter方法 ====================
    
    public Role getRole() {
        return role;
    }
    
    public long getMasterReplOffset() {
        return masterReplOffset;
    }
    
    public String getMasterReplId() {
        return masterReplId;
    }
    
    public int getSlaveCount() {
        return slaves.size();
    }
    
    /**
     * 获取复制信息
     */
    public String getInfo() {
        StringBuilder sb = new StringBuilder();
        sb.append("# Replication\n");
        sb.append("role:").append(role == Role.MASTER ? "master" : "slave").append("\n");
        
        if (role == Role.MASTER) {
            sb.append("connected_slaves:").append(slaves.size()).append("\n");
            sb.append("master_replid:").append(masterReplId).append("\n");
            sb.append("master_repl_offset:").append(masterReplOffset).append("\n");
        } else {
            sb.append("master_host:").append(masterHost).append("\n");
            sb.append("master_port:").append(masterPort).append("\n");
            sb.append("master_link_status:").append(masterChannel != null && masterChannel.isActive() ? "up" : "down").append("\n");
            sb.append("slave_repl_offset:").append(masterReplOffset).append("\n");
        }
        
        return sb.toString();
    }
}
