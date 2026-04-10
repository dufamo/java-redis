package com.learn.redis;

import com.learn.redis.config.RedisConfig;
import com.learn.redis.data.RedisDatabase;
import com.learn.redis.network.NettyServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RedisServer - Redis服务器启动类
 * 
 * 【功能说明】
 * 这是Redis服务器的主入口类，负责：
 * 1. 解析命令行参数，初始化配置
 * 2. 创建数据库实例
 * 3. 启动Netty服务器
 * 4. 处理服务器关闭信号
 * 
 * 【启动配置】
 * 支持通过命令行参数配置：
 * --port: 监听端口，默认6379
 * --databases: 数据库数量，默认16
 * --rdb-enabled: 是否启用RDB持久化，默认true
 * --aof-enabled: 是否启用AOF持久化，默认false
 * --replicaof: 主节点地址，格式host:port
 * --cluster-enabled: 是否启用集群模式，默认false
 * --sentinel-enabled: 是否启用哨兵模式，默认false
 * 
 * 【启动示例】
 * java -cp target/java-redis-1.0.0.jar com.learn.redis.RedisServer
 * java -cp target/java-redis-1.0.0.jar com.learn.redis.RedisServer --port 6380 --databases 32
 * java -cp target/java-redis-1.0.0.jar com.learn.redis.RedisServer --aof-enabled true
 * 
 * 【关闭机制】
 * 使用JVM的ShutdownHook优雅关闭服务器：
 * - 捕获SIGINT信号（Ctrl+C）
 * - 调用shutdown()方法关闭服务器
 * - 释放所有资源
 */
public class RedisServer {
    private static final Logger logger = LoggerFactory.getLogger(RedisServer.class);
    
    /**
     * Netty服务器实例
     */
    private static NettyServer server;
    
    /**
     * 主入口方法
     * 
     * @param args 命令行参数
     */
    public static void main(String[] args) {
        try {
            // 解析命令行参数，初始化配置
            RedisConfig config = RedisConfig.fromArgs(args);
            
            logger.info("Starting Java Redis server...");
            logger.info("Port: {}", config.getPort());
            logger.info("Databases: {}", config.getDatabases());
            logger.info("RDB enabled: {}", config.isRdbEnabled());
            logger.info("AOF enabled: {}", config.isAofEnabled());
            
            // 创建数据库实例
            RedisDatabase database = new RedisDatabase(config.getDatabases());
            
            // 创建Netty服务器
            server = new NettyServer(config.getPort(), database);
            
            // 注册ShutdownHook，优雅关闭服务器
            registerShutdownHook();
            
            // 启动服务器
            logger.info("Redis server starting on port {}", config.getPort());
            server.start();
            
        } catch (Exception e) {
            logger.error("Failed to start Redis server", e);
            System.exit(1);
        }
    }
    
    /**
     * 注册ShutdownHook
     * 
     * 当JVM收到终止信号时（如Ctrl+C），会调用这个钩子。
     * 这样可以确保服务器优雅关闭，释放所有资源。
     * 
     * ShutdownHook会在以下情况被调用：
     * - 正常退出（System.exit()）
     * - 用户中断（Ctrl+C）
     * - 系统关闭
     */
    private static void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Received shutdown signal, shutting down gracefully...");
            if (server != null) {
                server.shutdown();
            }
            logger.info("Shutdown complete");
        }));
        
        logger.info("Shutdown hook registered");
    }
    
    /**
     * 关闭服务器
     * 
     * 可以通过这个方法手动关闭服务器。
     * 通常由ShutdownHook自动调用。
     */
    public static void shutdown() {
        if (server != null) {
            server.shutdown();
        }
    }
}
