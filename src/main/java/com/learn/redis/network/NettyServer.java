package com.learn.redis.network;

import com.learn.redis.data.RedisDatabase;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.bytes.ByteArrayDecoder;
import io.netty.handler.codec.bytes.ByteArrayEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NettyServer - 基于Netty的Redis服务器
 * 
 * 【Netty框架简介】
 * Netty是一个高性能的异步事件驱动网络框架，用于快速开发可维护的高性能协议服务器和客户端。
 * 
 * 【为什么使用Netty】
 * 1. 高性能：基于Java NIO，使用零拷贝、内存池等技术
 * 2. 异步非阻塞：使用事件循环模型，少量线程处理大量连接
 * 3. 成熟稳定：解决了Java NIO的很多问题（如epoll bug）
 * 4. 易于使用：提供了丰富的编解码器和处理器
 * 
 * 【Redis的IO模型】
 * Redis使用单线程事件循环模型：
 * - 单个线程处理所有客户端连接
 * - 使用IO多路复用（epoll/kqueue）
 * - 避免多线程的锁竞争开销
 * 
 * 本实现使用Netty模拟类似模型：
 * - bossGroup: 接受新连接（1个线程）
 * - workerGroup: 处理IO事件（多个线程，但命令执行是单线程）
 * 
 * 【服务器启动流程】
 * 1. 创建EventLoopGroup（事件循环组）
 * 2. 配置ServerBootstrap（服务器启动器）
 * 3. 设置Channel类型（NioServerSocketChannel）
 * 4. 配置ChannelPipeline（处理链）
 * 5. 绑定端口并启动
 * 
 * 【ChannelPipeline】
 * 每个客户端连接都有一个ChannelPipeline，包含多个处理器：
 * - ByteArrayDecoder: 将字节流解码为字节数组
 * - ByteArrayEncoder: 将字节数组编码为字节流
 * - RedisClientHandler: 处理Redis命令
 */
public class NettyServer {
    private static final Logger logger = LoggerFactory.getLogger(NettyServer.class);
    
    /**
     * 监听端口
     */
    private final int port;
    
    /**
     * 数据库实例
     */
    private final RedisDatabase database;
    
    /**
     * Boss线程组
     * 
     * 负责接受新的客户端连接。
     * 只需要1个线程，因为连接建立后交给workerGroup处理。
     */
    private EventLoopGroup bossGroup;
    
    /**
     * Worker线程组
     * 
     * 负责处理已建立连接的IO事件。
     * 默认线程数为CPU核心数*2。
     */
    private EventLoopGroup workerGroup;
    
    /**
     * 服务器Channel
     * 
     * 代表服务器本身的通道，用于接受新连接。
     */
    private Channel channel;
    
    /**
     * 构造函数
     * 
     * @param port 监听端口
     * @param database 数据库实例
     */
    public NettyServer(int port, RedisDatabase database) {
        this.port = port;
        this.database = database;
    }
    
    /**
     * 启动服务器
     * 
     * 【启动流程】
     * 1. 创建EventLoopGroup
     * 2. 配置ServerBootstrap
     * 3. 设置Channel选项
     * 4. 配置ChannelPipeline
     * 5. 绑定端口
     * 6. 等待服务器关闭
     * 
     * 【Channel选项说明】
     * - SO_BACKLOG: 等待连接队列的最大长度
     * - SO_KEEPALIVE: 启用TCP Keep-Alive
     * - TCP_NODELAY: 禁用Nagle算法，减少延迟
     * 
     * @throws Exception 如果启动失败
     */
    public void start() throws Exception {
        // 创建Boss线程组（接受连接）
        bossGroup = new NioEventLoopGroup(1);
        
        // 创建Worker线程组（处理IO）
        workerGroup = new NioEventLoopGroup();
        
        try {
            // 创建服务器启动器
            ServerBootstrap bootstrap = new ServerBootstrap();
            
            bootstrap.group(bossGroup, workerGroup)
                    // 使用NIO类型的ServerSocketChannel
                    .channel(NioServerSocketChannel.class)
                    // 设置等待连接队列长度
                    .option(ChannelOption.SO_BACKLOG, 128)
                    // 保持连接活跃
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    // 禁用Nagle算法，减少延迟
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    // 配置ChannelPipeline
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline pipeline = ch.pipeline();
                            
                            // 添加字节解码器
                            pipeline.addLast("decoder", new ByteArrayDecoder());
                            
                            // 添加字节编码器
                            pipeline.addLast("encoder", new ByteArrayEncoder());
                            
                            // 添加Redis命令处理器
                            pipeline.addLast("handler", new RedisClientHandler(database));
                        }
                    });
            
            // 绑定端口并启动服务器
            ChannelFuture future = bootstrap.bind(port).sync();
            channel = future.channel();
            
            logger.info("Redis server started on port {}", port);
            
            // 等待服务器Channel关闭
            // 这是一个阻塞操作，服务器会一直运行直到被关闭
            channel.closeFuture().sync();
            
        } finally {
            // 确保在退出时关闭所有资源
            shutdown();
        }
    }
    
    /**
     * 关闭服务器
     * 
     * 【关闭流程】
     * 1. 关闭服务器Channel
     * 2. 优雅关闭Boss线程组
     * 3. 优雅关闭Worker线程组
     * 
     * 优雅关闭（shutdownGracefully）会：
     * - 停止接受新连接
     * - 处理完已接受的请求
     * - 然后关闭所有连接
     */
    public void shutdown() {
        // 关闭服务器Channel
        if (channel != null) {
            channel.close();
        }
        
        // 优雅关闭Boss线程组
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        
        // 优雅关闭Worker线程组
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        
        logger.info("Redis server shutdown complete");
    }
}
