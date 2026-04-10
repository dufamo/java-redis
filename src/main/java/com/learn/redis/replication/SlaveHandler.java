package com.learn.redis.replication;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SlaveHandler - 从节点处理器
 * 
 * 【功能说明】
 * 这是Netty的ChannelHandler，用于处理从节点与主节点之间的网络通信。
 * 当从节点连接到主节点后，所有来自主节点的数据都会通过这个处理器处理。
 * 
 * 【工作流程】
 * 1. 从节点连接到主节点
 * 2. 主节点发送同步数据（RDB快照 + 命令传播）
 * 3. SlaveHandler接收数据并交给ReplicationManager处理
 * 4. ReplicationManager解析并执行命令，更新本地数据
 * 
 * 【Netty处理模型】
 * - channelActive: 连接建立时调用
 * - channelInactive: 连接断开时调用
 * - channelRead: 收到数据时调用
 * - exceptionCaught: 发生异常时调用
 * 
 * 【缓冲区管理】
 * 使用ByteBuf累积接收到的数据，因为：
 * - TCP是流式协议，一次发送的数据可能分多次到达
 * - 一次收到的数据可能包含多个命令
 * - 需要正确处理数据边界
 */
public class SlaveHandler extends ChannelInboundHandlerAdapter {
    private static final Logger logger = LoggerFactory.getLogger(SlaveHandler.class);
    
    /**
     * 复制管理器引用
     * 用于将接收到的数据交给复制管理器处理
     */
    private final ReplicationManager replicationManager;
    
    /**
     * 数据缓冲区
     * 
     * 使用ByteBuf累积数据的原因：
     * 1. RESP协议的命令可能跨越多个TCP包
     * 2. 一个TCP包可能包含多个命令
     * 3. 需要缓冲区来处理不完整的数据
     */
    private final ByteBuf buffer;
    
    /**
     * 构造函数
     * 
     * @param replicationManager 复制管理器，用于处理接收到的命令
     */
    public SlaveHandler(ReplicationManager replicationManager) {
        this.replicationManager = replicationManager;
        this.buffer = Unpooled.buffer();
    }
    
    /**
     * 连接建立时调用
     * 
     * 当从节点成功连接到主节点时，Netty会调用此方法。
     * 此时可以开始进行数据同步。
     * 
     * @param ctx Netty通道上下文，包含连接信息
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        logger.info("Connected to master: {}", ctx.channel().remoteAddress());
    }
    
    /**
     * 连接断开时调用
     * 
     * 当与主节点的连接断开时，Netty会调用此方法。
     * ReplicationManager会自动尝试重新连接。
     * 
     * @param ctx Netty通道上下文
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        logger.info("Disconnected from master: {}", ctx.channel().remoteAddress());
    }
    
    /**
     * 收到数据时调用
     * 
     * 【处理流程】
     * 1. 将收到的数据写入缓冲区
     * 2. 释放原始ByteBuf（避免内存泄漏）
     * 3. 将缓冲区数据交给ReplicationManager处理
     * 
     * 【内存管理】
     * - Netty使用引用计数管理ByteBuf内存
     * - 收到的ByteBuf引用计数为1
     * - 使用完毕后必须释放（调用release()）
     * - 否则会导致内存泄漏
     * 
     * @param ctx Netty通道上下文
     * @param msg 收到的消息（ByteBuf类型）
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf in = (ByteBuf) msg;
        
        // 将数据写入内部缓冲区
        buffer.writeBytes(in);
        
        // 释放原始ByteBuf，避免内存泄漏
        in.release();
        
        // 读取缓冲区中的所有数据
        byte[] data = new byte[buffer.readableBytes()];
        buffer.getBytes(buffer.readerIndex(), data);
        
        // 清空缓冲区（数据已复制到data数组）
        buffer.clear();
        
        // 将数据交给复制管理器处理
        // 复制管理器会解析RESP协议并执行命令
        replicationManager.handleMasterData(data);
    }
    
    /**
     * 发生异常时调用
     * 
     * 当网络通信发生异常时，Netty会调用此方法。
     * 通常的做法是记录日志并关闭连接。
     * 
     * @param ctx Netty通道上下文
     * @param cause 异常原因
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("Exception in slave handler", cause);
        
        // 关闭连接，让ReplicationManager重新建立连接
        ctx.close();
    }
}
