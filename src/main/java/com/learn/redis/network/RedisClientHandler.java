package com.learn.redis.network;

import com.learn.redis.command.CommandExecutor;
import com.learn.redis.data.RedisDatabase;
import com.learn.redis.protocol.RESPObject;
import com.learn.redis.protocol.RESPParser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * RedisClientHandler - Redis客户端连接处理器
 * 
 * 【功能说明】
 * 处理客户端发送的Redis命令，这是Netty ChannelHandler的核心实现。
 * 每个客户端连接都会创建一个RedisClientHandler实例。
 * 
 * 【处理流程】
 * 1. 客户端发送RESP格式的命令
 * 2. 数据经过ByteArrayDecoder解码为字节数组
 * 3. channelRead方法接收数据
 * 4. 使用RESPParser解析命令
 * 5. 使用CommandExecutor执行命令
 * 6. 将响应编码后返回给客户端
 * 
 * 【缓冲区管理】
 * 使用ByteBuf累积数据的原因：
 * - TCP是流式协议，一个命令可能分多个包到达
 * - 一个包可能包含多个命令（管道）
 * - 需要正确处理数据边界
 * 
 * 【管道支持】
 * 支持Redis管道（Pipeline）：
 * - 客户端可以连续发送多个命令
 * - 服务器依次处理并返回结果
 * - 减少网络往返时间
 * 
 * 【线程模型】
 * Netty使用事件循环模型：
 * - 每个Channel绑定到一个EventLoop
 * - EventLoop单线程处理所有IO事件
 * - 命令执行在IO线程中进行
 * - 需要注意避免阻塞操作
 */
public class RedisClientHandler extends ChannelInboundHandlerAdapter {
    private static final Logger logger = LoggerFactory.getLogger(RedisClientHandler.class);
    
    /**
     * 命令执行器
     * 
     * 负责解析命令名称，调用相应的方法执行命令。
     * 每个连接有自己的CommandExecutor，共享同一个RedisDatabase。
     */
    private final CommandExecutor commandExecutor;
    
    /**
     * 数据缓冲区
     * 
     * 用于累积接收到的数据，处理TCP粘包/拆包问题。
     */
    private final ByteBuf buffer;
    
    /**
     * 构造函数
     * 
     * @param database 数据库实例，所有连接共享
     */
    public RedisClientHandler(RedisDatabase database) {
        this.commandExecutor = new CommandExecutor(database);
        this.buffer = Unpooled.buffer();
    }
    
    /**
     * 连接建立时调用
     * 
     * 当客户端成功连接到服务器时，Netty会调用此方法。
     * 可以在这里进行连接统计、认证等操作。
     * 
     * @param ctx Netty通道上下文
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        logger.info("Client connected: {}", ctx.channel().remoteAddress());
    }
    
    /**
     * 连接断开时调用
     * 
     * 当客户端断开连接时，Netty会调用此方法。
     * 可以在这里清理资源、更新统计信息等。
     * 
     * @param ctx Netty通道上下文
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        logger.info("Client disconnected: {}", ctx.channel().remoteAddress());
    }
    
    /**
     * 收到数据时调用
     * 
     * 【处理流程】
     * 1. 将收到的数据写入缓冲区
     * 2. 释放原始ByteBuf
     * 3. 尝试处理缓冲区中的所有命令
     * 
     * 【内存管理】
     * Netty使用引用计数管理ByteBuf：
     * - 收到的ByteBuf引用计数为1
     * - 使用完毕后必须释放
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
        
        // 处理缓冲区中的命令
        processCommands(ctx);
    }
    
    /**
     * 处理缓冲区中的所有命令
     * 
     * 【处理逻辑】
     * 循环尝试解析并执行命令，直到：
     * - 缓冲区数据不足（等待更多数据）
     * - 发生解析错误
     * 
     * 【管道支持】
     * 支持一次处理多个命令：
     * - 客户端可以连续发送多个命令
     * - 服务器依次处理每个命令
     * - 每个命令的结果立即返回
     * 
     * 【标记与重置】
     * 使用markReaderIndex/resetReaderIndex处理数据不足的情况：
     * 1. 标记当前位置
     * 2. 尝试解析
     * 3. 如果数据不足，重置到标记位置
     * 4. 等待更多数据到达
     * 
     * @param ctx Netty通道上下文
     */
    private void processCommands(ChannelHandlerContext ctx) throws Exception {
        while (buffer.readableBytes() > 0) {
            // 标记当前位置，以便数据不足时回退
            buffer.markReaderIndex();
            
            // 获取缓冲区中的所有数据
            byte[] data = new byte[buffer.readableBytes()];
            buffer.getBytes(buffer.readerIndex(), data);
            
            try {
                // 创建输入流用于解析
                ByteArrayInputStream bais = new ByteArrayInputStream(data);
                RESPParser parser = new RESPParser(bais);
                
                // 尝试解析命令
                RESPObject command = parser.parse();
                
                if (command == null) {
                    // 数据不足，重置并等待更多数据
                    buffer.resetReaderIndex();
                    return;
                }
                
                // 计算已消费的字节数
                int bytesConsumed = data.length - bais.available();
                
                // 跳过已处理的字节
                buffer.skipBytes(bytesConsumed);
                
                // 执行命令并获取响应
                byte[] response = commandExecutor.execute(command);
                
                // 将响应写回客户端
                ctx.writeAndFlush(Unpooled.wrappedBuffer(response));
                
            } catch (Exception e) {
                // 解析错误，重置缓冲区
                buffer.resetReaderIndex();
                break;
            }
        }
    }
    
    /**
     * 发生异常时调用
     * 
     * 当处理过程中发生异常时，Netty会调用此方法。
     * 常见的异常情况：
     * - 客户端突然断开连接
     * - 数据格式错误
     * - 命令执行异常
     * 
     * 通常的做法是记录日志并关闭连接。
     * 
     * @param ctx Netty通道上下文
     * @param cause 异常原因
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("Exception in client handler", cause);
        
        // 关闭连接
        ctx.close();
    }
}
