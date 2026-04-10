package com.learn.redis.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * ClusterConnection - 集群节点间连接
 * 
 * 【功能说明】
 * 用于集群中节点之间的通信。每个节点需要与其他节点建立连接，
 * 以交换集群状态信息、迁移数据等。
 * 
 * 【使用场景】
 * 1. 发送PING心跳检测节点存活
 * 2. 交换集群配置信息
 * 3. 槽位迁移时传输数据
 * 4. 故障检测和故障转移
 * 
 * 【连接特点】
 * - 使用阻塞式Socket连接
 * - 实现RESP协议编解码
 * - 支持多种响应类型解析
 * - 自动管理连接状态
 * 
 * 【与SentinelClient的区别】
 * SentinelClient用于哨兵与Redis节点的通信，
 * ClusterConnection用于集群节点之间的通信。
 * 两者实现类似，但使用场景不同。
 */
public class ClusterConnection {
    private static final Logger logger = LoggerFactory.getLogger(ClusterConnection.class);
    
    /**
     * 目标主机地址
     */
    private final String host;
    
    /**
     * 目标端口
     */
    private final int port;
    
    /**
     * TCP Socket连接
     */
    private Socket socket;
    
    /**
     * 输出流，用于发送命令
     */
    private OutputStream output;
    
    /**
     * 输入流，用于接收响应
     */
    private InputStream input;
    
    /**
     * 构造函数 - 建立连接
     * 
     * 与SentinelClient不同，这里在构造时就建立连接。
     * 因为集群节点间的连接通常需要长期保持。
     * 
     * @param host 目标主机地址
     * @param port 目标端口
     * @throws IOException 如果连接失败
     */
    public ClusterConnection(String host, int port) throws IOException {
        this.host = host;
        this.port = port;
        connect();
    }
    
    /**
     * 建立TCP连接
     * 
     * 设置5秒超时，避免长时间等待无响应的节点。
     * 
     * @throws IOException 如果连接失败
     */
    private void connect() throws IOException {
        socket = new Socket(host, port);
        socket.setSoTimeout(5000);
        output = socket.getOutputStream();
        input = socket.getInputStream();
    }
    
    /**
     * 检查连接状态
     * 
     * @return true表示连接正常，false表示连接断开
     */
    public boolean isConnected() {
        return socket != null && socket.isConnected() && !socket.isClosed();
    }
    
    /**
     * 发送PING命令
     * 
     * 【心跳机制】
     * 集群节点之间定期发送PING/PONG心跳：
     * 1. 检测节点是否存活
     * 2. 交换集群配置信息（Gossip协议）
     * 3. 更新节点的lastPongTime
     * 
     * @throws IOException 如果发送失败或响应异常
     */
    public void sendPing() throws IOException {
        sendCommand("PING");
        String response = readLine();
        
        // 验证PONG响应
        if (!"+PONG".equals(response) && !"PONG".equals(response)) {
            throw new IOException("Unexpected ping response: " + response);
        }
    }
    
    /**
     * 发送RESP格式命令
     * 
     * 【RESP协议格式】
     * *<参数数量>\r\n
     * $<参数1长度>\r\n<参数1>\r\n
     * $<参数2长度>\r\n<参数2>\r\n
     * ...
     * 
     * @param args 命令参数
     * @throws IOException 如果发送失败
     */
    public void sendCommand(String... args) throws IOException {
        StringBuilder sb = new StringBuilder();
        
        // 写入数组长度
        sb.append("*").append(args.length).append("\r\n");
        
        // 写入每个参数
        for (String arg : args) {
            sb.append("$").append(arg.length()).append("\r\n");
            sb.append(arg).append("\r\n");
        }
        
        output.write(sb.toString().getBytes(StandardCharsets.UTF_8));
        output.flush();
    }
    
    /**
     * 发送命令并读取响应
     * 
     * @param args 命令参数
     * @return 响应内容
     * @throws IOException 如果通信失败
     */
    public String sendAndRead(String... args) throws IOException {
        sendCommand(args);
        return readResponse();
    }
    
    /**
     * 读取响应
     * 
     * 根据响应类型标识符，调用相应的解析方法：
     * - '+': 简单字符串
     * - '-': 错误消息
     * - ':': 整数
     * - '$': 批量字符串
     * - '*': 数组
     * 
     * @return 响应内容
     * @throws IOException 如果读取失败
     */
    private String readResponse() throws IOException {
        int c = input.read();
        if (c == -1) {
            throw new IOException("Connection closed");
        }
        
        switch (c) {
            case '+':
            case '-':
            case ':':
                return readLine();
            case '$':
                return readBulkString();
            case '*':
                return readArray();
            default:
                throw new IOException("Unknown response type: " + (char) c);
        }
    }
    
    /**
     * 读取一行（直到CRLF）
     * 
     * @return 行内容
     * @throws IOException 如果读取失败
     */
    private String readLine() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int c;
        
        while ((c = input.read()) != -1) {
            if (c == '\r') {
                c = input.read();
                if (c == '\n') {
                    break;
                }
                baos.write('\r');
                if (c != -1) {
                    baos.write(c);
                }
            } else {
                baos.write(c);
            }
        }
        
        return baos.toString();
    }
    
    /**
     * 读取批量字符串
     * 
     * @return 字符串内容
     * @throws IOException 如果读取失败
     */
    private String readBulkString() throws IOException {
        String lenLine = readLine();
        int len = Integer.parseInt(lenLine);
        
        if (len < 0) {
            return null;
        }
        
        byte[] data = new byte[len];
        int read = 0;
        while (read < len) {
            int n = input.read(data, read, len - read);
            if (n == -1) break;
            read += n;
        }
        
        // 读取末尾的CRLF
        input.read();
        input.read();
        
        return new String(data, StandardCharsets.UTF_8);
    }
    
    /**
     * 读取数组响应
     * 
     * 递归读取数组中的每个元素。
     * 
     * @return 数组内容的字符串表示
     * @throws IOException 如果读取失败
     */
    private String readArray() throws IOException {
        String lenLine = readLine();
        int len = Integer.parseInt(lenLine);
        
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            if (i > 0) sb.append(",");
            sb.append(readResponse());
        }
        return sb.toString();
    }
    
    /**
     * 关闭连接
     * 
     * 释放Socket资源。
     */
    public void close() {
        try {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        } catch (IOException e) {
            logger.debug("Failed to close connection", e);
        }
    }
}
