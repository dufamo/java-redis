package com.learn.redis.sentinel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * SentinelClient - 哨兵客户端
 * 
 * 【功能说明】
 * 哨兵客户端用于哨兵与Redis节点或其他哨兵之间的通信。
 * 它实现了基本的RESP协议，可以发送命令并接收响应。
 * 
 * 【使用场景】
 * 1. PING主节点/从节点，检测是否存活
 * 2. 发送INFO命令获取节点信息
 * 3. 向其他哨兵询问主节点状态
 * 4. 请求其他哨兵投票
 * 5. 发送故障转移命令
 * 
 * 【连接管理】
 * - 使用阻塞式Socket连接
 * - 按需建立连接（懒连接）
 * - 支持超时设置
 * - 使用完毕后需要手动关闭
 * 
 * 【RESP协议实现】
 * 实现了RESP协议的基本编码和解码：
 * - 简单字符串（+）
 * - 错误消息（-）
 * - 整数（:）
 * - 批量字符串（$）
 * - 数组（*）
 */
public class SentinelClient {
    private static final Logger logger = LoggerFactory.getLogger(SentinelClient.class);
    
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
     * 构造函数
     * 
     * 注意：构造函数不会立即建立连接，而是在第一次使用时连接（懒加载）
     * 
     * @param host 目标主机地址
     * @param port 目标端口
     */
    public SentinelClient(String host, int port) {
        this.host = host;
        this.port = port;
    }
    
    /**
     * 建立连接
     * 
     * 如果连接不存在或已关闭，则建立新连接。
     * 设置5秒超时，避免长时间等待。
     * 
     * @throws IOException 如果连接失败
     */
    private void connect() throws IOException {
        if (socket == null || socket.isClosed()) {
            socket = new Socket(host, port);
            
            // 设置读取超时为5秒
            // 这样可以避免在节点无响应时无限等待
            socket.setSoTimeout(5000);
            
            output = socket.getOutputStream();
            input = socket.getInputStream();
        }
    }
    
    /**
     * PING命令 - 检测节点是否存活
     * 
     * 【实现细节】
     * 1. 发送PING命令
     * 2. 等待PONG响应
     * 3. 如果收到PONG，返回true
     * 4. 如果超时或出错，返回false
     * 
     * @return 节点存活返回true，否则返回false
     */
    public boolean ping() {
        try {
            connect();
            sendCommand("PING");
            String response = readLine();
            
            // 检查是否收到PONG响应
            // "+PONG"是RESP简单字符串格式
            // "PONG"是没有类型标识符的响应（某些实现）
            return "+PONG".equals(response) || "PONG".equals(response);
        } catch (Exception e) {
            return false;
        }
    }
    
    /**
     * INFO命令 - 获取节点信息
     * 
     * 【INFO命令】
     * INFO命令返回服务器的各种信息和统计数据。
     * 常用的section参数：
     * - server: 服务器信息
     * - replication: 复制信息（用于发现从节点）
     * - memory: 内存信息
     * - stats: 统计信息
     * 
     * @param section 信息类型（如"replication"）
     * @return 信息内容，失败返回null
     */
    public String getInfo(String section) {
        try {
            connect();
            sendCommand("INFO", section);
            return readBulkString();
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * 询问其他哨兵是否认为主节点下线
     * 
     * 【SENTINEL IS-MASTER-DOWN-BY-ADDR命令】
     * 用于哨兵之间的通信，询问其他哨兵是否认为指定的主节点已下线。
     * 
     * 参数说明：
     * - masterName: 主节点名称
     * - ip: 主节点IP（这里用0表示使用已知的IP）
     * - port: 主节点端口（这里用*表示使用已知的端口）
     * 
     * @param masterName 主节点名称
     * @return true表示认为下线，false表示认为在线
     */
    public boolean askMasterDown(String masterName) {
        try {
            connect();
            sendCommand("SENTINEL", "IS-MASTER-DOWN-BY-ADDR", masterName, "0", "*");
            String response = readLine();
            
            // 响应中包含"1"表示下线
            return response != null && response.contains("1");
        } catch (Exception e) {
            return false;
        }
    }
    
    /**
     * 请求投票
     * 
     * 【Raft选举】
     * 在故障转移时，哨兵需要获得多数票才能成为领导者。
     * 这个方法向其他哨兵请求投票。
     * 
     * 参数说明：
     * - masterName: 主节点名称
     * - epoch: 选举纪元，每次选举递增
     * 
     * @param masterName 主节点名称
     * @param epoch 选举纪元
     * @return 返回投票给的哨兵ID，失败返回null
     */
    public String requestVote(String masterName, int epoch) {
        try {
            connect();
            sendCommand("SENTINEL", "VOTE", masterName, String.valueOf(epoch));
            return readBulkString();
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * 发送RESP格式命令
     * 
     * 【RESP协议格式】
     * 命令使用RESP数组格式发送：
     * *<参数数量>\r\n
     * $<参数1长度>\r\n
     * <参数1>\r\n
     * $<参数2长度>\r\n
     * <参数2>\r\n
     * ...
     * 
     * 例如：SET key value
     * *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
     * 
     * @param args 命令参数
     * @throws IOException 如果发送失败
     */
    public void sendCommand(String... args) throws IOException {
        connect();
        
        StringBuilder sb = new StringBuilder();
        
        // 写入数组长度
        sb.append("*").append(args.length).append("\r\n");
        
        // 写入每个参数
        for (String arg : args) {
            sb.append("$").append(arg.length()).append("\r\n");
            sb.append(arg).append("\r\n");
        }
        
        // 发送数据
        output.write(sb.toString().getBytes(StandardCharsets.UTF_8));
        output.flush();
    }
    
    /**
     * 读取一行（直到CRLF）
     * 
     * RESP协议使用CRLF（\r\n）作为行结束符。
     * 这个方法读取直到遇到\r\n，返回行内容（不包含CRLF）。
     * 
     * @return 行内容
     * @throws IOException 如果读取失败
     */
    private String readLine() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int c;
        
        while ((c = input.read()) != -1) {
            if (c == '\r') {
                // 读到\r，检查下一个是否是\n
                c = input.read();
                if (c == '\n') {
                    // 找到\r\n，行结束
                    break;
                }
                // 不是\r\n，把\r写回
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
     * 【批量字符串格式】
     * $<长度>\r\n
     * <数据>\r\n
     * 
     * 特殊值：
     * $-1\r\n 表示null
     * $0\r\n\r\n 表示空字符串
     * 
     * @return 字符串内容，null值返回null
     * @throws IOException 如果读取失败
     */
    private String readBulkString() throws IOException {
        // 读取长度行
        String line = readLine();
        
        if (line == null || !line.startsWith("$")) {
            return null;
        }
        
        // 解析长度
        int len = Integer.parseInt(line.substring(1));
        
        // $-1表示null
        if (len < 0) {
            return null;
        }
        
        // 读取指定长度的数据
        byte[] data = new byte[len];
        int read = 0;
        while (read < len) {
            int n = input.read(data, read, len - read);
            if (n == -1) break;
            read += n;
        }
        
        // 读取末尾的CRLF
        input.read();  // \r
        input.read();  // \n
        
        return new String(data, StandardCharsets.UTF_8);
    }
    
    /**
     * 关闭连接
     * 
     * 释放Socket资源。使用完毕后必须调用此方法。
     */
    public void close() {
        try {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        } catch (IOException e) {
            logger.debug("Failed to close socket", e);
        }
    }
}
