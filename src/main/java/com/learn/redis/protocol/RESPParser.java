package com.learn.redis.protocol;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * RESPParser - RESP协议解析器
 * 
 * 【RESP协议简介】
 * RESP (REdis Serialization Protocol) 是Redis使用的序列化协议。
 * 它是一种简单、高效、可读性强的二进制安全协议。
 * 
 * 【协议特点】
 * 1. 简单：易于实现和调试
 * 2. 高效：使用前缀标识类型，解析速度快
 * 3. 二进制安全：可以传输任意二进制数据
 * 4. 可读：纯文本协议，便于调试
 * 
 * 【数据类型】
 * RESP支持5种数据类型：
 * 1. Simple String（简单字符串）：+OK\r\n
 * 2. Error（错误）：-ERR message\r\n
 * 3. Integer（整数）：:1000\r\n
 * 4. Bulk String（批量字符串）：$5\r\nhello\r\n
 * 5. Array（数组）：*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
 * 
 * 【协议格式】
 * 每个RESP数据都以类型标识符开头：
 * - '+' 表示简单字符串
 * - '-' 表示错误
 * - ':' 表示整数
 * - '$' 表示批量字符串
 * - '*' 表示数组
 * 
 * 每行以CRLF（\r\n）结尾
 * 
 * 【示例】
 * SET命令的RESP表示：
 * *3\r\n          - 数组，包含3个元素
 * $3\r\n          - 批量字符串，长度3
 * SET\r\n         - 内容"SET"
 * $3\r\n          - 批量字符串，长度3
 * key\r\n         - 内容"key"
 * $5\r\n          - 批量字符串，长度5
 * value\r\n       - 内容"value"
 */
public class RESPParser {
    
    /** CRLF常量：RESP协议的行结束符 */
    private static final String CRLF = "\r\n";
    
    /** 类型标识符常量 */
    public static final byte SIMPLE_STRING = '+';   // 简单字符串
    public static final byte ERROR = '-';            // 错误
    public static final byte INTEGER = ':';          // 整数
    public static final byte BULK_STRING = '$';      // 批量字符串
    public static final byte ARRAY = '*';            // 数组
    
    /** 输入流：从客户端读取数据 */
    private final InputStream input;
    
    /**
     * 构造函数
     * 
     * @param input 输入流，用于读取客户端发送的数据
     */
    public RESPParser(InputStream input) {
        this.input = input;
    }
    
    /**
     * 解析RESP数据
     * 
     * 【解析流程】
     * 1. 读取第一个字节判断类型
     * 2. 根据类型调用相应的解析方法
     * 
     * @return 解析后的RESPObject对象
     * @throws IOException 如果读取失败
     */
    public RESPObject parse() throws IOException {
        // 读取类型标识符
        int type = input.read();
        if (type == -1) {
            return null;  // 连接关闭
        }
        
        // 根据类型分发到对应的解析方法
        switch (type) {
            case SIMPLE_STRING:
                return parseSimpleString();
            case ERROR:
                return parseError();
            case INTEGER:
                return parseInteger();
            case BULK_STRING:
                return parseBulkString();
            case ARRAY:
                return parseArray();
            default:
                throw new IOException("Unknown RESP type: " + (char) type);
        }
    }
    
    /**
     * 解析简单字符串
     * 
     * 【格式】+<string>\r\n
     * 例如：+OK\r\n
     * 
     * 简单字符串不能包含\r或\n
     * 通常用于服务器返回简单的状态信息
     * 
     * @return 简单字符串对象
     * @throws IOException 如果读取失败
     */
    private RESPObject parseSimpleString() throws IOException {
        String line = readLine();
        return new RESPObject(RESPObject.Type.SIMPLE_STRING, line);
    }
    
    /**
     * 解析错误消息
     * 
     * 【格式】-<error type> <error message>\r\n
     * 例如：-ERR unknown command 'xyz'\r\n
     *       -WRONGTYPE Operation against a key holding the wrong kind of value\r\n
     * 
     * 错误类型通常是：
     * - ERR：一般错误
     * - WRONGTYPE：类型错误
     * - MOVED：集群重定向
     * - ASK：集群询问重定向
     * 
     * @return 错误对象
     * @throws IOException 如果读取失败
     */
    private RESPObject parseError() throws IOException {
        String line = readLine();
        return new RESPObject(RESPObject.Type.ERROR, line);
    }
    
    /**
     * 解析整数
     * 
     * 【格式】:<integer>\r\n
     * 例如：:1000\r\n
     *       :-100\r\n
     * 
     * 用于返回整数值，如INCR的结果、LLEN的结果等
     * 
     * @return 整数对象
     * @throws IOException 如果读取失败
     */
    private RESPObject parseInteger() throws IOException {
        String line = readLine();
        return new RESPObject(RESPObject.Type.INTEGER, Long.parseLong(line));
    }
    
    /**
     * 解析批量字符串
     * 
     * 【格式】$<length>\r\n<data>\r\n
     * 例如：$5\r\nhello\r\n
     * 
     * 【特殊值】
     * $-1\r\n 表示null（键不存在）
     * $0\r\n\r\n 表示空字符串
     * 
     * 批量字符串是二进制安全的，可以包含任意字节
     * 包括\r和\n，因为长度是明确指定的
     * 
     * @return 批量字符串对象
     * @throws IOException 如果读取失败
     */
    private RESPObject parseBulkString() throws IOException {
        // 读取长度
        String lenStr = readLine();
        int len = Integer.parseInt(lenStr);
        
        // 处理null值
        if (len == -1) {
            return new RESPObject(RESPObject.Type.BULK_STRING, null);
        }
        
        // 读取指定长度的数据
        byte[] data = new byte[len];
        int read = 0;
        while (read < len) {
            int n = input.read(data, read, len - read);
            if (n == -1) {
                throw new IOException("Unexpected end of stream");
            }
            read += n;
        }
        
        // 读取并验证CRLF
        int cr = input.read();
        int lf = input.read();
        if (cr != '\r' || lf != '\n') {
            throw new IOException("Expected CRLF after bulk string");
        }
        
        // 转换为字符串
        return new RESPObject(RESPObject.Type.BULK_STRING, new String(data, StandardCharsets.UTF_8));
    }
    
    /**
     * 解析数组
     * 
     * 【格式】*<count>\r\n<element1><element2>...
     * 例如：*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
     * 
     * 【特殊值】
     * *-1\r\n 表示null数组
     * *0\r\n 表示空数组
     * 
     * 数组可以包含任意类型的元素，包括嵌套数组
     * 
     * @return 数组对象
     * @throws IOException 如果读取失败
     */
    private RESPObject parseArray() throws IOException {
        // 读取元素数量
        String lenStr = readLine();
        int len = Integer.parseInt(lenStr);
        
        // 处理null数组
        if (len == -1) {
            return new RESPObject(RESPObject.Type.ARRAY, null);
        }
        
        // 递归解析每个元素
        List<RESPObject> elements = new ArrayList<>();
        for (int i = 0; i < len; i++) {
            elements.add(parse());
        }
        
        return new RESPObject(RESPObject.Type.ARRAY, elements);
    }
    
    /**
     * 读取一行（直到CRLF）
     * 
     * 【实现说明】
     * RESP协议使用CRLF作为行结束符
     * 这个方法读取直到遇到\r\n
     * 
     * @return 行内容（不包含CRLF）
     * @throws IOException 如果读取失败
     */
    private String readLine() throws IOException {
        StringBuilder sb = new StringBuilder();
        int c;
        while ((c = input.read()) != -1) {
            if (c == '\r') {
                // 检查下一个字符是否是\n
                c = input.read();
                if (c == '\n') {
                    break;  // 找到CRLF，行结束
                }
                // 不是CRLF，继续
                sb.append('\r');
                if (c != -1) {
                    sb.append((char) c);
                }
            } else {
                sb.append((char) c);
            }
        }
        return sb.toString();
    }
    
    // ==================== 编码方法 ====================
    
    /**
     * 编码简单字符串
     * 
     * @param value 字符串值
     * @return 编码后的字节数组
     */
    public static byte[] encodeSimpleString(String value) {
        return ("+" + value + CRLF).getBytes(StandardCharsets.UTF_8);
    }
    
    /**
     * 编码错误消息
     * 
     * @param message 错误消息
     * @return 编码后的字节数组
     */
    public static byte[] encodeError(String message) {
        return ("-" + message + CRLF).getBytes(StandardCharsets.UTF_8);
    }
    
    /**
     * 编码整数
     * 
     * @param value 整数值
     * @return 编码后的字节数组
     */
    public static byte[] encodeInteger(long value) {
        return (":" + value + CRLF).getBytes(StandardCharsets.UTF_8);
    }
    
    /**
     * 编码批量字符串
     * 
     * @param value 字符串值，null表示不存在的键
     * @return 编码后的字节数组
     */
    public static byte[] encodeBulkString(String value) {
        if (value == null) {
            // null值：$-1\r\n
            return ("$-1" + CRLF).getBytes(StandardCharsets.UTF_8);
        }
        byte[] data = value.getBytes(StandardCharsets.UTF_8);
        return ("$" + data.length + CRLF + value + CRLF).getBytes(StandardCharsets.UTF_8);
    }
    
    /**
     * 编码数组
     * 
     * @param elements 数组元素列表
     * @return 编码后的字节数组
     */
    public static byte[] encodeArray(List<RESPObject> elements) {
        if (elements == null) {
            return ("*-1" + CRLF).getBytes(StandardCharsets.UTF_8);
        }
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(elements.size()).append(CRLF);
        for (RESPObject element : elements) {
            sb.append(new String(encode(element), StandardCharsets.UTF_8));
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }
    
    /**
     * 编码任意RESP对象
     * 
     * @param obj RESP对象
     * @return 编码后的字节数组
     */
    @SuppressWarnings("unchecked")
    public static byte[] encode(RESPObject obj) {
        switch (obj.getType()) {
            case SIMPLE_STRING:
                return encodeSimpleString((String) obj.getValue());
            case ERROR:
                return encodeError((String) obj.getValue());
            case INTEGER:
                return encodeInteger((Long) obj.getValue());
            case BULK_STRING:
                return encodeBulkString((String) obj.getValue());
            case ARRAY:
                return encodeArray((List<RESPObject>) obj.getValue());
            default:
                return encodeError("ERR unknown type");
        }
    }
    
    /**
     * 编码字符串数组
     * 
     * 用于快速编码MGET、KEYS等命令的结果
     * 
     * @param strings 字符串列表
     * @return 编码后的字节数组
     */
    public static byte[] encodeStringArray(List<String> strings) {
        if (strings == null) {
            return ("*-1" + CRLF).getBytes(StandardCharsets.UTF_8);
        }
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(strings.size()).append(CRLF);
        for (String s : strings) {
            if (s == null) {
                sb.append("$-1").append(CRLF);
            } else {
                byte[] data = s.getBytes(StandardCharsets.UTF_8);
                sb.append("$").append(data.length).append(CRLF).append(s).append(CRLF);
            }
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }
    
    /**
     * 编码null数组
     */
    public static byte[] encodeNullArray() {
        return ("*-1" + CRLF).getBytes(StandardCharsets.UTF_8);
    }
    
    /**
     * 编码null批量字符串
     */
    public static byte[] encodeNullBulkString() {
        return ("$-1" + CRLF).getBytes(StandardCharsets.UTF_8);
    }
    
    /**
     * 编码OK响应
     */
    public static byte[] encodeOK() {
        return encodeSimpleString("OK");
    }
}
