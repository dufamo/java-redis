package com.learn.redis.protocol;

import java.util.List;

/**
 * RESPObject - RESP协议数据对象
 * 
 * 【功能说明】
 * RESPObject是RESP协议中所有数据类型的统一封装。
 * 它可以表示RESP协议的5种基本数据类型。
 * 
 * 【RESP数据类型】
 * 1. Simple String（简单字符串）
 *    - 格式：+OK\r\n
 *    - 用于简单的状态回复
 * 
 * 2. Error（错误）
 *    - 格式：-ERR message\r\n
 *    - 用于返回错误信息
 * 
 * 3. Integer（整数）
 *    - 格式：:1000\r\n
 *    - 用于返回整数值
 * 
 * 4. Bulk String（批量字符串）
 *    - 格式：$5\r\nhello\r\n
 *    - 用于返回字符串，支持二进制安全
 * 
 * 5. Array（数组）
 *    - 格式：*2\r\n...元素...
 *    - 用于返回多个值，可嵌套
 * 
 * 【设计模式】
 * 使用组合模式（Composite Pattern）：
 * - 单一值类型（Simple String、Error、Integer、Bulk String）
 * - 复合类型（Array，可包含其他RESPObject）
 * 
 * 【使用示例】
 * // 创建简单字符串
 * RESPObject ok = new RESPObject(Type.SIMPLE_STRING, "OK");
 * 
 * // 创建整数
 * RESPObject count = new RESPObject(Type.INTEGER, 100L);
 * 
 * // 创建数组
 * List<RESPObject> elements = new ArrayList<>();
 * elements.add(new RESPObject(Type.BULK_STRING, "value1"));
 * elements.add(new RESPObject(Type.BULK_STRING, "value2"));
 * RESPObject array = new RESPObject(Type.ARRAY, elements);
 */
public class RESPObject {
    
    /**
     * RESP数据类型枚举
     */
    public enum Type {
        /**
         * 简单字符串
         * 用于简单的状态回复，如"+OK\r\n"
         */
        SIMPLE_STRING,
        
        /**
         * 错误消息
         * 用于返回错误，如"-ERR unknown command\r\n"
         */
        ERROR,
        
        /**
         * 整数
         * 用于返回整数值，如":1000\r\n"
         */
        INTEGER,
        
        /**
         * 批量字符串
         * 用于返回字符串值，如"$5\r\nhello\r\n"
         * 支持二进制安全，可以包含任意字节
         */
        BULK_STRING,
        
        /**
         * 数组
         * 用于返回多个值，如"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
         * 数组可以嵌套，元素可以是任意类型
         */
        ARRAY
    }
    
    /**
     * 数据类型
     */
    private final Type type;
    
    /**
     * 数据值
     * 
     * 根据类型不同，value的实际类型也不同：
     * - SIMPLE_STRING: String
     * - ERROR: String
     * - INTEGER: Long
     * - BULK_STRING: String（可以为null）
     * - ARRAY: List<RESPObject>
     */
    private final Object value;
    
    /**
     * 构造函数
     * 
     * @param type 数据类型
     * @param value 数据值
     */
    public RESPObject(Type type, Object value) {
        this.type = type;
        this.value = value;
    }
    
    /**
     * 获取数据类型
     * 
     * @return 类型枚举值
     */
    public Type getType() {
        return type;
    }
    
    /**
     * 获取原始值
     * 
     * 返回Object类型，需要调用者根据类型进行转换。
     * 建议使用类型特定的方法（getString、getInteger、getArray）。
     * 
     * @return 原始值对象
     */
    public Object getValue() {
        return value;
    }
    
    /**
     * 获取字符串值
     * 
     * 适用于SIMPLE_STRING、ERROR、BULK_STRING类型。
     * 
     * @return 字符串值
     */
    @SuppressWarnings("unchecked")
    public String getString() {
        return (String) value;
    }
    
    /**
     * 获取整数值
     * 
     * 适用于INTEGER类型。
     * 
     * @return Long类型的整数值
     */
    @SuppressWarnings("unchecked")
    public Long getInteger() {
        return (Long) value;
    }
    
    /**
     * 获取数组值
     * 
     * 适用于ARRAY类型。
     * 返回的List中的每个元素都是RESPObject。
     * 
     * @return RESPObject列表
     */
    @SuppressWarnings("unchecked")
    public List<RESPObject> getArray() {
        return (List<RESPObject>) value;
    }
    
    /**
     * 判断是否为数组类型
     * 
     * @return 如果是数组类型返回true
     */
    public boolean isArray() {
        return type == Type.ARRAY;
    }
    
    /**
     * 判断是否为批量字符串类型
     * 
     * @return 如果是批量字符串类型返回true
     */
    public boolean isBulkString() {
        return type == Type.BULK_STRING;
    }
    
    /**
     * 判断是否为错误类型
     * 
     * @return 如果是错误类型返回true
     */
    public boolean isError() {
        return type == Type.ERROR;
    }
    
    /**
     * 字符串表示
     * 
     * 用于调试和日志输出。
     * 
     * @return 对象的字符串表示
     */
    @Override
    public String toString() {
        return "RESPObject{" +
                "type=" + type +
                ", value=" + value +
                '}';
    }
}
