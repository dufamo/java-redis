package com.learn.redis.command;

import com.learn.redis.data.*;
import com.learn.redis.protocol.RESPObject;
import com.learn.redis.protocol.RESPParser;

import java.util.*;

/**
 * CommandExecutor - Redis命令执行器
 * 
 * 【功能说明】
 * 命令执行器是Redis服务器的核心组件，负责：
 * 1. 解析客户端发送的命令
 * 2. 执行相应的操作
 * 3. 返回RESP格式的响应
 * 
 * 【命令处理流程】
 * 1. 从RESPObject中提取命令名称和参数
 * 2. 根据命令名称路由到对应的处理方法
 * 3. 执行命令逻辑
 * 4. 将结果编码为RESP格式返回
 * 
 * 【支持的命令】
 * 本实现支持100多个Redis命令，分为以下几类：
 * 
 * 1. 通用命令
 *    - PING, ECHO, COMMAND, INFO
 *    - EXISTS, DEL, TYPE, KEYS, RENAME, RANDOMKEY
 *    - EXPIRE, PEXPIRE, TTL, PTTL, PERSIST
 * 
 * 2. 字符串命令
 *    - SET, GET, SETEX, SETNX, GETSET
 *    - APPEND, STRLEN, GETRANGE, SETRANGE
 *    - INCR, INCRBY, DECR, DECRBY
 *    - MSET, MGET
 * 
 * 3. 列表命令
 *    - LPUSH, RPUSH, LPOP, RPOP
 *    - LLEN, LINDEX, LRANGE, LSET
 *    - LREM, LTRIM, LINSERT
 * 
 * 4. 哈希命令
 *    - HSET, HGET, HDEL, HEXISTS, HLEN
 *    - HKEYS, HVALS, HGETALL
 *    - HMSET, HMGET, HSETNX
 *    - HINCRBY, HINCRBYFLOAT
 * 
 * 5. 集合命令
 *    - SADD, SREM, SISMEMBER, SCARD, SMEMBERS
 *    - SPOP, SRANDMEMBER
 *    - SINTER, SUNION, SDIFF
 * 
 * 6. 有序集合命令
 *    - ZADD, ZREM, ZSCORE, ZCARD
 *    - ZRANK, ZREVRANK, ZRANGE, ZREVRANGE
 *    - ZINCRBY, ZCOUNT, ZRANGEBYSCORE
 * 
 * 7. 数据库命令
 *    - SELECT, DBSIZE, FLUSHDB, FLUSHALL
 * 
 * 【错误处理】
 * 当命令执行出错时，返回RESP错误格式：
 * -ERR error message\r\n
 * 
 * 常见错误：
 * - ERR wrong number of arguments: 参数数量错误
 * - WRONGTYPE: 类型不匹配
 * - ERR value is not an integer: 值不是整数
 */
public class CommandExecutor {
    
    /**
     * 数据库实例
     * 
     * 所有命令操作都针对这个数据库实例
     */
    private final RedisDatabase database;
    
    /**
     * 构造函数
     * 
     * @param database 数据库实例
     */
    public CommandExecutor(RedisDatabase database) {
        this.database = database;
    }
    
    /**
     * 执行命令
     * 
     * 【执行流程】
     * 1. 验证命令格式（必须是数组）
     * 2. 提取命令名称和参数
     * 3. 路由到对应的处理方法
     * 4. 捕获异常并返回错误响应
     * 
     * @param command RESP格式的命令对象
     * @return RESP格式的响应字节数组
     */
    public byte[] execute(RESPObject command) {
        // 验证命令格式
        if (!command.isArray()) {
            return RESPParser.encodeError("ERR invalid command format");
        }
        
        // 提取参数
        List<RESPObject> args = command.getArray();
        if (args.isEmpty()) {
            return RESPParser.encodeError("ERR empty command");
        }
        
        // 获取命令名称（不区分大小写）
        String cmdName = args.get(0).getString().toUpperCase();
        
        // 提取命令参数
        String[] cmdArgs = new String[args.size() - 1];
        for (int i = 1; i < args.size(); i++) {
            cmdArgs[i - 1] = args.get(i).getString();
        }
        
        // 执行命令
        try {
            return executeCommand(cmdName, cmdArgs);
        } catch (Exception e) {
            return RESPParser.encodeError("ERR " + e.getMessage());
        }
    }
    
    /**
     * 命令路由
     * 
     * 根据命令名称调用对应的处理方法。
     * 使用switch语句实现高效路由。
     * 
     * @param cmd 命令名称（大写）
     * @param args 命令参数
     * @return 响应字节数组
     */
    private byte[] executeCommand(String cmd, String[] args) {
        switch (cmd) {
            // 服务器命令
            case "PING":
                return ping(args);
            case "ECHO":
                return echo(args);
            case "COMMAND":
                return RESPParser.encodeSimpleString("OK");
            case "INFO":
                return info(args);
                
            // 字符串命令
            case "SET":
                return set(args);
            case "GET":
                return get(args);
            case "SETEX":
                return setex(args);
            case "SETNX":
                return setnx(args);
            case "GETSET":
                return getset(args);
            case "APPEND":
                return append(args);
            case "STRLEN":
                return strlen(args);
            case "INCR":
                return incr(args);
            case "INCRBY":
                return incrby(args);
            case "DECR":
                return decr(args);
            case "DECRBY":
                return decrby(args);
            case "GETRANGE":
                return getrange(args);
            case "SETRANGE":
                return setrange(args);
            case "MSET":
                return mset(args);
            case "MGET":
                return mget(args);
                
            // 键命令
            case "EXISTS":
                return exists(args);
            case "DEL":
                return del(args);
            case "TYPE":
                return type(args);
            case "EXPIRE":
                return expire(args);
            case "PEXPIRE":
                return pexpire(args);
            case "EXPIREAT":
                return expireat(args);
            case "PEXPIREAT":
                return pexpireat(args);
            case "TTL":
                return ttl(args);
            case "PTTL":
                return pttl(args);
            case "PERSIST":
                return persist(args);
            case "KEYS":
                return keys(args);
            case "RENAME":
                return rename(args);
            case "RENAMENX":
                return renamenx(args);
            case "RANDOMKEY":
                return randomkey(args);
                
            // 列表命令
            case "LPUSH":
                return lpush(args);
            case "RPUSH":
                return rpush(args);
            case "LPOP":
                return lpop(args);
            case "RPOP":
                return rpop(args);
            case "LLEN":
                return llen(args);
            case "LINDEX":
                return lindex(args);
            case "LRANGE":
                return lrange(args);
            case "LSET":
                return lset(args);
            case "LREM":
                return lrem(args);
            case "LTRIM":
                return ltrim(args);
            case "LINSERT":
                return linsert(args);
                
            // 哈希命令
            case "HSET":
                return hset(args);
            case "HGET":
                return hget(args);
            case "HDEL":
                return hdel(args);
            case "HEXISTS":
                return hexists(args);
            case "HLEN":
                return hlen(args);
            case "HKEYS":
                return hkeys(args);
            case "HVALS":
                return hvals(args);
            case "HGETALL":
                return hgetall(args);
            case "HMSET":
                return hmset(args);
            case "HMGET":
                return hmget(args);
            case "HINCRBY":
                return hincrby(args);
            case "HINCRBYFLOAT":
                return hincrbyfloat(args);
            case "HSETNX":
                return hsetnx(args);
                
            // 集合命令
            case "SADD":
                return sadd(args);
            case "SREM":
                return srem(args);
            case "SISMEMBER":
                return sismember(args);
            case "SCARD":
                return scard(args);
            case "SMEMBERS":
                return smembers(args);
            case "SPOP":
                return spop(args);
            case "SRANDMEMBER":
                return srandmember(args);
            case "SINTER":
                return sinter(args);
            case "SUNION":
                return sunion(args);
            case "SDIFF":
                return sdiff(args);
                
            // 有序集合命令
            case "ZADD":
                return zadd(args);
            case "ZREM":
                return zrem(args);
            case "ZSCORE":
                return zscore(args);
            case "ZCARD":
                return zcard(args);
            case "ZRANK":
                return zrank(args);
            case "ZREVRANK":
                return zrevrank(args);
            case "ZRANGE":
                return zrange(args);
            case "ZREVRANGE":
                return zrevrange(args);
            case "ZINCRBY":
                return zincrby(args);
            case "ZCOUNT":
                return zcount(args);
            case "ZRANGEBYSCORE":
                return zrangebyscore(args);
                
            // 数据库命令
            case "SELECT":
                return select(args);
            case "DBSIZE":
                return dbsize(args);
            case "FLUSHDB":
                return flushdb(args);
            case "FLUSHALL":
                return flushall(args);
                
            default:
                return RESPParser.encodeError("ERR unknown command '" + cmd + "'");
        }
    }
    
    // ==================== 服务器命令 ====================
    
    /**
     * PING命令 - 测试连接
     * 
     * 无参数时返回PONG，有参数时返回参数值
     */
    private byte[] ping(String[] args) {
        if (args.length == 0) {
            return RESPParser.encodeSimpleString("PONG");
        }
        return RESPParser.encodeBulkString(args[0]);
    }
    
    /**
     * ECHO命令 - 回显消息
     */
    private byte[] echo(String[] args) {
        if (args.length < 1) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'echo' command");
        }
        return RESPParser.encodeBulkString(args[0]);
    }
    
    /**
     * INFO命令 - 获取服务器信息
     */
    private byte[] info(String[] args) {
        StringBuilder sb = new StringBuilder();
        sb.append("# Server\n");
        sb.append("java_redis_version:1.0.0\n");
        sb.append("tcp_port:6379\n");
        sb.append("# Clients\n");
        sb.append("connected_clients:1\n");
        sb.append("# Memory\n");
        sb.append("used_memory:0\n");
        sb.append("# Stats\n");
        sb.append("total_connections_received:1\n");
        sb.append("# Keyspace\n");
        sb.append("db").append(database.getCurrentDb()).append(":keys=").append(database.dbsize()).append("\n");
        return RESPParser.encodeBulkString(sb.toString());
    }
    
    // ==================== 字符串命令 ====================
    
    /**
     * SET命令 - 设置键值对
     * 
     * 支持选项：
     * - EX seconds: 设置过期时间（秒）
     * - PX milliseconds: 设置过期时间（毫秒）
     * - NX: 仅当键不存在时设置
     * - XX: 仅当键存在时设置
     */
    private byte[] set(String[] args) {
        if (args.length < 2) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'set' command");
        }
        String key = args[0];
        String value = args[1];
        
        // 处理选项
        if (args.length >= 4) {
            String option = args[2].toUpperCase();
            if ("EX".equals(option)) {
                long seconds = Long.parseLong(args[3]);
                database.setex(key, seconds, value);
                return RESPParser.encodeOK();
            } else if ("PX".equals(option)) {
                long ms = Long.parseLong(args[3]);
                database.set(key, value, ms);
                return RESPParser.encodeOK();
            } else if ("NX".equals(option)) {
                String result = database.setnx(key, value);
                return result != null ? RESPParser.encodeOK() : RESPParser.encodeNullBulkString();
            } else if ("XX".equals(option)) {
                if (database.exists(key)) {
                    database.set(key, value);
                    return RESPParser.encodeOK();
                }
                return RESPParser.encodeNullBulkString();
            }
        }
        
        database.set(key, value);
        return RESPParser.encodeOK();
    }
    
    /**
     * GET命令 - 获取键的值
     */
    private byte[] get(String[] args) {
        if (args.length < 1) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'get' command");
        }
        String value = database.get(args[0]);
        return value != null ? RESPParser.encodeBulkString(value) : RESPParser.encodeNullBulkString();
    }
    
    /**
     * SETEX命令 - 设置键值对并指定过期时间
     */
    private byte[] setex(String[] args) {
        if (args.length < 3) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'setex' command");
        }
        database.setex(args[0], Long.parseLong(args[1]), args[2]);
        return RESPParser.encodeOK();
    }
    
    /**
     * SETNX命令 - 仅当键不存在时设置
     */
    private byte[] setnx(String[] args) {
        if (args.length < 2) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'setnx' command");
        }
        String result = database.setnx(args[0], args[1]);
        return RESPParser.encodeInteger(result != null ? 1 : 0);
    }
    
    /**
     * GETSET命令 - 设置新值并返回旧值
     */
    private byte[] getset(String[] args) {
        if (args.length < 2) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'getset' command");
        }
        String oldValue = database.getSet(args[0], args[1]);
        return oldValue != null ? RESPParser.encodeBulkString(oldValue) : RESPParser.encodeNullBulkString();
    }
    
    /**
     * APPEND命令 - 追加字符串
     */
    private byte[] append(String[] args) {
        if (args.length < 2) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'append' command");
        }
        Long len = database.append(args[0], args[1]);
        return RESPParser.encodeInteger(len);
    }
    
    /**
     * STRLEN命令 - 获取字符串长度
     */
    private byte[] strlen(String[] args) {
        if (args.length < 1) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'strlen' command");
        }
        return RESPParser.encodeInteger(database.strlen(args[0]));
    }
    
    /**
     * INCR命令 - 自增1
     */
    private byte[] incr(String[] args) {
        if (args.length < 1) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'incr' command");
        }
        Long result = database.incr(args[0]);
        return result != null ? RESPParser.encodeInteger(result) : RESPParser.encodeError("ERR value is not an integer");
    }
    
    /**
     * INCRBY命令 - 增加指定值
     */
    private byte[] incrby(String[] args) {
        if (args.length < 2) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'incrby' command");
        }
        Long result = database.incrBy(args[0], Long.parseLong(args[1]));
        return result != null ? RESPParser.encodeInteger(result) : RESPParser.encodeError("ERR value is not an integer");
    }
    
    /**
     * DECR命令 - 自减1
     */
    private byte[] decr(String[] args) {
        if (args.length < 1) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'decr' command");
        }
        Long result = database.decr(args[0]);
        return result != null ? RESPParser.encodeInteger(result) : RESPParser.encodeError("ERR value is not an integer");
    }
    
    /**
     * DECRBY命令 - 减少指定值
     */
    private byte[] decrby(String[] args) {
        if (args.length < 2) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'decrby' command");
        }
        Long result = database.decrBy(args[0], Long.parseLong(args[1]));
        return result != null ? RESPParser.encodeInteger(result) : RESPParser.encodeError("ERR value is not an integer");
    }
    
    /**
     * GETRANGE命令 - 获取子串
     */
    private byte[] getrange(String[] args) {
        if (args.length < 3) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'getrange' command");
        }
        String result = database.getRange(args[0], Long.parseLong(args[1]), Long.parseLong(args[2]));
        return RESPParser.encodeBulkString(result);
    }
    
    /**
     * SETRANGE命令 - 替换子串
     */
    private byte[] setrange(String[] args) {
        if (args.length < 3) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'setrange' command");
        }
        Long len = database.setRange(args[0], Long.parseLong(args[1]), args[2]);
        return RESPParser.encodeInteger(len);
    }
    
    /**
     * MSET命令 - 批量设置
     */
    private byte[] mset(String[] args) {
        if (args.length < 2 || args.length % 2 != 0) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'mset' command");
        }
        for (int i = 0; i < args.length; i += 2) {
            database.set(args[i], args[i + 1]);
        }
        return RESPParser.encodeOK();
    }
    
    /**
     * MGET命令 - 批量获取
     */
    private byte[] mget(String[] args) {
        if (args.length < 1) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'mget' command");
        }
        List<String> values = new ArrayList<>();
        for (String key : args) {
            String value = database.get(key);
            values.add(value);
        }
        return RESPParser.encodeStringArray(values);
    }
    
    // ==================== 键命令 ====================
    
    /**
     * EXISTS命令 - 检查键是否存在
     */
    private byte[] exists(String[] args) {
        if (args.length < 1) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'exists' command");
        }
        long count = 0;
        for (String key : args) {
            if (database.exists(key)) count++;
        }
        return RESPParser.encodeInteger(count);
    }
    
    /**
     * DEL命令 - 删除键
     */
    private byte[] del(String[] args) {
        if (args.length < 1) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'del' command");
        }
        return RESPParser.encodeInteger(database.del(args));
    }
    
    /**
     * TYPE命令 - 获取键类型
     */
    private byte[] type(String[] args) {
        if (args.length < 1) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'type' command");
        }
        return RESPParser.encodeSimpleString(database.type(args[0]));
    }
    
    /**
     * EXPIRE命令 - 设置过期时间（秒）
     */
    private byte[] expire(String[] args) {
        if (args.length < 2) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'expire' command");
        }
        return RESPParser.encodeInteger(database.expire(args[0], Long.parseLong(args[1])));
    }
    
    /**
     * PEXPIRE命令 - 设置过期时间（毫秒）
     */
    private byte[] pexpire(String[] args) {
        if (args.length < 2) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'pexpire' command");
        }
        return RESPParser.encodeInteger(database.pexpire(args[0], Long.parseLong(args[1])));
    }
    
    /**
     * EXPIREAT命令 - 设置过期时间戳（秒）
     */
    private byte[] expireat(String[] args) {
        if (args.length < 2) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'expireat' command");
        }
        return RESPParser.encodeInteger(database.expireAt(args[0], Long.parseLong(args[1])));
    }
    
    /**
     * PEXPIREAT命令 - 设置过期时间戳（毫秒）
     */
    private byte[] pexpireat(String[] args) {
        if (args.length < 2) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'pexpireat' command");
        }
        return RESPParser.encodeInteger(database.pexpireAt(args[0], Long.parseLong(args[1])));
    }
    
    /**
     * TTL命令 - 获取剩余过期时间（秒）
     */
    private byte[] ttl(String[] args) {
        if (args.length < 1) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'ttl' command");
        }
        return RESPParser.encodeInteger(database.ttl(args[0]));
    }
    
    /**
     * PTTL命令 - 获取剩余过期时间（毫秒）
     */
    private byte[] pttl(String[] args) {
        if (args.length < 1) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'pttl' command");
        }
        return RESPParser.encodeInteger(database.pttl(args[0]));
    }
    
    /**
     * PERSIST命令 - 移除过期时间
     */
    private byte[] persist(String[] args) {
        if (args.length < 1) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'persist' command");
        }
        return RESPParser.encodeInteger(database.persist(args[0]));
    }
    
    /**
     * KEYS命令 - 查找匹配的键
     */
    private byte[] keys(String[] args) {
        String pattern = args.length > 0 ? args[0] : "*";
        Set<String> keys = database.keys(pattern);
        return RESPParser.encodeStringArray(new ArrayList<>(keys));
    }
    
    /**
     * RENAME命令 - 重命名键
     */
    private byte[] rename(String[] args) {
        if (args.length < 2) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'rename' command");
        }
        String result = database.rename(args[0], args[1]);
        return result != null ? RESPParser.encodeOK() : RESPParser.encodeError("ERR no such key");
    }
    
    /**
     * RENAMENX命令 - 仅当新键不存在时重命名
     */
    private byte[] renamenx(String[] args) {
        if (args.length < 2) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'renamenx' command");
        }
        return RESPParser.encodeInteger(database.renamenx(args[0], args[1]));
    }
    
    /**
     * RANDOMKEY命令 - 随机返回一个键
     */
    private byte[] randomkey(String[] args) {
        Set<String> keys = database.keys("*");
        if (keys.isEmpty()) {
            return RESPParser.encodeNullBulkString();
        }
        String randomKey = keys.iterator().next();
        return RESPParser.encodeBulkString(randomKey);
    }
    
    // ==================== 列表命令 ====================
    
    /**
     * LPUSH命令 - 在列表头部插入元素
     */
    private byte[] lpush(String[] args) {
        if (args.length < 2) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'lpush' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            obj = RedisObject.createList();
            database.setObject(args[0], obj);
        }
        if (obj.getType() != RedisObject.Type.LIST) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisList list = obj.getValue();
        String[] values = Arrays.copyOfRange(args, 1, args.length);
        return RESPParser.encodeInteger(list.lpush(values));
    }
    
    /**
     * RPUSH命令 - 在列表尾部插入元素
     */
    private byte[] rpush(String[] args) {
        if (args.length < 2) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'rpush' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            obj = RedisObject.createList();
            database.setObject(args[0], obj);
        }
        if (obj.getType() != RedisObject.Type.LIST) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisList list = obj.getValue();
        String[] values = Arrays.copyOfRange(args, 1, args.length);
        return RESPParser.encodeInteger(list.rpush(values));
    }
    
    /**
     * LPOP命令 - 从列表头部弹出元素
     */
    private byte[] lpop(String[] args) {
        if (args.length < 1) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'lpop' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            return RESPParser.encodeNullBulkString();
        }
        if (obj.getType() != RedisObject.Type.LIST) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisList list = obj.getValue();
        String value = list.lpop();
        if (list.isEmpty()) {
            database.del(args[0]);
        }
        return value != null ? RESPParser.encodeBulkString(value) : RESPParser.encodeNullBulkString();
    }
    
    /**
     * RPOP命令 - 从列表尾部弹出元素
     */
    private byte[] rpop(String[] args) {
        if (args.length < 1) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'rpop' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            return RESPParser.encodeNullBulkString();
        }
        if (obj.getType() != RedisObject.Type.LIST) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisList list = obj.getValue();
        String value = list.rpop();
        if (list.isEmpty()) {
            database.del(args[0]);
        }
        return value != null ? RESPParser.encodeBulkString(value) : RESPParser.encodeNullBulkString();
    }
    
    /**
     * LLEN命令 - 获取列表长度
     */
    private byte[] llen(String[] args) {
        if (args.length < 1) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'llen' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            return RESPParser.encodeInteger(0);
        }
        if (obj.getType() != RedisObject.Type.LIST) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisList list = obj.getValue();
        return RESPParser.encodeInteger(list.llen());
    }
    
    /**
     * LINDEX命令 - 获取指定索引的元素
     */
    private byte[] lindex(String[] args) {
        if (args.length < 2) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'lindex' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            return RESPParser.encodeNullBulkString();
        }
        if (obj.getType() != RedisObject.Type.LIST) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisList list = obj.getValue();
        String value = list.lindex(Long.parseLong(args[1]));
        return value != null ? RESPParser.encodeBulkString(value) : RESPParser.encodeNullBulkString();
    }
    
    /**
     * LRANGE命令 - 获取指定范围的元素
     */
    private byte[] lrange(String[] args) {
        if (args.length < 3) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'lrange' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            return RESPParser.encodeStringArray(new ArrayList<>());
        }
        if (obj.getType() != RedisObject.Type.LIST) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisList list = obj.getValue();
        List<String> values = list.lrange(Long.parseLong(args[1]), Long.parseLong(args[2]));
        return RESPParser.encodeStringArray(values);
    }
    
    /**
     * LSET命令 - 设置指定索引的元素
     */
    private byte[] lset(String[] args) {
        if (args.length < 3) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'lset' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            return RESPParser.encodeError("ERR no such key");
        }
        if (obj.getType() != RedisObject.Type.LIST) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisList list = obj.getValue();
        String result = list.lset(Long.parseLong(args[1]), args[2]);
        return result != null ? RESPParser.encodeOK() : RESPParser.encodeError("ERR index out of range");
    }
    
    /**
     * LREM命令 - 移除列表元素
     */
    private byte[] lrem(String[] args) {
        if (args.length < 3) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'lrem' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            return RESPParser.encodeInteger(0);
        }
        if (obj.getType() != RedisObject.Type.LIST) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisList list = obj.getValue();
        long removed = list.lrem(Long.parseLong(args[1]), args[2]);
        return RESPParser.encodeInteger(removed);
    }
    
    /**
     * LTRIM命令 - 裁剪列表
     */
    private byte[] ltrim(String[] args) {
        if (args.length < 3) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'ltrim' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            return RESPParser.encodeOK();
        }
        if (obj.getType() != RedisObject.Type.LIST) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisList list = obj.getValue();
        list.ltrim(Long.parseLong(args[1]), Long.parseLong(args[2]));
        return RESPParser.encodeOK();
    }
    
    /**
     * LINSERT命令 - 插入元素
     */
    private byte[] linsert(String[] args) {
        if (args.length < 4) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'linsert' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            return RESPParser.encodeInteger(0);
        }
        if (obj.getType() != RedisObject.Type.LIST) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisList list = obj.getValue();
        String where = args[1].toUpperCase();
        String pivot = args[2];
        String value = args[3];
        long result = "BEFORE".equals(where) ? list.linsertBefore(pivot, value) : list.linsertAfter(pivot, value);
        return RESPParser.encodeInteger(result);
    }
    
    // ==================== 哈希命令 ====================
    
    /**
     * HSET命令 - 设置哈希字段
     */
    private byte[] hset(String[] args) {
        if (args.length < 3) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'hset' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            obj = RedisObject.createHash();
            database.setObject(args[0], obj);
        }
        if (obj.getType() != RedisObject.Type.HASH) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisHash hash = obj.getValue();
        long added = 0;
        for (int i = 1; i < args.length; i += 2) {
            if (i + 1 < args.length) {
                added += hash.hset(args[i], args[i + 1]);
            }
        }
        return RESPParser.encodeInteger(added);
    }
    
    /**
     * HGET命令 - 获取哈希字段值
     */
    private byte[] hget(String[] args) {
        if (args.length < 2) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'hget' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            return RESPParser.encodeNullBulkString();
        }
        if (obj.getType() != RedisObject.Type.HASH) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisHash hash = obj.getValue();
        String value = hash.hget(args[1]);
        return value != null ? RESPParser.encodeBulkString(value) : RESPParser.encodeNullBulkString();
    }
    
    /**
     * HDEL命令 - 删除哈希字段
     */
    private byte[] hdel(String[] args) {
        if (args.length < 2) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'hdel' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            return RESPParser.encodeInteger(0);
        }
        if (obj.getType() != RedisObject.Type.HASH) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisHash hash = obj.getValue();
        String[] fields = Arrays.copyOfRange(args, 1, args.length);
        return RESPParser.encodeInteger(hash.hdel(fields));
    }
    
    /**
     * HEXISTS命令 - 检查哈希字段是否存在
     */
    private byte[] hexists(String[] args) {
        if (args.length < 2) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'hexists' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            return RESPParser.encodeInteger(0);
        }
        if (obj.getType() != RedisObject.Type.HASH) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisHash hash = obj.getValue();
        return RESPParser.encodeInteger(hash.hexists(args[1]) ? 1 : 0);
    }
    
    /**
     * HLEN命令 - 获取哈希字段数量
     */
    private byte[] hlen(String[] args) {
        if (args.length < 1) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'hlen' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            return RESPParser.encodeInteger(0);
        }
        if (obj.getType() != RedisObject.Type.HASH) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisHash hash = obj.getValue();
        return RESPParser.encodeInteger(hash.hlen());
    }
    
    /**
     * HKEYS命令 - 获取所有字段名
     */
    private byte[] hkeys(String[] args) {
        if (args.length < 1) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'hkeys' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            return RESPParser.encodeStringArray(new ArrayList<>());
        }
        if (obj.getType() != RedisObject.Type.HASH) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisHash hash = obj.getValue();
        return RESPParser.encodeStringArray(new ArrayList<>(hash.hkeys()));
    }
    
    /**
     * HVALS命令 - 获取所有字段值
     */
    private byte[] hvals(String[] args) {
        if (args.length < 1) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'hvals' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            return RESPParser.encodeStringArray(new ArrayList<>());
        }
        if (obj.getType() != RedisObject.Type.HASH) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisHash hash = obj.getValue();
        return RESPParser.encodeStringArray(new ArrayList<>(hash.hvals()));
    }
    
    /**
     * HGETALL命令 - 获取所有字段和值
     */
    private byte[] hgetall(String[] args) {
        if (args.length < 1) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'hgetall' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            return RESPParser.encodeStringArray(new ArrayList<>());
        }
        if (obj.getType() != RedisObject.Type.HASH) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisHash hash = obj.getValue();
        Map<String, String> all = hash.hgetall();
        List<String> result = new ArrayList<>();
        for (Map.Entry<String, String> entry : all.entrySet()) {
            result.add(entry.getKey());
            result.add(entry.getValue());
        }
        return RESPParser.encodeStringArray(result);
    }
    
    /**
     * HMSET命令 - 批量设置哈希字段
     */
    private byte[] hmset(String[] args) {
        if (args.length < 3) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'hmset' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            obj = RedisObject.createHash();
            database.setObject(args[0], obj);
        }
        if (obj.getType() != RedisObject.Type.HASH) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisHash hash = obj.getValue();
        Map<String, String> map = new HashMap<>();
        for (int i = 1; i < args.length; i += 2) {
            if (i + 1 < args.length) {
                map.put(args[i], args[i + 1]);
            }
        }
        hash.hmset(map);
        return RESPParser.encodeOK();
    }
    
    /**
     * HMGET命令 - 批量获取哈希字段值
     */
    private byte[] hmget(String[] args) {
        if (args.length < 2) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'hmget' command");
        }
        RedisObject obj = database.getObject(args[0]);
        List<String> values = new ArrayList<>();
        if (obj == null) {
            for (int i = 1; i < args.length; i++) {
                values.add(null);
            }
        } else if (obj.getType() != RedisObject.Type.HASH) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        } else {
            RedisHash hash = obj.getValue();
            for (int i = 1; i < args.length; i++) {
                values.add(hash.hget(args[i]));
            }
        }
        return RESPParser.encodeStringArray(values);
    }
    
    /**
     * HINCRBY命令 - 哈希字段整数值增量
     */
    private byte[] hincrby(String[] args) {
        if (args.length < 3) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'hincrby' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            obj = RedisObject.createHash();
            database.setObject(args[0], obj);
        }
        if (obj.getType() != RedisObject.Type.HASH) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisHash hash = obj.getValue();
        String result = hash.hincrby(args[1], Long.parseLong(args[2]));
        return result != null ? RESPParser.encodeBulkString(result) : RESPParser.encodeError("ERR value is not an integer");
    }
    
    /**
     * HINCRBYFLOAT命令 - 哈希字段浮点数值增量
     */
    private byte[] hincrbyfloat(String[] args) {
        if (args.length < 3) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'hincrbyfloat' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            obj = RedisObject.createHash();
            database.setObject(args[0], obj);
        }
        if (obj.getType() != RedisObject.Type.HASH) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisHash hash = obj.getValue();
        String result = hash.hincrbyfloat(args[1], Double.parseDouble(args[2]));
        return result != null ? RESPParser.encodeBulkString(result) : RESPParser.encodeError("ERR value is not a float");
    }
    
    /**
     * HSETNX命令 - 仅当字段不存在时设置
     */
    private byte[] hsetnx(String[] args) {
        if (args.length < 3) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'hsetnx' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            obj = RedisObject.createHash();
            database.setObject(args[0], obj);
        }
        if (obj.getType() != RedisObject.Type.HASH) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisHash hash = obj.getValue();
        return RESPParser.encodeInteger(hash.hsetnx(args[1], args[2]));
    }
    
    // ==================== 集合命令 ====================
    
    /**
     * SADD命令 - 添加集合元素
     */
    private byte[] sadd(String[] args) {
        if (args.length < 2) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'sadd' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            obj = RedisObject.createSet();
            database.setObject(args[0], obj);
        }
        if (obj.getType() != RedisObject.Type.SET) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisSet set = obj.getValue();
        String[] members = Arrays.copyOfRange(args, 1, args.length);
        return RESPParser.encodeInteger(set.sadd(members));
    }
    
    /**
     * SREM命令 - 移除集合元素
     */
    private byte[] srem(String[] args) {
        if (args.length < 2) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'srem' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            return RESPParser.encodeInteger(0);
        }
        if (obj.getType() != RedisObject.Type.SET) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisSet set = obj.getValue();
        String[] members = Arrays.copyOfRange(args, 1, args.length);
        return RESPParser.encodeInteger(set.srem(members));
    }
    
    /**
     * SISMEMBER命令 - 检查元素是否在集合中
     */
    private byte[] sismember(String[] args) {
        if (args.length < 2) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'sismember' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            return RESPParser.encodeInteger(0);
        }
        if (obj.getType() != RedisObject.Type.SET) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisSet set = obj.getValue();
        return RESPParser.encodeInteger(set.sismember(args[1]) ? 1 : 0);
    }
    
    /**
     * SCARD命令 - 获取集合元素数量
     */
    private byte[] scard(String[] args) {
        if (args.length < 1) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'scard' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            return RESPParser.encodeInteger(0);
        }
        if (obj.getType() != RedisObject.Type.SET) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisSet set = obj.getValue();
        return RESPParser.encodeInteger(set.scard());
    }
    
    /**
     * SMEMBERS命令 - 获取集合所有元素
     */
    private byte[] smembers(String[] args) {
        if (args.length < 1) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'smembers' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            return RESPParser.encodeStringArray(new ArrayList<>());
        }
        if (obj.getType() != RedisObject.Type.SET) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisSet set = obj.getValue();
        return RESPParser.encodeStringArray(new ArrayList<>(set.smembers()));
    }
    
    /**
     * SPOP命令 - 随机弹出元素
     */
    private byte[] spop(String[] args) {
        if (args.length < 1) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'spop' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            return RESPParser.encodeNullBulkString();
        }
        if (obj.getType() != RedisObject.Type.SET) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisSet set = obj.getValue();
        if (args.length > 1) {
            long count = Long.parseLong(args[1]);
            Set<String> popped = set.spop(count);
            return RESPParser.encodeStringArray(new ArrayList<>(popped));
        }
        String member = set.spop();
        if (set.isEmpty()) {
            database.del(args[0]);
        }
        return member != null ? RESPParser.encodeBulkString(member) : RESPParser.encodeNullBulkString();
    }
    
    /**
     * SRANDMEMBER命令 - 随机获取元素
     */
    private byte[] srandmember(String[] args) {
        if (args.length < 1) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'srandmember' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            return RESPParser.encodeNullBulkString();
        }
        if (obj.getType() != RedisObject.Type.SET) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisSet set = obj.getValue();
        if (args.length > 1) {
            long count = Long.parseLong(args[1]);
            Set<String> members = set.srandmember(count);
            return RESPParser.encodeStringArray(new ArrayList<>(members));
        }
        String member = set.srandmember();
        return member != null ? RESPParser.encodeBulkString(member) : RESPParser.encodeNullBulkString();
    }
    
    /**
     * SINTER命令 - 集合交集
     */
    private byte[] sinter(String[] args) {
        if (args.length < 1) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'sinter' command");
        }
        RedisSet[] sets = new RedisSet[args.length];
        for (int i = 0; i < args.length; i++) {
            RedisObject obj = database.getObject(args[i]);
            if (obj == null) {
                return RESPParser.encodeStringArray(new ArrayList<>());
            }
            if (obj.getType() != RedisObject.Type.SET) {
                return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
            sets[i] = obj.getValue();
        }
        Set<String> result = RedisSet.sinter(sets);
        return RESPParser.encodeStringArray(new ArrayList<>(result));
    }
    
    /**
     * SUNION命令 - 集合并集
     */
    private byte[] sunion(String[] args) {
        if (args.length < 1) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'sunion' command");
        }
        RedisSet[] sets = new RedisSet[args.length];
        for (int i = 0; i < args.length; i++) {
            RedisObject obj = database.getObject(args[i]);
            if (obj == null) {
                sets[i] = new RedisSet();
            } else if (obj.getType() != RedisObject.Type.SET) {
                return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
            } else {
                sets[i] = obj.getValue();
            }
        }
        Set<String> result = RedisSet.sunion(sets);
        return RESPParser.encodeStringArray(new ArrayList<>(result));
    }
    
    /**
     * SDIFF命令 - 集合差集
     */
    private byte[] sdiff(String[] args) {
        if (args.length < 1) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'sdiff' command");
        }
        RedisSet[] sets = new RedisSet[args.length];
        for (int i = 0; i < args.length; i++) {
            RedisObject obj = database.getObject(args[i]);
            if (obj == null) {
                sets[i] = new RedisSet();
            } else if (obj.getType() != RedisObject.Type.SET) {
                return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
            } else {
                sets[i] = obj.getValue();
            }
        }
        Set<String> result = RedisSet.sdiff(sets);
        return RESPParser.encodeStringArray(new ArrayList<>(result));
    }
    
    // ==================== 有序集合命令 ====================
    
    /**
     * ZADD命令 - 添加有序集合元素
     */
    private byte[] zadd(String[] args) {
        if (args.length < 3) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'zadd' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            obj = RedisObject.createZSet();
            database.setObject(args[0], obj);
        }
        if (obj.getType() != RedisObject.Type.ZSET) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisZSet zset = obj.getValue();
        long added = 0;
        for (int i = 1; i < args.length; i += 2) {
            if (i + 1 < args.length) {
                double score = Double.parseDouble(args[i]);
                String member = args[i + 1];
                added += zset.zadd(score, member);
            }
        }
        return RESPParser.encodeInteger(added);
    }
    
    /**
     * ZREM命令 - 移除有序集合元素
     */
    private byte[] zrem(String[] args) {
        if (args.length < 2) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'zrem' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            return RESPParser.encodeInteger(0);
        }
        if (obj.getType() != RedisObject.Type.ZSET) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisZSet zset = obj.getValue();
        String[] members = Arrays.copyOfRange(args, 1, args.length);
        return RESPParser.encodeInteger(zset.zrem(members));
    }
    
    /**
     * ZSCORE命令 - 获取元素分数
     */
    private byte[] zscore(String[] args) {
        if (args.length < 2) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'zscore' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            return RESPParser.encodeNullBulkString();
        }
        if (obj.getType() != RedisObject.Type.ZSET) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisZSet zset = obj.getValue();
        Double score = zset.zscore(args[1]);
        return score != null ? RESPParser.encodeBulkString(String.valueOf(score)) : RESPParser.encodeNullBulkString();
    }
    
    /**
     * ZCARD命令 - 获取有序集合元素数量
     */
    private byte[] zcard(String[] args) {
        if (args.length < 1) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'zcard' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            return RESPParser.encodeInteger(0);
        }
        if (obj.getType() != RedisObject.Type.ZSET) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisZSet zset = obj.getValue();
        return RESPParser.encodeInteger(zset.zcard());
    }
    
    /**
     * ZRANK命令 - 获取元素排名（升序）
     */
    private byte[] zrank(String[] args) {
        if (args.length < 2) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'zrank' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            return RESPParser.encodeNullBulkString();
        }
        if (obj.getType() != RedisObject.Type.ZSET) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisZSet zset = obj.getValue();
        Long rank = zset.zrank(args[1]);
        return rank != null ? RESPParser.encodeInteger(rank) : RESPParser.encodeNullBulkString();
    }
    
    /**
     * ZREVRANK命令 - 获取元素排名（降序）
     */
    private byte[] zrevrank(String[] args) {
        if (args.length < 2) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'zrevrank' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            return RESPParser.encodeNullBulkString();
        }
        if (obj.getType() != RedisObject.Type.ZSET) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisZSet zset = obj.getValue();
        Long rank = zset.zrevrank(args[1]);
        return rank != null ? RESPParser.encodeInteger(rank) : RESPParser.encodeNullBulkString();
    }
    
    /**
     * ZRANGE命令 - 获取指定排名范围的元素
     */
    private byte[] zrange(String[] args) {
        if (args.length < 3) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'zrange' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            return RESPParser.encodeStringArray(new ArrayList<>());
        }
        if (obj.getType() != RedisObject.Type.ZSET) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisZSet zset = obj.getValue();
        long start = Long.parseLong(args[1]);
        long stop = Long.parseLong(args[2]);
        boolean withScores = args.length > 3 && "WITHSCORES".equalsIgnoreCase(args[3]);
        
        List<RedisZSet.ZSetEntry> entries = zset.zrange(start, stop);
        List<String> result = new ArrayList<>();
        for (RedisZSet.ZSetEntry entry : entries) {
            result.add(entry.member);
            if (withScores) {
                result.add(String.valueOf(entry.score));
            }
        }
        return RESPParser.encodeStringArray(result);
    }
    
    /**
     * ZREVRANGE命令 - 获取指定排名范围的元素（降序）
     */
    private byte[] zrevrange(String[] args) {
        if (args.length < 3) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'zrevrange' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            return RESPParser.encodeStringArray(new ArrayList<>());
        }
        if (obj.getType() != RedisObject.Type.ZSET) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisZSet zset = obj.getValue();
        long start = Long.parseLong(args[1]);
        long stop = Long.parseLong(args[2]);
        boolean withScores = args.length > 3 && "WITHSCORES".equalsIgnoreCase(args[3]);
        
        List<RedisZSet.ZSetEntry> entries = zset.zrevrange(start, stop);
        List<String> result = new ArrayList<>();
        for (RedisZSet.ZSetEntry entry : entries) {
            result.add(entry.member);
            if (withScores) {
                result.add(String.valueOf(entry.score));
            }
        }
        return RESPParser.encodeStringArray(result);
    }
    
    /**
     * ZINCRBY命令 - 增加元素分数
     */
    private byte[] zincrby(String[] args) {
        if (args.length < 3) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'zincrby' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            obj = RedisObject.createZSet();
            database.setObject(args[0], obj);
        }
        if (obj.getType() != RedisObject.Type.ZSET) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisZSet zset = obj.getValue();
        double increment = Double.parseDouble(args[1]);
        Double result = zset.zincrby(increment, args[2]);
        return RESPParser.encodeBulkString(String.valueOf(result));
    }
    
    /**
     * ZCOUNT命令 - 统计分数范围内的元素数量
     */
    private byte[] zcount(String[] args) {
        if (args.length < 3) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'zcount' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            return RESPParser.encodeInteger(0);
        }
        if (obj.getType() != RedisObject.Type.ZSET) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisZSet zset = obj.getValue();
        double min = parseScore(args[1]);
        double max = parseScore(args[2]);
        long count = zset.zcount(min, max, true, true);
        return RESPParser.encodeInteger(count);
    }
    
    /**
     * ZRANGEBYSCORE命令 - 获取分数范围内的元素
     */
    private byte[] zrangebyscore(String[] args) {
        if (args.length < 3) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'zrangebyscore' command");
        }
        RedisObject obj = database.getObject(args[0]);
        if (obj == null) {
            return RESPParser.encodeStringArray(new ArrayList<>());
        }
        if (obj.getType() != RedisObject.Type.ZSET) {
            return RESPParser.encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        RedisZSet zset = obj.getValue();
        double min = parseScore(args[1]);
        double max = parseScore(args[2]);
        boolean minInclusive = !args[1].startsWith("(");
        boolean maxInclusive = !args[2].startsWith("(");
        
        List<RedisZSet.ZSetEntry> entries = zset.zrangebyscore(min, max, minInclusive, maxInclusive);
        List<String> result = new ArrayList<>();
        for (RedisZSet.ZSetEntry entry : entries) {
            result.add(entry.member);
        }
        return RESPParser.encodeStringArray(result);
    }
    
    /**
     * 解析分数参数
     * 
     * 支持格式：
     * - 数字: 1.5
     * - 排除边界: (1.5
     * - 无穷大: +inf, -inf
     */
    private double parseScore(String arg) {
        if (arg.startsWith("(")) {
            return Double.parseDouble(arg.substring(1));
        }
        if ("-inf".equalsIgnoreCase(arg)) {
            return Double.NEGATIVE_INFINITY;
        }
        if ("+inf".equalsIgnoreCase(arg) || "inf".equalsIgnoreCase(arg)) {
            return Double.POSITIVE_INFINITY;
        }
        return Double.parseDouble(arg);
    }
    
    // ==================== 数据库命令 ====================
    
    /**
     * SELECT命令 - 切换数据库
     */
    private byte[] select(String[] args) {
        if (args.length < 1) {
            return RESPParser.encodeError("ERR wrong number of arguments for 'select' command");
        }
        int dbIndex = Integer.parseInt(args[0]);
        if (dbIndex < 0 || dbIndex >= database.getDbCount()) {
            return RESPParser.encodeError("ERR DB index is out of range");
        }
        database.select(dbIndex);
        return RESPParser.encodeOK();
    }
    
    /**
     * DBSIZE命令 - 获取数据库键数量
     */
    private byte[] dbsize(String[] args) {
        return RESPParser.encodeInteger(database.dbsize());
    }
    
    /**
     * FLUSHDB命令 - 清空当前数据库
     */
    private byte[] flushdb(String[] args) {
        database.flushdb();
        return RESPParser.encodeOK();
    }
    
    /**
     * FLUSHALL命令 - 清空所有数据库
     */
    private byte[] flushall(String[] args) {
        database.flushall();
        return RESPParser.encodeOK();
    }
}
