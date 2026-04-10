package com.learn.redis.persistence;

import com.learn.redis.config.RedisConfig;
import com.learn.redis.data.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * PersistenceManager - 持久化管理器
 * 
 * 【Redis持久化简介】
 * Redis是一个内存数据库，数据存储在内存中。
 * 为了防止服务器重启后数据丢失，Redis提供了两种持久化机制：
 * 
 * 1. RDB (Redis Database)
 *    - 在指定时间间隔内将内存中的数据集快照写入磁盘
 *    - 生成一个压缩的二进制文件
 *    - 适合备份和灾难恢复
 *    - 文件小，恢复速度快
 *    - 可能丢失最后一次快照后的数据
 * 
 * 2. AOF (Append Only File)
 *    - 记录所有写操作命令
 *    - 以追加方式写入文件
 *    - 数据更安全，最多丢失1秒数据
 *    - 文件可能很大，需要定期重写
 * 
 * 【持久化策略选择】
 * - 只需备份，可接受少量数据丢失：使用RDB
 * - 数据安全要求高：使用AOF
 * - 综合考虑：同时使用RDB和AOF
 */
public class PersistenceManager {
    private static final Logger logger = LoggerFactory.getLogger(PersistenceManager.class);
    
    /** RDB文件魔数，用于识别文件格式 */
    private static final String RDB_MAGIC = "REDIS";
    
    /** RDB文件版本号 */
    private static final int RDB_VERSION = 6;
    
    /** 数据库实例 */
    private final RedisDatabase database;
    
    /** 配置信息 */
    private final RedisConfig config;
    
    /** 定时任务调度器 */
    private final ScheduledExecutorService scheduler;
    
    /** 自上次保存以来的修改次数 */
    private final AtomicLong changesSinceLastSave;
    
    /** AOF管理器（如果启用） */
    private final AOFManager aofManager;
    
    /**
     * 构造函数
     * 
     * @param database 数据库实例
     * @param config   配置信息
     */
    public PersistenceManager(RedisDatabase database, RedisConfig config) {
        this.database = database;
        this.config = config;
        this.scheduler = Executors.newScheduledThreadPool(2);
        this.changesSinceLastSave = new AtomicLong(0);
        this.aofManager = config.isAofEnabled() ? new AOFManager(config.getAofFilename()) : null;
    }
    
    /**
     * 启动持久化服务
     * 
     * 【启动流程】
     * 1. 如果启用RDB，启动定时保存任务
     * 2. 如果启用AOF，启动AOF写入线程
     */
    public void start() {
        // 启动RDB定时保存
        if (config.isRdbEnabled()) {
            scheduler.scheduleAtFixedRate(() -> {
                try {
                    // 检查是否满足保存条件
                    // save <seconds> <changes>：在seconds秒内有changes次修改就保存
                    if (changesSinceLastSave.get() >= config.getRdbSaveChanges()) {
                        saveRDB();
                    }
                } catch (Exception e) {
                    logger.error("RDB save failed", e);
                }
            }, config.getRdbSaveInterval(), config.getRdbSaveInterval(), TimeUnit.SECONDS);
        }
        
        // 启动AOF
        if (aofManager != null) {
            aofManager.start();
        }
    }
    
    /**
     * 加载持久化数据
     * 
     * 【加载策略】
     * 1. 如果同时启用AOF和RDB，优先加载AOF（数据更完整）
     * 2. 否则加载RDB
     */
    public void load() {
        if (config.isAofEnabled() && new File(config.getAofFilename()).exists()) {
            loadAOF();
        } else if (config.isRdbEnabled() && new File(config.getRdbFilename()).exists()) {
            loadRDB();
        }
    }
    
    /**
     * 关闭持久化服务
     * 
     * 【关闭流程】
     * 1. 停止定时任务
     * 2. 关闭AOF
     * 3. 执行最后一次RDB保存
     */
    public void shutdown() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
        }
        
        if (aofManager != null) {
            aofManager.shutdown();
        }
        
        // 关闭前保存一次
        try {
            saveRDB();
        } catch (Exception e) {
            logger.error("Final RDB save failed", e);
        }
    }
    
    /**
     * 记录数据变更
     * 
     * 用于：
     * 1. 统计修改次数，判断是否需要RDB保存
     * 2. 写入AOF日志
     * 
     * @param command 执行的命令
     */
    public void recordChange(String command) {
        changesSinceLastSave.incrementAndGet();
        if (aofManager != null) {
            aofManager.appendCommand(command);
        }
    }
    
    // ==================== RDB持久化 ====================
    
    /**
     * 保存RDB文件
     * 
     * 【RDB文件结构】
     * +-------------+-------------+-----------+-----+-----------+
     * | REDIS       | RDB_VERSION | SELECTDB  | ... | EOF       |
     * | (magic)     | (version)   | (db index)|     | (0xFF)    |
     * +-------------+-------------+-----------+-----+-----------+
     * 
     * 每个数据库的数据：
     * +-------------+-------------+------------------+
     * | SELECTDB    | db_number   | key_value_pairs  |
     * | (0xFE)      |             |                  |
     * +-------------+-------------+------------------+
     * 
     * 每个键值对：
     * +-------------+-------------+-------------+-------------+
     * | EXPIRETIME  | timestamp   | type        | key         |
     * | (optional)  |             |             |             |
     * +-------------+-------------+-------------+-------------+
     * | value       |             |             |             |
     * +-------------+-------------+-------------+-------------+
     * 
     * @throws IOException 如果写入失败
     */
    public void saveRDB() throws IOException {
        String filename = config.getRdbFilename();
        String tempFile = filename + ".tmp";
        
        // 使用临时文件写入，完成后原子性重命名
        try (FileOutputStream fos = new FileOutputStream(tempFile);
             FileChannel channel = fos.getChannel()) {
            
            ByteBuffer buffer = ByteBuffer.allocate(8192);
            
            // 写入魔数和版本号
            writeString(buffer, channel, RDB_MAGIC);
            writeInt(buffer, channel, RDB_VERSION);
            
            // 遍历所有数据库
            for (int dbIndex = 0; dbIndex < database.getDbCount(); dbIndex++) {
                Map<String, RedisObject> dbData = database.getDb(dbIndex);
                if (dbData == null || dbData.isEmpty()) continue;
                
                // 写入数据库选择标记和索引
                writeByte(buffer, channel, (byte) 0xFE);
                writeInt(buffer, channel, dbIndex);
                
                // 写入所有键值对
                for (Map.Entry<String, RedisObject> entry : dbData.entrySet()) {
                    String key = entry.getKey();
                    RedisObject obj = entry.getValue();
                    
                    // 跳过已过期的键
                    if (obj.isExpired()) continue;
                    
                    // 写入过期时间（如果有）
                    long expireTime = obj.getExpireTime();
                    if (expireTime != -1) {
                        writeByte(buffer, channel, (byte) 0xFC);
                        writeLong(buffer, channel, expireTime);
                    }
                    
                    // 写入类型、键和值
                    writeByte(buffer, channel, getTypeByte(obj.getType()));
                    writeString(buffer, channel, key);
                    writeObject(buffer, channel, obj);
                }
            }
            
            // 写入结束标记
            writeByte(buffer, channel, (byte) 0xFF);
            
            // 刷新缓冲区
            buffer.flip();
            if (buffer.hasRemaining()) {
                channel.write(buffer);
            }
        }
        
        // 原子性重命名
        File oldFile = new File(filename);
        File newFile = new File(tempFile);
        if (oldFile.exists()) {
            oldFile.delete();
        }
        newFile.renameTo(oldFile);
        
        changesSinceLastSave.set(0);
        logger.info("RDB saved to {}", filename);
    }
    
    /**
     * 获取类型对应的字节码
     */
    private byte getTypeByte(RedisObject.Type type) {
        switch (type) {
            case STRING: return 0;
            case LIST: return 1;
            case HASH: return 2;
            case SET: return 3;
            case ZSET: return 4;
            default: return 0;
        }
    }
    
    /**
     * 写入单字节
     */
    private void writeByte(ByteBuffer buffer, FileChannel channel, byte value) throws IOException {
        if (!buffer.hasRemaining()) {
            buffer.flip();
            channel.write(buffer);
            buffer.clear();
        }
        buffer.put(value);
    }
    
    /**
     * 写入32位整数（大端序）
     */
    private void writeInt(ByteBuffer buffer, FileChannel channel, int value) throws IOException {
        byte[] bytes = new byte[4];
        bytes[0] = (byte) ((value >> 24) & 0xFF);
        bytes[1] = (byte) ((value >> 16) & 0xFF);
        bytes[2] = (byte) ((value >> 8) & 0xFF);
        bytes[3] = (byte) (value & 0xFF);
        for (byte b : bytes) {
            writeByte(buffer, channel, b);
        }
    }
    
    /**
     * 写入64位长整数（大端序）
     */
    private void writeLong(ByteBuffer buffer, FileChannel channel, long value) throws IOException {
        byte[] bytes = new byte[8];
        for (int i = 0; i < 8; i++) {
            bytes[i] = (byte) ((value >> (56 - i * 8)) & 0xFF);
        }
        for (byte b : bytes) {
            writeByte(buffer, channel, b);
        }
    }
    
    /**
     * 写入字符串（长度前缀格式）
     */
    private void writeString(ByteBuffer buffer, FileChannel channel, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        writeInt(buffer, channel, bytes.length);
        for (byte b : bytes) {
            writeByte(buffer, channel, b);
        }
    }
    
    /**
     * 写入Redis对象
     * 
     * 根据类型写入不同格式的数据
     */
    private void writeObject(ByteBuffer buffer, FileChannel channel, RedisObject obj) throws IOException {
        switch (obj.getType()) {
            case STRING:
                writeString(buffer, channel, obj.getStringValue());
                break;
            case LIST:
                RedisList list = obj.getValue();
                writeInt(buffer, channel, (int) list.llen());
                for (String item : list.getAll()) {
                    writeString(buffer, channel, item);
                }
                break;
            case HASH:
                RedisHash hash = obj.getValue();
                Map<String, String> hashData = hash.hgetall();
                writeInt(buffer, channel, hashData.size());
                for (Map.Entry<String, String> entry : hashData.entrySet()) {
                    writeString(buffer, channel, entry.getKey());
                    writeString(buffer, channel, entry.getValue());
                }
                break;
            case SET:
                RedisSet set = obj.getValue();
                Set<String> setData = set.smembers();
                writeInt(buffer, channel, setData.size());
                for (String member : setData) {
                    writeString(buffer, channel, member);
                }
                break;
            case ZSET:
                RedisZSet zset = obj.getValue();
                List<RedisZSet.ZSetEntry> entries = zset.zrange(0, -1);
                writeInt(buffer, channel, entries.size());
                for (RedisZSet.ZSetEntry entry : entries) {
                    writeString(buffer, channel, entry.member);
                    buffer.putDouble(entry.score);
                }
                break;
        }
    }
    
    /**
     * 加载RDB文件
     */
    public void loadRDB() {
        String filename = config.getRdbFilename();
        File file = new File(filename);
        if (!file.exists()) {
            logger.info("RDB file not found: {}", filename);
            return;
        }
        
        try (FileInputStream fis = new FileInputStream(file);
             FileChannel channel = fis.getChannel()) {
            
            ByteBuffer buffer = ByteBuffer.allocate(8192);
            buffer.order(java.nio.ByteOrder.BIG_ENDIAN);
            
            // 验证魔数
            String magic = readString(buffer, channel);
            if (!RDB_MAGIC.equals(magic)) {
                throw new IOException("Invalid RDB file magic: " + magic);
            }
            
            // 读取版本号
            int version = readInt(buffer, channel);
            logger.info("Loading RDB file version {}", version);
            
            int currentDb = 0;
            long expireTime = -1;
            
            // 读取数据
            while (true) {
                byte type = readByte(buffer, channel);
                
                // 结束标记
                if (type == (byte) 0xFF) {
                    break;
                }
                
                // 数据库选择标记
                if (type == (byte) 0xFE) {
                    currentDb = readInt(buffer, channel);
                    database.select(currentDb);
                    continue;
                }
                
                // 过期时间标记
                if (type == (byte) 0xFC) {
                    expireTime = readLong(buffer, channel);
                    type = readByte(buffer, channel);
                }
                
                // 读取键值对
                String key = readString(buffer, channel);
                RedisObject obj = readObject(buffer, channel, type);
                
                if (expireTime != -1) {
                    obj.setExpireTime(expireTime);
                    expireTime = -1;
                }
                
                database.setObject(key, obj);
            }
            
            logger.info("RDB loaded successfully");
            
        } catch (Exception e) {
            logger.error("Failed to load RDB file", e);
        }
    }
    
    /**
     * 读取单字节
     */
    private byte readByte(ByteBuffer buffer, FileChannel channel) throws IOException {
        fillBuffer(buffer, channel);
        return buffer.get();
    }
    
    /**
     * 读取32位整数
     */
    private int readInt(ByteBuffer buffer, FileChannel channel) throws IOException {
        fillBuffer(buffer, channel);
        int value = 0;
        for (int i = 0; i < 4; i++) {
            value = (value << 8) | (buffer.get() & 0xFF);
        }
        return value;
    }
    
    /**
     * 读取64位长整数
     */
    private long readLong(ByteBuffer buffer, FileChannel channel) throws IOException {
        fillBuffer(buffer, channel);
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value = (value << 8) | (buffer.get() & 0xFF);
        }
        return value;
    }
    
    /**
     * 读取字符串
     */
    private String readString(ByteBuffer buffer, FileChannel channel) throws IOException {
        int len = readInt(buffer, channel);
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = readByte(buffer, channel);
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }
    
    /**
     * 填充缓冲区
     */
    private void fillBuffer(ByteBuffer buffer, FileChannel channel) throws IOException {
        if (!buffer.hasRemaining()) {
            buffer.clear();
            channel.read(buffer);
            buffer.flip();
        }
    }
    
    /**
     * 读取Redis对象
     */
    private RedisObject readObject(ByteBuffer buffer, FileChannel channel, byte type) throws IOException {
        switch (type) {
            case 0:  // STRING
                String value = readString(buffer, channel);
                return RedisObject.createString(value);
            case 1:  // LIST
                int listLen = readInt(buffer, channel);
                RedisObject listObj = RedisObject.createList();
                RedisList list = listObj.getValue();
                for (int i = 0; i < listLen; i++) {
                    String item = readString(buffer, channel);
                    list.rpush(item);
                }
                return listObj;
            case 2:  // HASH
                int hashLen = readInt(buffer, channel);
                RedisObject hashObj = RedisObject.createHash();
                RedisHash hash = hashObj.getValue();
                for (int i = 0; i < hashLen; i++) {
                    String field = readString(buffer, channel);
                    String val = readString(buffer, channel);
                    hash.hset(field, val);
                }
                return hashObj;
            case 3:  // SET
                int setLen = readInt(buffer, channel);
                RedisObject setObj = RedisObject.createSet();
                RedisSet set = setObj.getValue();
                for (int i = 0; i < setLen; i++) {
                    String member = readString(buffer, channel);
                    set.sadd(member);
                }
                return setObj;
            case 4:  // ZSET
                int zsetLen = readInt(buffer, channel);
                RedisObject zsetObj = RedisObject.createZSet();
                RedisZSet zset = zsetObj.getValue();
                for (int i = 0; i < zsetLen; i++) {
                    String member = readString(buffer, channel);
                    fillBuffer(buffer, channel);
                    double score = buffer.getDouble();
                    zset.zadd(score, member);
                }
                return zsetObj;
            default:
                throw new IOException("Unknown object type: " + type);
        }
    }
    
    /**
     * 加载AOF文件
     */
    public void loadAOF() {
        if (aofManager == null) return;
        
        try {
            aofManager.loadAndReplay(database);
            logger.info("AOF loaded successfully");
        } catch (Exception e) {
            logger.error("Failed to load AOF file", e);
        }
    }
    
    /**
     * 执行AOF重写
     * 
     * 【AOF重写原理】
     * 随着操作增多，AOF文件会越来越大
     * 重写可以压缩AOF文件：
     * 1. 创建子进程
     * 2. 遍历数据库，将当前状态写入新文件
     * 3. 原子性替换旧文件
     * 
     * 例如：
     * 原AOF：SET k 1, INCR k, INCR k, INCR k
     * 重写后：SET k 4
     */
    public void rewriteAOF() {
        if (aofManager != null) {
            try {
                aofManager.rewrite(database);
                logger.info("AOF rewrite completed");
            } catch (Exception e) {
                logger.error("AOF rewrite failed", e);
            }
        }
    }
}
