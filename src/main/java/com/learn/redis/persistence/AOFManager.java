package com.learn.redis.persistence;

import com.learn.redis.data.RedisDatabase;
import com.learn.redis.data.RedisObject;
import com.learn.redis.protocol.RESPParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * AOFManager - AOF持久化管理器
 * 
 * 【AOF简介】
 * AOF (Append Only File) 是Redis的另一种持久化方式。
 * 它记录所有写操作命令，以追加的方式写入文件。
 * 
 * 【AOF与RDB的比较】
 * ┌──────────────┬─────────────────────┬─────────────────────┐
 * │     特性     │        RDB          │        AOF          │
 * ├──────────────┼─────────────────────┼─────────────────────┤
 * │ 数据安全性   │ 可能丢失几分钟数据  │ 最多丢失1秒数据     │
 * │ 文件大小     │ 小（压缩二进制）    │ 大（文本命令）      │
 * │ 恢复速度     │ 快                  │ 慢                  │
 * │ 系统资源     │ CPU高（压缩）       │ IO高（持续写入）    │
 * │ 可读性       │ 不可读              │ 可读                │
 * └──────────────┴─────────────────────┴─────────────────────┘
 * 
 * 【AOF写入流程】
 * 1. 客户端发送写命令
 * 2. 服务器执行命令
 * 3. 将命令追加到AOF缓冲区
 * 4. 根据策略写入磁盘：
 *    - always: 每个命令都fsync
 *    - everysec: 每秒fsync一次
 *    - no: 由操作系统决定
 * 
 * 【AOF重写】
 * 随着操作增多，AOF文件会越来越大。
 * 重写可以压缩AOF文件：
 * - 创建新进程
 * - 遍历数据库，生成恢复当前状态所需的最小命令集
 * - 原子性替换旧文件
 * 
 * 例如：
 * 原AOF：SET k 1, INCR k, INCR k, INCR k
 * 重写后：SET k 4
 * 
 * 【实现细节】
 * - 使用阻塞队列缓冲命令
 * - 后台线程异步写入
 * - 定期fsync保证数据安全
 */
public class AOFManager {
    private static final Logger logger = LoggerFactory.getLogger(AOFManager.class);
    
    /**
     * AOF文件名
     */
    private final String filename;
    
    /**
     * 命令队列
     * 
     * 使用阻塞队列实现生产者-消费者模式：
     * - 主线程将命令放入队列（生产者）
     * - 后台线程从队列取出命令写入文件（消费者）
     * 
     * 队列容量设为10000，防止内存溢出。
     */
    private final BlockingQueue<String> commandQueue;
    
    /**
     * 定时任务调度器
     * 
     * 用于定期执行fsync操作
     */
    private final ScheduledExecutorService scheduler;
    
    /**
     * 写入线程执行器
     * 
     * 单线程执行AOF写入，保证写入顺序
     */
    private final ExecutorService writerExecutor;
    
    /**
     * 运行状态标志
     */
    private volatile boolean running;
    
    /**
     * 随机访问文件对象
     * 
     * 支持随机读写，用于追加写入
     */
    private RandomAccessFile raf;
    
    /**
     * 文件通道
     * 
     * 使用NIO FileChannel进行高效写入
     */
    private FileChannel channel;
    
    /**
     * 当前文件大小
     * 
     * 用于判断是否需要重写
     */
    private final AtomicLong currentSize;
    
    /**
     * 同步锁
     * 
     * 用于保护文件操作的线程安全
     */
    private final Object lock = new Object();
    
    /**
     * 构造函数
     * 
     * @param filename AOF文件名
     */
    public AOFManager(String filename) {
        this.filename = filename;
        this.commandQueue = new LinkedBlockingQueue<>(10000);
        this.scheduler = Executors.newScheduledThreadPool(1);
        this.writerExecutor = Executors.newSingleThreadExecutor();
        this.running = false;
        this.currentSize = new AtomicLong(0);
    }
    
    /**
     * 启动AOF管理器
     * 
     * 【启动流程】
     * 1. 打开AOF文件（追加模式）
     * 2. 定位到文件末尾
     * 3. 启动写入线程
     * 4. 启动定时fsync任务
     */
    public void start() {
        running = true;
        
        try {
            // 以读写模式打开文件
            raf = new RandomAccessFile(filename, "rw");
            channel = raf.getChannel();
            
            // 定位到文件末尾，准备追加
            channel.position(channel.size());
            currentSize.set(channel.size());
            
        } catch (IOException e) {
            logger.error("Failed to open AOF file", e);
            return;
        }
        
        // 启动写入线程
        writerExecutor.submit(this::writeLoop);
        
        // 每秒执行一次fsync
        // 这是Redis默认的appendfsync策略：everysec
        scheduler.scheduleAtFixedRate(() -> {
            try {
                sync();
            } catch (IOException e) {
                logger.error("AOF sync failed", e);
            }
        }, 1, 1, TimeUnit.SECONDS);
        
        logger.info("AOF manager started");
    }
    
    /**
     * 关闭AOF管理器
     * 
     * 【关闭流程】
     * 1. 设置停止标志
     * 2. 发送空命令唤醒写入线程
     * 3. 等待线程结束
     * 4. 最后一次fsync
     * 5. 关闭文件
     */
    public void shutdown() {
        running = false;
        
        // 发送空命令唤醒可能阻塞的写入线程
        commandQueue.offer("");
        
        // 关闭线程池
        writerExecutor.shutdown();
        scheduler.shutdown();
        
        try {
            // 等待线程结束
            writerExecutor.awaitTermination(10, TimeUnit.SECONDS);
            scheduler.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        synchronized (lock) {
            try {
                if (channel != null) {
                    // 最后一次fsync
                    channel.force(true);
                    channel.close();
                }
                if (raf != null) {
                    raf.close();
                }
            } catch (IOException e) {
                logger.error("Failed to close AOF file", e);
            }
        }
        
        logger.info("AOF manager shutdown");
    }
    
    /**
     * 追加命令到队列
     * 
     * 由主线程调用，将写命令加入队列。
     * 使用offer而非put，避免阻塞主线程。
     * 
     * @param command 命令字符串
     */
    public void appendCommand(String command) {
        if (!running) return;
        
        try {
            // 使用带超时的offer，避免队列满时无限等待
            commandQueue.offer(command, 100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * 写入循环
     * 
     * 后台线程的主循环，不断从队列取出命令并写入文件。
     */
    private void writeLoop() {
        while (running) {
            try {
                // 从队列取出命令，最多等待100毫秒
                String command = commandQueue.poll(100, TimeUnit.MILLISECONDS);
                
                if (command == null || command.isEmpty()) {
                    continue;
                }
                
                // 写入命令
                writeCommand(command);
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Error writing to AOF", e);
            }
        }
    }
    
    /**
     * 写入单个命令
     * 
     * 将命令编码为RESP格式并写入文件。
     * 
     * @param command 命令字符串
     */
    private void writeCommand(String command) {
        synchronized (lock) {
            try {
                // 编码为RESP格式
                byte[] data = encodeCommand(command);
                
                // 使用ByteBuffer写入
                ByteBuffer buffer = ByteBuffer.wrap(data);
                channel.write(buffer);
                
                // 更新文件大小
                currentSize.addAndGet(data.length);
                
            } catch (IOException e) {
                logger.error("Failed to write command to AOF", e);
            }
        }
    }
    
    /**
     * 编码命令为RESP格式
     * 
     * 【RESP格式】
     * *<参数数量>\r\n
     * $<参数1长度>\r\n<参数1>\r\n
     * $<参数2长度>\r\n<参数2>\r\n
     * ...
     * 
     * 例如：SET key value
     * *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
     * 
     * @param command 命令字符串
     * @return RESP编码后的字节数组
     */
    private byte[] encodeCommand(String command) {
        String[] parts = command.split(" ");
        StringBuilder sb = new StringBuilder();
        
        // 写入数组长度
        sb.append("*").append(parts.length).append("\r\n");
        
        // 写入每个参数
        for (String part : parts) {
            sb.append("$").append(part.length()).append("\r\n");
            sb.append(part).append("\r\n");
        }
        
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }
    
    /**
     * 同步到磁盘
     * 
     * 调用FileChannel.force()将缓冲区数据写入磁盘。
     * 参数true表示同时更新文件元数据（如修改时间）。
     * 
     * @throws IOException 如果同步失败
     */
    private void sync() throws IOException {
        synchronized (lock) {
            if (channel != null && channel.isOpen()) {
                channel.force(false);
            }
        }
    }
    
    /**
     * 加载并重放AOF文件
     * 
     * 【加载流程】
     * 1. 打开AOF文件
     * 2. 读取所有数据
     * 3. 解析RESP命令
     * 4. 依次执行命令
     * 
     * @param database 数据库实例
     * @throws IOException 如果加载失败
     */
    public void loadAndReplay(RedisDatabase database) throws IOException {
        File file = new File(filename);
        if (!file.exists()) {
            logger.info("AOF file not found: {}", filename);
            return;
        }
        
        try (FileInputStream fis = new FileInputStream(file);
             FileChannel readChannel = fis.getChannel()) {
            
            // 读取所有数据
            ByteBuffer buffer = ByteBuffer.allocate(8192);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            
            while (readChannel.read(buffer) != -1) {
                buffer.flip();
                while (buffer.hasRemaining()) {
                    byte b = buffer.get();
                    baos.write(b);
                }
                buffer.clear();
            }
            
            // 解析并执行命令
            byte[] data = baos.toByteArray();
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            
            com.learn.redis.protocol.RESPParser parser = 
                    new com.learn.redis.protocol.RESPParser(bais);
            com.learn.redis.command.CommandExecutor executor = 
                    new com.learn.redis.command.CommandExecutor(database);
            
            while (bais.available() > 0) {
                try {
                    com.learn.redis.protocol.RESPObject cmd = parser.parse();
                    if (cmd != null) {
                        executor.execute(cmd);
                    }
                } catch (Exception e) {
                    break;
                }
            }
        }
    }
    
    /**
     * AOF重写
     * 
     * 【重写原理】
     * 1. 创建临时文件
     * 2. 遍历数据库，生成恢复当前状态的最小命令集
     * 3. 同步到磁盘
     * 4. 原子性替换旧文件
     * 
     * 【重写的好处】
     * - 减少文件大小
     * - 提高加载速度
     * - 去除冗余命令
     * 
     * 例如：
     * 原AOF：SET k 1, INCR k, INCR k, INCR k, DEL k
     * 重写后：（空，因为k已被删除）
     * 
     * @param database 数据库实例
     * @throws IOException 如果重写失败
     */
    public void rewrite(RedisDatabase database) throws IOException {
        String tempFile = filename + ".tmp";
        
        try (RandomAccessFile tempRaf = new RandomAccessFile(tempFile, "rw");
             FileChannel tempChannel = tempRaf.getChannel()) {
            
            // 遍历所有数据库
            for (int dbIndex = 0; dbIndex < database.getDbCount(); dbIndex++) {
                Map<String, RedisObject> dbData = database.getDb(dbIndex);
                if (dbData == null || dbData.isEmpty()) continue;
                
                // 写入SELECT命令
                String selectCmd = encodeSelectCommand(dbIndex);
                tempChannel.write(ByteBuffer.wrap(selectCmd.getBytes(StandardCharsets.UTF_8)));
                
                // 写入所有键值对
                for (Map.Entry<String, RedisObject> entry : dbData.entrySet()) {
                    String key = entry.getKey();
                    RedisObject obj = entry.getValue();
                    
                    // 跳过已过期的键
                    if (obj.isExpired()) continue;
                    
                    // 生成并写入SET命令
                    String cmd = generateSetCommand(key, obj);
                    if (cmd != null) {
                        tempChannel.write(ByteBuffer.wrap(cmd.getBytes(StandardCharsets.UTF_8)));
                    }
                    
                    // 写入过期时间命令
                    if (obj.hasExpire()) {
                        String expireCmd = generateExpireCommand(key, obj.getExpireTime());
                        tempChannel.write(ByteBuffer.wrap(expireCmd.getBytes(StandardCharsets.UTF_8)));
                    }
                }
            }
            
            // 同步到磁盘
            tempChannel.force(true);
        }
        
        // 原子性替换文件
        synchronized (lock) {
            // 关闭旧文件
            if (channel != null) {
                channel.close();
            }
            if (raf != null) {
                raf.close();
            }
            
            // 删除旧文件，重命名新文件
            File oldFile = new File(filename);
            oldFile.delete();
            new File(tempFile).renameTo(oldFile);
            
            // 重新打开文件
            raf = new RandomAccessFile(filename, "rw");
            channel = raf.getChannel();
            currentSize.set(channel.size());
        }
        
        logger.info("AOF rewrite completed");
    }
    
    /**
     * 编码SELECT命令
     * 
     * @param dbIndex 数据库索引
     * @return RESP格式的SELECT命令
     */
    private String encodeSelectCommand(int dbIndex) {
        return "*2\r\n$6\r\nSELECT\r\n$" + 
               String.valueOf(dbIndex).length() + "\r\n" + dbIndex + "\r\n";
    }
    
    /**
     * 生成SET命令
     * 
     * 根据对象类型生成相应的命令：
     * - STRING: SET
     * - LIST: RPUSH
     * - HASH: HMSET
     * - SET: SADD
     * - ZSET: ZADD
     * 
     * @param key 键名
     * @param obj 值对象
     * @return RESP格式的命令
     */
    private String generateSetCommand(String key, RedisObject obj) {
        StringBuilder sb = new StringBuilder();
        
        switch (obj.getType()) {
            case STRING:
                String value = obj.getStringValue();
                sb.append("*3\r\n$3\r\nSET\r\n");
                sb.append("$").append(key.length()).append("\r\n").append(key).append("\r\n");
                sb.append("$").append(value.length()).append("\r\n").append(value).append("\r\n");
                break;
                
            case LIST:
                RedisList list = obj.getValue();
                List<String> items = list.getAll();
                if (items.isEmpty()) return null;
                sb.append("*").append(items.size() + 2).append("\r\n$5\r\nRPUSH\r\n");
                sb.append("$").append(key.length()).append("\r\n").append(key).append("\r\n");
                for (String item : items) {
                    sb.append("$").append(item.length()).append("\r\n").append(item).append("\r\n");
                }
                break;
                
            case HASH:
                RedisHash hash = obj.getValue();
                Map<String, String> hashData = hash.hgetall();
                if (hashData.isEmpty()) return null;
                int hashCount = hashData.size() * 2 + 2;
                sb.append("*").append(hashCount).append("\r\n$4\r\nHMSET\r\n");
                sb.append("$").append(key.length()).append("\r\n").append(key).append("\r\n");
                for (Map.Entry<String, String> entry : hashData.entrySet()) {
                    sb.append("$").append(entry.getKey().length()).append("\r\n").append(entry.getKey()).append("\r\n");
                    sb.append("$").append(entry.getValue().length()).append("\r\n").append(entry.getValue()).append("\r\n");
                }
                break;
                
            case SET:
                RedisSet set = obj.getValue();
                java.util.Set<String> members = set.smembers();
                if (members.isEmpty()) return null;
                int setCount = members.size() + 2;
                sb.append("*").append(setCount).append("\r\n$4\r\nSADD\r\n");
                sb.append("$").append(key.length()).append("\r\n").append(key).append("\r\n");
                for (String member : members) {
                    sb.append("$").append(member.length()).append("\r\n").append(member).append("\r\n");
                }
                break;
                
            case ZSET:
                RedisZSet zset = obj.getValue();
                List<RedisZSet.ZSetEntry> entries = zset.zrange(0, -1);
                if (entries.isEmpty()) return null;
                int zsetCount = entries.size() * 2 + 2;
                sb.append("*").append(zsetCount).append("\r\n$4\r\nZADD\r\n");
                sb.append("$").append(key.length()).append("\r\n").append(key).append("\r\n");
                for (RedisZSet.ZSetEntry entry : entries) {
                    String scoreStr = String.valueOf(entry.score);
                    sb.append("$").append(scoreStr.length()).append("\r\n").append(scoreStr).append("\r\n");
                    sb.append("$").append(entry.member.length()).append("\r\n").append(entry.member).append("\r\n");
                }
                break;
        }
        
        return sb.toString();
    }
    
    /**
     * 生成EXPIRE命令
     * 
     * @param key 键名
     * @param expireTime 过期时间戳
     * @return RESP格式的EXPIRE命令
     */
    private String generateExpireCommand(String key, long expireTime) {
        long ttl = (expireTime - System.currentTimeMillis()) / 1000;
        if (ttl <= 0) return "";
        
        String ttlStr = String.valueOf(ttl);
        StringBuilder sb = new StringBuilder();
        sb.append("*3\r\n$6\r\nEXPIRE\r\n");
        sb.append("$").append(key.length()).append("\r\n").append(key).append("\r\n");
        sb.append("$").append(ttlStr.length()).append("\r\n").append(ttlStr).append("\r\n");
        return sb.toString();
    }
}
