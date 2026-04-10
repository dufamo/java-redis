package com.learn.redis.data;

/**
 * SDS (Simple Dynamic String) - 简单动态字符串
 * 
 * 这是Redis中最基础的数据结构之一，用于替代C语言中的传统字符串。
 * Redis没有直接使用C语言的char*，而是自己实现了SDS，原因如下：
 * 
 * 【SDS相比C字符串的优势】
 * 1. O(1)复杂度获取字符串长度 - C字符串需要O(N)遍历
 * 2. 杜绝缓冲区溢出 - 自动扩容机制
 * 3. 减少修改字符串时的内存重分配次数 - 空间预分配和惰性释放
 * 4. 二进制安全 - 可以存储任意二进制数据（包括'\0'）
 * 5. 兼容部分C字符串函数 - 以'\0'结尾
 * 
 * 【内存布局】
 * +--------+--------+--------+--------+--------+--------+--------+--------+
 * |  len   |  free  |  buf[0]|  buf[1]|  buf[2]|  buf[3]|  buf[4]|  '\0'  |
 * +--------+--------+--------+--------+--------+--------+--------+--------+
 * 
 * len:   已使用字节数（不包括'\0'）
 * free:  剩余可用字节数
 * buf:   实际存储数据的字节数组
 * 
 * 【应用场景】
 * - 存储字符串值
 * - 用作缓冲区（AOF缓冲区、客户端输入缓冲区等）
 * - 存储键名
 */
public class SDS {
    
    /**
     * 实际存储数据的字节数组
     * 使用byte[]而不是char[]是为了支持二进制安全
     * 二进制安全意味着可以存储任意二进制数据，而不仅仅是文本
     */
    private byte[] buf;
    
    /**
     * 已使用的字节数（不包括结尾的'\0'）
     * 通过这个字段，获取字符串长度的时间复杂度为O(1)
     * 而C语言的strlen()需要遍历整个字符串，时间复杂度为O(N)
     */
    private int len;
    
    /**
     * 剩余可用的字节数
     * 这个字段实现了空间预分配和惰性释放策略
     * 
     * 【空间预分配策略】
     * 当SDS需要扩容时，不仅分配需要的空间，还会额外分配一些空间
     * - 如果新长度 < 1MB：分配同样大小的额外空间（总空间 = 2 * 新长度）
     * - 如果新长度 >= 1MB：额外分配1MB空间（总空间 = 新长度 + 1MB）
     * 这样可以减少内存重分配的次数
     * 
     * 【惰性释放策略】
     * 当缩短SDS时，不立即释放多余内存，而是记录在free字段中
     * 等待将来使用，避免频繁的内存重分配
     */
    private int free;
    
    /**
     * 默认构造函数 - 创建空字符串
     */
    public SDS() {
        this.buf = new byte[0];
        this.len = 0;
        this.free = 0;
    }
    
    /**
     * 从字符串创建SDS
     * 
     * 【空间预分配】
     * 初始分配时，额外分配与内容相同大小的空间
     * 例如：存储"hello"(5字节)，实际分配10字节
     * 这样后续追加操作时可能不需要重新分配内存
     * 
     * @param str 要存储的字符串
     */
    public SDS(String str) {
        byte[] bytes = str.getBytes();
        // 分配2倍空间，一半存储内容，一半预分配
        this.buf = new byte[bytes.length * 2];
        // 将数据复制到buf中
        System.arraycopy(bytes, 0, this.buf, 0, bytes.length);
        this.len = bytes.length;
        this.free = bytes.length;  // 预分配的空间
    }
    
    /**
     * 从字节数组创建SDS
     * 
     * @param bytes 要存储的字节数组
     */
    public SDS(byte[] bytes) {
        this.buf = new byte[bytes.length * 2];
        System.arraycopy(bytes, 0, this.buf, 0, bytes.length);
        this.len = bytes.length;
        this.free = bytes.length;
    }
    
    /**
     * 获取字符串长度 - O(1)时间复杂度
     * 
     * 这比C语言的strlen()函数快得多
     * strlen()需要遍历整个字符串直到遇到'\0'
     * 
     * @return 字符串长度
     */
    public int length() {
        return len;
    }
    
    /**
     * 获取剩余可用空间
     * 
     * @return 剩余字节数
     */
    public int available() {
        return free;
    }
    
    /**
     * 追加字符串 - 实现了自动扩容
     * 
     * 【扩容策略】
     * 1. 检查剩余空间是否足够
     * 2. 如果不够，计算新需要的空间
     * 3. 按照预分配策略分配新空间
     * 4. 复制数据到新空间
     * 
     * @param str 要追加的字符串
     * @return 返回自身，支持链式调用
     */
    public SDS append(String str) {
        byte[] bytes = str.getBytes();
        return append(bytes);
    }
    
    /**
     * 追加字节数组
     * 
     * @param bytes 要追加的字节数组
     * @return 返回自身
     */
    public SDS append(byte[] bytes) {
        int needed = len + bytes.length;
        
        // 如果剩余空间不足，需要扩容
        if (free < bytes.length) {
            // 计算新的总容量：取需要的空间和当前容量的2倍中的较大值
            // 这就是空间预分配策略
            int newLen = Math.max(needed, buf.length * 2);
            byte[] newBuf = new byte[newLen];
            
            // 复制原有数据到新数组
            System.arraycopy(buf, 0, newBuf, 0, len);
            buf = newBuf;
            free = newLen - needed;  // 更新剩余空间
        } else {
            free -= bytes.length;  // 直接使用剩余空间
        }
        
        // 追加新数据
        System.arraycopy(bytes, 0, buf, len, bytes.length);
        len = needed;
        return this;
    }
    
    /**
     * 追加另一个SDS
     * 
     * @param other 要追加的SDS
     * @return 返回自身
     */
    public SDS cat(SDS other) {
        return append(other.buf, 0, other.len);
    }
    
    /**
     * 追加字节数组的一部分
     * 
     * @param bytes 源数组
     * @param offset 起始偏移量
     * @param length 要追加的长度
     * @return 返回自身
     */
    public SDS append(byte[] bytes, int offset, int length) {
        int needed = len + length;
        
        if (free < length) {
            int newLen = Math.max(needed, buf.length * 2);
            byte[] newBuf = new byte[newLen];
            System.arraycopy(buf, 0, newBuf, 0, len);
            buf = newBuf;
            free = newLen - needed;
        } else {
            free -= length;
        }
        
        System.arraycopy(bytes, offset, buf, len, length);
        len = needed;
        return this;
    }
    
    /**
     * 覆盖写入字符串 - 替换整个内容
     * 
     * 与append不同，这个方法会清空原有内容，写入新内容
     * 如果新内容比原容量大，会扩容
     * 
     * @param str 新的字符串内容
     * @return 返回自身
     */
    public SDS sdscpy(String str) {
        byte[] bytes = str.getBytes();
        if (buf.length < bytes.length) {
            buf = new byte[bytes.length * 2];
        }
        System.arraycopy(bytes, 0, buf, 0, bytes.length);
        len = bytes.length;
        free = buf.length - len;
        return this;
    }
    
    /**
     * 覆盖写入字节数组
     * 
     * @param bytes 新的字节数组
     * @return 返回自身
     */
    public SDS sdscpy(byte[] bytes) {
        if (buf.length < bytes.length) {
            buf = new byte[bytes.length * 2];
        }
        System.arraycopy(bytes, 0, buf, 0, bytes.length);
        len = bytes.length;
        free = buf.length - len;
        return this;
    }
    
    /**
     * 截取字符串指定范围 - 惰性释放
     * 
     * 这个方法实现了惰性释放策略：
     * - 不会立即释放内存
     * - 只是修改len和free的值
     * - 被截掉的空间记录在free中，将来可能被复用
     * 
     * 【参数说明】
     * start和end支持负数索引：
     * - 正数：从开头计数，0表示第一个字符
     * - 负数：从末尾计数，-1表示最后一个字符
     * 
     * 例如：对"hello world"执行range(0, 4)得到"hello"
     *       对"hello world"执行range(-5, -1)得到"world"
     * 
     * @param start 起始索引（包含）
     * @param end 结束索引（包含）
     * @return 返回自身
     */
    public SDS range(int start, int end) {
        // 处理负数索引
        if (start < 0) start = len + start;
        if (end < 0) end = len + end;
        
        // 边界检查
        if (start < 0) start = 0;
        if (end >= len) end = len - 1;
        
        // 空范围处理
        if (start > end) {
            len = 0;
            free = buf.length;
            return this;
        }
        
        // 计算新长度
        int newLen = end - start + 1;
        
        // 将需要保留的数据移动到开头
        System.arraycopy(buf, start, buf, 0, newLen);
        
        // 更新长度信息 - 惰性释放，不实际释放内存
        len = newLen;
        free = buf.length - len;
        return this;
    }
    
    /**
     * 去除首尾指定字符 - 惰性释放
     * 
     * 类似于Python的strip()方法
     * 从字符串首尾去除所有在cset中指定的字符
     * 
     * 例如：对"  hello  "执行trim(" ")得到"hello"
     * 
     * @param cset 要去除的字符集合
     * @return 返回自身
     */
    public SDS trim(String cset) {
        byte[] trimBytes = cset.getBytes();
        int start = 0, end = len - 1;
        
        // 从开头找到第一个不需要去除的字符
        while (start <= end && contains(trimBytes, buf[start])) {
            start++;
        }
        
        // 从末尾找到第一个不需要去除的字符
        while (end >= start && contains(trimBytes, buf[end])) {
            end--;
        }
        
        // 计算新长度
        int newLen = end - start + 1;
        if (newLen != len) {
            // 移动数据到开头
            System.arraycopy(buf, start, buf, 0, newLen);
            len = newLen;
            free = buf.length - len;
        }
        return this;
    }
    
    /**
     * 辅助方法：检查字节数组是否包含指定字节
     */
    private boolean contains(byte[] bytes, byte b) {
        for (byte aByte : bytes) {
            if (aByte == b) return true;
        }
        return false;
    }
    
    /**
     * 获取字节数组 - 返回有效数据的副本
     * 
     * @return 字节数组副本
     */
    public byte[] getBytes() {
        byte[] result = new byte[len];
        System.arraycopy(buf, 0, result, 0, len);
        return result;
    }
    
    /**
     * 转换为字符串
     * 
     * @return 字符串表示
     */
    @Override
    public String toString() {
        return new String(buf, 0, len);
    }
    
    /**
     * 比较两个SDS - 字典序比较
     * 
     * 按字节逐个比较，类似于strcmp()
     * 
     * @param other 要比较的SDS
     * @return 负数表示小于，0表示相等，正数表示大于
     */
    public int compareTo(SDS other) {
        int minLen = Math.min(this.len, other.len);
        for (int i = 0; i < minLen; i++) {
            int cmp = Byte.compare(this.buf[i], other.buf[i]);
            if (cmp != 0) return cmp;
        }
        // 如果公共部分相同，长度长的更大
        return Integer.compare(this.len, other.len);
    }
    
    /**
     * 复制SDS - 深拷贝
     * 
     * 创建一个完全独立的副本，修改副本不会影响原对象
     * 
     * @return 新的SDS副本
     */
    public SDS dup() {
        SDS dup = new SDS();
        dup.buf = new byte[this.buf.length];
        System.arraycopy(this.buf, 0, dup.buf, 0, this.len);
        dup.len = this.len;
        dup.free = this.free;
        return dup;
    }
    
    /**
     * 清空SDS - 惰性释放
     * 
     * 不释放内存，只是重置len和free
     * 将来可以复用已分配的空间
     * 
     * @return 返回自身
     */
    public SDS clear() {
        len = 0;
        free = buf.length;
        return this;
    }
    
    /**
     * 获取指定位置的字符
     * 
     * @param index 索引位置
     * @return 该位置的字节
     * @throws IndexOutOfBoundsException 如果索引越界
     */
    public byte byteAt(int index) {
        if (index < 0 || index >= len) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Length: " + len);
        }
        return buf[index];
    }
}
