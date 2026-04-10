package com.learn.redis.data;

import java.util.LinkedList;
import java.util.List;

/**
 * RedisList - Redis列表数据结构实现
 * 
 * 【列表简介】
 * Redis的列表是一个简单的字符串列表， * 按照插入顺序排序
 * 支持在列表两端进行插入和删除操作
 * 
 * 【底层实现】
 * Redis 3.2之前:
 * - 使用ziplist(压缩列表)或linkedlist(双端链表)
 * - 元素少时用ziplist节省内存
 * - 元素多时用linkedlist提高性能
 * 
 * Redis 3.2之后:
 * - 使用quicklist(快速列表)
 * - quicklist = ziplist + linkedlist的组合
 * - 兼顾内存效率和操作性能
 * 
 * 【时间复杂度】
 * - LPUSH/RPUSH: O(1) - 头部/尾部插入
 * - LPOP/RPOP: O(1) - 头部/尾部删除
 * - LINDEX: O(N) - 按索引访问
 * - LRANGE: O(N) - 范围查询
 * - LLEN: O(1) - 获取长度
 * 
 * 【应用场景】
 * - 消息队列
 * - 最新文章列表
 * - 时间线
 * - 评论列表
 */
public class RedisList {
    
    /**
     * 底层存储结构
     * 
     * 使用Java的LinkedList实现
     * LinkedList是一个双端链表:
     * - 在头部插入/删除: O(1)
     * - 在尾部插入/删除: O(1)
     * - 按索引访问: O(N)
     * 
     * Redis使用双端链表是为了支持LPUSH和RPUSH两种操作
     */
    private final LinkedList<String> list;
    
    /**
     * 构造函数 - 初始化空列表
     */
    public RedisList() {
        this.list = new LinkedList<>();
    }
    
    /**
     * LPUSH - 在列表头部插入元素
     * 
     * 【实现细节】
     * 为了保证插入顺序，需要逆序插入
     * 例如: LPUSH mylist a b c
     * 结果: [c, b, a] (c在最前面)
     * 
     * @param values 要插入的值(可多个)
     * @return 插入后的列表长度
     */
    public long lpush(String... values) {
        // 从后往前插入，保证第一个参数在最前面
        for (int i = values.length - 1; i >= 0; i--) {
            list.addFirst(values[i]);
        }
        return list.size();
    }
    
    /**
     * RPUSH - 在列表尾部插入元素
     * 
     * 【实现细节】
     * 按顺序在尾部添加
     * 例如: RPUSH mylist a b c
     * 结果: [a, b, c] (c在最后面)
     * 
     * @param values 要插入的值(可多个)
     * @return 插入后的列表长度
     */
    public long rpush(String... values) {
        for (String value : values) {
            list.addLast(value);
        }
        return list.size();
    }
    
    /**
     * LPOP - 从列表头部弹出元素
     * 
     * 【实现细节】
     * 删除并返回列表第一个元素
     * 如果列表为空，返回null
     * 
     * @return 被弹出的元素，列表为空时返回null
     */
    public String lpop() {
        if (list.isEmpty()) return null;
        return list.removeFirst();
    }
    
    /**
     * RPOP - 从列表尾部弹出元素
     * 
     * @return 被弹出的元素，列表为空时返回null
     */
    public String rpop() {
        if (list.isEmpty()) return null;
        return list.removeLast();
    }
    
    /**
     * LINDEX - 按索引获取元素
     * 
     * 【索引规则】
     * - 正数索引: 从0开始，0表示第一个元素
     * - 负数索引: 从-1开始，-1表示最后一个元素
     * 
     * 例如: 对于[a, b, c]
     * - LINDEX 0 -> a
     * - LINDEX -1 -> c
     * - LINDEX 1 -> b
     * 
     * 【时间复杂度】O(N)
     * 因为LinkedList需要遍历到指定位置
     * 
     * @param index 索引值(支持负数)
     * @return 对应位置的元素，不存在返回null
     */
    public String lindex(long index) {
        // 处理负数索引
        if (index < 0) index = list.size() + index;
        
        // 边界检查
        if (index < 0 || index >= list.size()) return null;
        
        return list.get((int) index);
    }
    
    /**
     * LLEN - 获取列表长度
     * 
     * @return 列表中元素的数量
     */
    public long llen() {
        return list.size();
    }
    
    /**
     * LRANGE - 获取指定范围内的元素
     * 
     * 【范围规则】
     * - start和stop都支持负数索引
     * - 范围是闭区间，     * - 超出范围的索引会被自动调整到边界
     * 
     * 例如: 对于[a, b, c, d, e]
     * - LRANGE 0 2 -> [b, c]
     * - LRANGE -3 -1 -> [c, d, e]
     * - LRANGE 0 -1 -> [a, b, c, d, e] (所有元素)
     * 
     * @param start 起始索引(包含)
     * @param stop 结束索引(包含)
     * @return 范围内的元素列表
     */
    public List<String> lrange(long start, long stop) {
        // 处理负数索引
        if (start < 0) start = list.size() + start;
        if (stop < 0) stop = list.size() + stop;
        
        // 边界调整
        if (start < 0) start = 0;
        if (stop >= list.size()) stop = list.size() - 1;
        
        // 空范围检查
        if (start > stop || start >= list.size()) {
            return new LinkedList<>();
        }
        
        // 返回子列表
        return new LinkedList<>(list.subList((int) start, (int) stop + 1));
    }
    
    /**
     * LREM - 移除列表中的元素
     * 
     * 【移除规则】
     * - count > 0: 从头开始移除count个匹配元素
     * - count < 0: 从尾开始移除|count|个匹配元素
     * - count = 0: 移除所有匹配元素
     * 
     * 例如: 对于[a, b, a, c, a]
     * - LREM 1 a -> 移除第一个a，结果: [b, a, c, a]，返回1
     * - LREM -1 a -> 移除最后一个a，结果: [a, b, a, c]，返回1
     * - LREM 0 a -> 移除所有a，结果: [b, c]，返回3
     * 
     * @param count 移除数量和方向
     * @param value 要移除的值
     * @return 实际移除的元素数量
     */
    public long lrem(long count, String value) {
        long removed = 0;
        
        if (count > 0) {
            // 从头开始移除
            java.util.Iterator<String> it = list.iterator();
            while (it.hasNext() && removed < count) {
                if (it.next().equals(value)) {
                    it.remove();
                    removed++;
                }
            }
        } else if (count < 0) {
            // 从尾开始移除
            java.util.ListIterator<String> it = list.listIterator(list.size());
            while (it.hasPrevious() && removed < -count) {
                if (it.previous().equals(value)) {
                    it.remove();
                    removed++;
                }
            }
        } else {
            // 移除所有匹配元素
            java.util.Iterator<String> it = list.iterator();
            while (it.hasNext()) {
                if (it.next().equals(value)) {
                    it.remove();
                    removed++;
                }
            }
        }
        return removed;
    }
    
    /**
     * LSET - 设置指定索引位置的值
     * 
     * @param index 索引位置
     * @param value 新值
     * @return 成功返回"OK"，索引越界返回null
     */
    public String lset(long index, String value) {
        if (index < 0) index = list.size() + index;
        if (index < 0 || index >= list.size()) return null;
        list.set((int) index, value);
        return "OK";
    }
    
    /**
     * LTRIM - 裁剪列表，     * 只保留指定范围内的元素
     * 
     * @param start 起始索引
     * @param stop 结束索引
     */
    public void ltrim(long start, long stop) {
        if (start < 0) start = list.size() + start;
        if (stop < 0) stop = list.size() + stop;
        if (start < 0) start = 0;
        if (stop >= list.size()) stop = list.size() - 1;
        
        if (start > stop) {
            list.clear();
            return;
        }
        
        // 创建新列表，只保留范围内的元素
        LinkedList<String> newList = new LinkedList<>();
        for (int i = (int) start; i <= (int) stop; i++) {
            newList.addLast(list.get(i));
        }
        list.clear();
        list.addAll(newList);
    }
    
    /**
     * LINSERT - 在指定元素前/后插入新元素
     * 
     * @param pivot 参考元素
     * @param value 要插入的值
     * @param before true表示在pivot前插入，false表示在pivot后插入
     * @return 插入后的列表长度，pivot不存在返回-1
     */
    public long linsertBefore(String pivot, String value) {
        int index = list.indexOf(pivot);
        if (index == -1) return -1;
        list.add(index, value);
        return list.size();
    }
    
    public long linsertAfter(String pivot, String value) {
        int index = list.indexOf(pivot);
        if (index == -1) return -1;
        list.add(index + 1, value);
        return list.size();
    }
    
    /**
     * 检查列表是否为空
     */
    public boolean isEmpty() {
        return list.isEmpty();
    }
    
    /**
     * 获取所有元素
     * 
     * @return 元素列表的副本
     */
    public List<String> getAll() {
        return new LinkedList<>(list);
    }
}
