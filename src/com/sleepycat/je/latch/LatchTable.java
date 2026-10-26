/*-
 * Copyright (C) 2002, 2017, Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle Berkeley
 * DB Java Edition made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/berkeleydb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle Berkeley DB Java Edition for a copy of the
 * license and additional information.
 */

package com.sleepycat.je.latch;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Table of latches by thread for debugging.
 */
// 核心定位：它利用 Java 的 ThreadLocal 机制，在每个线程独立维护一个由该线程当前所持有的所有 Latch（锁对象）构成的集合（HashSet）。
// 主要用途：
// 死锁检测与排查：通过跟踪每个线程持有的 Latch，可以在发生死锁或长时间阻塞时，快速诊断是哪个线程持有了哪些锁。
// 锁释放检查：确保线程在退出关键逻辑或操作结束时，正确释放了所有获取的 Latch，防止锁泄漏（Latch Leak）。
public class LatchTable {
    // 线程本地变量（ThreadLocal），其内部存储的是一个 Set<Object> 集合。
    // 集合中的每一个元素代表一个被当前线程成功获取并持有的 Latch 对象。
    // 通过 ThreadLocal，不同线程的 Latch 集合彼此隔离、互不干扰，从而实现了无锁的线程安全记录。
    private ThreadLocal<Set<Object>> latchesByThread;
            
    LatchTable() {
        latchesByThread = new ThreadLocal<Set<Object>>();
    }

    /**
     * Adds latch acquired by this thread.
     * @return true if added, false if already present.
     */
    // 向当前线程的 Latch 集合中添加一个新获取的 Latch
    boolean add(Object latch) {
        Set<Object> threadLatches = latchesByThread.get();
        if (threadLatches == null) {
            threadLatches = new HashSet<Object>();
            latchesByThread.set(threadLatches);
        }
        return threadLatches.add(latch);
    }

    /**
     * Removes latch acquired by this thread.
     * @return true if removed, false if not present.
     */
    // 从当前线程的 Latch 集合中移除指定的 Latch（表示该锁已被释放）
    boolean remove(Object latch) {
        Set<Object> threadLatches = latchesByThread.get();
        if (threadLatches == null) {
            return false;
        } else {
            return threadLatches.remove(latch);
        }
    }

    /**
     * Returns the number of latches held by this thread.
     */
    // 获取当前线程当前一共持有了多少个 Latch
    int nLatchesHeld() {
        Set<Object> threadLatches = latchesByThread.get();
        if (threadLatches != null) {
            return threadLatches.size();
        } else {
            return 0;
        }
    }
    // 将当前线程当前持有的所有 Latch 对象信息格式化为字符串，便于日志打印或调试输出。
    String latchesHeldToString() {
        Set<Object> threadLatches = latchesByThread.get();
        StringBuilder sb = new StringBuilder();
        if (threadLatches != null) {
            Iterator<Object> i = threadLatches.iterator();
            while (i.hasNext()) {
                sb.append(i.next()).append('\n');
            }
        }
        return sb.toString();
    }

    void clear() {
        latchesByThread = new ThreadLocal<Set<Object>>();
    }
}
