package lordeath.local.collection.test;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lordeath.local.collection.LocalList;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 检测内存消耗
 */
@Slf4j
public class MemoryCostTest {

    public static void testMemoryCostAll() throws InterruptedException {
        long localListCost = testLocalListMemoryCost();
        long inMemoryCost = testMemoryCost();
        log.warn("内存消耗: {} 本地列表消耗: {}", inMemoryCost, localListCost);
        assertTrue(inMemoryCost >= localListCost);
    }


    public static long testMemoryCost() throws InterruptedException {

        // 获取当前内存使用情况
        MemoryUsage memoryUsageBefore = printMem();

        // 创建大量对象
        List<UserVo> list = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            List<UserVo> subList = new ArrayList<>();
            for (int j = 0; j < 1000; j++) {
                subList.add(new UserVo("name" + j, j));
            }
            list.addAll(subList);
        }

        MemoryUsage memoryUsageAfter = printMem();
        long cost = memoryUsageAfter.getUsed() - memoryUsageBefore.getUsed();
        log.info("列表大小: {} 使用的内存: {}", list.size(), cost);
        return cost;
    }


    public static long testLocalListMemoryCost() throws InterruptedException {
        // 获取当前内存使用情况
        MemoryUsage memoryUsageBefore = printMem();

        // 创建大量对象
        try (LocalList<UserVo> list = new LocalList<>()) {
            for (int i = 0; i < 1000; i++) {
                List<UserVo> subList = new ArrayList<>();
                for (int j = 0; j < 1000; j++) {
                    subList.add(new UserVo("name" + j, j));
                }
                list.addAll(subList);
            }

            MemoryUsage memoryUsageAfter = printMem();
            long cost = memoryUsageAfter.getUsed() - memoryUsageBefore.getUsed();
            log.info("列表大小: {} 使用的内存: {}", list.size(), cost);
            return cost;
        }
    }

    private static MemoryUsage printMem() throws InterruptedException {

        // 尽量触发垃圾回收
        System.gc();
//        // 暂停一会儿，等待 GC 完成
//        Thread.sleep(1000);

        // 获取堆内存使用信息
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heapUsage = memoryMXBean.getHeapMemoryUsage();
        log.warn("Heap usage: {}", heapUsage);
        return heapUsage;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class UserVo {
        private String name;
        private int age;
    }
}
