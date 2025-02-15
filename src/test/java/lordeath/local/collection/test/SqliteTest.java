package lordeath.local.collection.test;

import lombok.extern.slf4j.Slf4j;
import lordeath.local.collection.LocalList;
import lordeath.local.collection.db.config.MainConfig;
import lordeath.local.collection.db.util.DBUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static lordeath.local.collection.test.LocalListTest.testCases;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class SqliteTest {


    @BeforeAll
    public static void before() {
        MainConfig.DB_ENGINE.setProperty("sqlite");
        MainConfig.DB_ENGINE_APP_NAME.setProperty("appTestName");
        Thread.currentThread().setName("sqlite");
    }

    @AfterAll
    public static void after() {
        Thread.currentThread().setName("main");
    }

    /**
     * 测试新增和获取
     */
    @Test
    public void testInsertAndGet() {
        testCases();
    }

    @Test
    public void testMemoryCost() {
        MemoryCostTest.testMemoryCostAll();
    }

    @Test
    public void testClose() throws InterruptedException {
        int start = DBUtil.dropTableCounter.get();
        testAddManyList();
        testAddManyList();
        testAddManyList();
        // 手动触发回收
        System.gc();
        Thread.sleep(1000);
        int now = DBUtil.dropTableCounter.get();
        assertEquals(3, now - start);
        testAddManyList();
    }

    @SuppressWarnings("resource")
    private static void testAddManyList() {
        LocalList<String> list = new LocalList<>();
        list.addAll(IntStream.range(1, 100000).mapToObj(x -> x + "").collect(Collectors.toList()));
        log.info("插入用于内存测试的列表 size: {}", list.size());
    }

}
