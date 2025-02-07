package lordeath.local.collection.test;

import lordeath.local.collection.LocalList;
import lordeath.local.collection.db.util.DBUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static lordeath.local.collection.db.config.MainConfig.CONST_DB_ENGINE;
import static lordeath.local.collection.test.LocalListTest.testCases;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SqliteTest {


    @BeforeAll
    public static void before() {
        System.setProperty(CONST_DB_ENGINE, "sqlite");
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
    public void testMemoryCost() throws InterruptedException {
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

    private static void testAddManyList() {
        LocalList<String> list = new LocalList<>();
        list.addAll(IntStream.range(1, 100000).mapToObj(x -> x + "").collect(Collectors.toList()));
    }

}
