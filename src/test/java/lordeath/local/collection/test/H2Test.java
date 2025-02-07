package lordeath.local.collection.test;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static lordeath.local.collection.db.config.MainConfig.CONST_DB_ENGINE;
import static lordeath.local.collection.test.LocalListTest.testCases;

public class H2Test {

    @BeforeAll
    public static void before() {
        System.setProperty(CONST_DB_ENGINE, "h2");
        Thread.currentThread().setName("h2");
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

}
