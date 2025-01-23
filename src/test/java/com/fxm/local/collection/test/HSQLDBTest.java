package com.fxm.local.collection.test;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.fxm.local.collection.db.config.MainConfig.CONST_DB_ENGINE;
import static com.fxm.local.collection.test.SqliteTest.testCases;

public class HSQLDBTest {

    @BeforeAll
    public static void before() {
        System.setProperty(CONST_DB_ENGINE, "hsqldb");
    }

    /**
     * 测试新增和获取
     */
    @Test
    public void testInsertAndGet() throws Exception {
        testCases();
    }

}
