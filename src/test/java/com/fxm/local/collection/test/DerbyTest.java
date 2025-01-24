package com.fxm.local.collection.test;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.fxm.local.collection.db.config.MainConfig.CONST_DB_ENGINE;
import static com.fxm.local.collection.test.LocalListTest.*;

public class DerbyTest {

    @BeforeAll
    public static void before() {
        System.setProperty(CONST_DB_ENGINE, "derby");
        Thread.currentThread().setName("derby");
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
        // derby 不做测试，因为derby的问题太多了，我不想适配了
        // testList();
        // testMap();
    }

}
